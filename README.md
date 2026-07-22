# pgspawn

Durable PostgreSQL-backed background jobs for Tokio applications.

`pgspawn` runs background jobs without a separate queue service. Jobs, schedules, worker heartbeats, and locks live in PostgreSQL, while Tokio runs the handlers inside your application processes.

The crate has not been published yet, so its API and initial migration may still change.

## Features

- Durable jobs shared safely by workers across application processes.
- Fast wakeups for new work, with polling as an independent fallback.
- Configurable concurrency, delayed jobs, priorities, retry limits, and exponential retry backoff.
- Job keys for deduplicating, replacing, or debouncing active work.
- Transactional and batch enqueueing, so jobs commit with the application changes that justify them.
- Named queues for serial execution across worker processes.
- Daily, monthly, and interval schedules with IANA time-zone support, coordinated through PostgreSQL.
- Worker heartbeats, lock renewal, dead-worker recovery, graceful shutdown, and shutdown-aware handlers.
- Configurable retention that prunes finished jobs in the background.
- Typed Serde payloads alongside direct `serde_json::Value` access, and Jiff timestamps in the public API.
- Structured `tracing` logs and a per-job span around handler execution.

## Requirements

`pgspawn` requires PostgreSQL 13 or newer and a Tokio application with an SQLx PostgreSQL connection pool.

## Quick start

Run the embedded migrations once during deployment or application startup, register handlers, start a worker, and enqueue jobs through a `Queue`.

```rust,no_run
use pgspawn::{Queue, Registry, Worker, db};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Deserialize, Serialize)]
struct SendEmail {
    address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect("postgres://localhost/my_app").await?;
    db::migrate(&pool).await?;

    let registry = Registry::builder()
        .register_typed("send_email", |_job, payload: SendEmail| async move {
            println!("sending email to {}", payload.address);
            Ok::<(), std::io::Error>(())
        })
        .build();

    let worker = Worker::new(pool.clone(), registry).start_with_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    });
    let queue = Queue::new(pool);

    queue
        .enqueue_typed(
            "send_email",
            &SendEmail {
                address: "person@example.com".to_owned(),
            },
        )
        .await?;

    worker.await??;
    Ok(())
}
```

The worker runs until Ctrl-C, then stops claiming jobs and gives active handlers time to finish. Applications with an existing shutdown signal can pass that signal to `start_with_shutdown` or await `run_until_shutdown` directly.

## Delivery semantics

`pgspawn` provides at-least-once delivery. A job may run more than once when a worker loses its database connection or process ownership after performing side effects but before recording completion. Handlers should therefore be idempotent, transactional, or protected by an application-level idempotency key.

Named queues limit execution to one running job with a given queue name across all workers. Finishing a job in a named queue sends a PostgreSQL notification, so the next job in that queue starts immediately instead of waiting for the next poll. Job keys prevent more than one queued or running job with the same key. Neither mechanism turns arbitrary external side effects into exactly-once operations.

## Handlers with shared state

Most applications register several jobs against shared state such as a connection pool, configuration, and API clients. A common layout keeps job name constants, payload types, handlers, and enqueue helpers together in one module, so callers never touch raw job names or payload shapes:

```rust,no_run
use pgspawn::{EnqueueOptions, Queue, Registry, Worker, db};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

const JOB_SEND_RECEIPT: &str = "send_receipt";
const JOB_SYNC_PRICES: &str = "sync_prices";

#[derive(Deserialize, Serialize)]
struct SendReceipt {
    order_id: Uuid,
}

#[derive(Clone)]
struct AppContext {
    db: PgPool,
    // configuration, API clients, ...
}

fn registry(context: AppContext) -> Registry {
    Registry::builder()
        .register_typed(JOB_SEND_RECEIPT, {
            let context = context.clone();
            move |_job, payload: SendReceipt| {
                let context = context.clone();
                async move { send_receipt(&context, payload.order_id).await }
            }
        })
        .register(JOB_SYNC_PRICES, {
            let context = context.clone();
            move |_job| {
                let context = context.clone();
                async move { sync_prices(&context).await }
            }
        })
        .build()
}

async fn send_receipt(context: &AppContext, order_id: Uuid) -> Result<(), sqlx::Error> {
    // load the order from context.db and send the receipt
    Ok(())
}

async fn sync_prices(context: &AppContext) -> Result<(), sqlx::Error> {
    // refresh prices from an upstream API
    Ok(())
}

// Enqueue helpers keep job names, payloads, and options in one place. The job key deduplicates work if the same order is submitted twice.
async fn enqueue_send_receipt(
    queue: &Queue,
    order_id: Uuid,
) -> Result<Uuid, pgspawn::EnqueueError> {
    queue
        .enqueue_typed_with_options(
            JOB_SEND_RECEIPT,
            &SendReceipt { order_id },
            EnqueueOptions {
                job_key: Some(format!("send_receipt:{order_id}")),
                ..EnqueueOptions::default()
            },
        )
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect("postgres://localhost/my_app").await?;
    db::migrate(&pool).await?;

    let context = AppContext { db: pool.clone() };
    let worker = Worker::new(pool.clone(), registry(context)).start_with_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    });

    let queue = Queue::new(pool);
    enqueue_send_receipt(&queue, Uuid::new_v4()).await?;

    worker.await??;
    Ok(())
}
```

## Inside an axum application

`pgspawn` is designed to run inside an existing application process, so an axum server and its job worker share one pool, one process, and one shutdown signal. Request handlers enqueue jobs through a `Queue` in the router state and return immediately; the worker executes them in the background:

```rust,ignore
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use pgspawn::{Queue, Registry, Worker, db};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

const JOB_WELCOME_EMAIL: &str = "welcome_email";

#[derive(Deserialize, Serialize)]
struct WelcomeEmail {
    email: String,
}

#[derive(Clone)]
struct AppState {
    db: PgPool,
    queue: Queue,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let pool = PgPool::connect("postgres://localhost/my_app").await?;
    db::migrate(&pool).await?;

    let registry = Registry::builder()
        .register_typed(JOB_WELCOME_EMAIL, {
            let pool = pool.clone();
            move |_job, payload: WelcomeEmail| {
                let pool = pool.clone();
                async move { send_welcome_email(&pool, &payload.email).await }
            }
        })
        .build();
    let worker = Worker::new(pool.clone(), registry).start_with_shutdown(shutdown_signal());

    let app = Router::new()
        .route("/signups", post(create_signup))
        .with_state(AppState {
            db: pool.clone(),
            queue: Queue::new(pool),
        });
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // Wait for the worker to finish in-flight jobs and release its locks.
    worker.await??;
    Ok(())
}

async fn create_signup(
    State(state): State<AppState>,
    Json(input): Json<WelcomeEmail>,
) -> Result<StatusCode, StatusCode> {
    // ... insert the signup into state.db ...
    state
        .queue
        .enqueue_typed(JOB_WELCOME_EMAIL, &input)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::ACCEPTED)
}

async fn send_welcome_email(db: &PgPool, email: &str) -> Result<(), sqlx::Error> {
    // render and send the email
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
```

## Enqueue options

`EnqueueOptions` controls job execution:

- `run_at` delays a job until a PostgreSQL timestamp.
- `priority` orders available work; lower values run first.
- `max_attempts` includes the initial execution attempt.
- `queue_name` serializes jobs sharing the same queue name.
- `job_key` deduplicates queued and running jobs.
- `job_key_mode` decides what happens when that key is already held by an unfinished job.

### Job key modes

| Mode | Effect on a queued job with the same key | Use it for |
| --- | --- | --- |
| `Dedupe` (default) | Keeps the existing job unchanged and returns its id. | Collapsing repeated triggers into one run. |
| `Replace` | Overwrites the job, including `run_at`, so the timer restarts on every enqueue. | Debouncing repeated work while keeping the newest payload. |
| `PreserveRunAt` | Overwrites the job but keeps its current `run_at`. | Refreshing the payload without postponing execution. |

A running job's definition and lock are never modified under any mode, and no follow-up job is created. Do not reuse a key when every state change needs an execution after the current handler started; use a distinct occurrence key instead.

## Transactional enqueueing

`Queue::enqueue` and its variants run on the queue's own pool, so a job is durable as soon as the call returns. When a job should exist only if an application change also succeeds, enqueue through the caller's transaction instead with `enqueue_in`, `enqueue_in_with_options`, `enqueue_typed_in`, or `enqueue_typed_in_with_options`. These accept any SQLx PostgreSQL executor, so the insert commits or rolls back with the surrounding work and workers never observe a job belonging to an abandoned transaction.

```rust,no_run
use pgspawn::Queue;
use serde::Serialize;
use sqlx::PgPool;

#[derive(Serialize)]
struct SendReceipt {
    order_id: i64,
}

async fn place_order(pool: &PgPool, queue: &Queue) -> Result<(), Box<dyn std::error::Error>> {
    let mut transaction = pool.begin().await?;
    let order_id: i64 = sqlx::query_scalar("INSERT INTO orders DEFAULT VALUES RETURNING id")
        .fetch_one(&mut *transaction)
        .await?;
    queue
        .enqueue_typed_in(&mut *transaction, "send_receipt", &SendReceipt { order_id })
        .await?;
    transaction.commit().await?;
    Ok(())
}
```

## Enqueueing in batches

`Queue::enqueue_many` inserts many jobs in one transaction and returns their ids in request order. Each job carries its own name, payload, and options through an `EnqueueRequest`, and the whole batch is atomic: either every job is enqueued or none is. `Queue::enqueue_many_in` does the same through a caller's connection or transaction, using a transaction or savepoint so a mixed-mode batch cannot partially commit.

```rust,no_run
use pgspawn::{EnqueueError, EnqueueOptions, EnqueueRequest, Queue};
use uuid::Uuid;

async fn enqueue_emails(queue: &Queue, addresses: Vec<String>) -> Result<Vec<Uuid>, EnqueueError> {
    let requests: Vec<EnqueueRequest> = addresses
        .into_iter()
        .map(|address| {
            EnqueueRequest::new("send_email", serde_json::json!({ "address": &address }))
                .with_options(EnqueueOptions {
                    job_key: Some(format!("send_email:{address}")),
                    ..EnqueueOptions::default()
                })
        })
        .collect();
    queue.enqueue_many(&requests).await
}
```

Job keys resolve against existing jobs exactly as a single enqueue does, so a returned id may belong to a job that already existed. Job keys must be unique within one batch; a repeat returns `EnqueueError::DuplicateJobKey` and inserts nothing, since PostgreSQL cannot apply two conflicting upserts to the same row in one statement.

Batching is also the recommended way to keep notification overhead low; see [Notification cost at high enqueue rates](#notification-cost-at-high-enqueue-rates).

## Managing jobs

`Queue` exposes management operations alongside enqueueing. `Queue::get` fetches a job by id, and `Queue::recent` lists the most recently created jobs. `Job::status` is a typed `JobStatus` enum. `Queue::cancel` cancels a job that is still queued; running jobs are not interrupted. `Queue::retry` resets a failed or cancelled job to run again immediately with a fresh attempt counter. `Queue::reschedule` moves a queued job to a new `run_at`, either postponing it or making it due now; running and finished jobs are never rescheduled.

For operational recovery, `Queue::permanently_fail` stops a job for good and `Queue::complete` retires one as succeeded; both work on queued and running jobs. Neither interrupts a handler that is already executing, but they release the job's lock, so that handler's result is discarded and the job does not retry. Releasing a running named-queue job also allows the next job in that queue to start before the old handler exits, so only use either operation when that overlap is safe. `Queue::force_unlock_worker` atomically requeues everything locked by a worker known to be gone and deregisters it rather than waiting out its heartbeat timeout. Calling it for a live worker can make its in-flight jobs run concurrently a second time. `Queue::workers` lists registered workers, including stale registrations not yet recovered. `Queue::counts` returns exact job totals by status and may scan all retained jobs, so avoid calling it at high frequency on large queues.

Finished jobs stay in the table as execution history. By default a worker deletes succeeded, failed, and cancelled jobs 30 days after they finish; tune this with `WorkerConfig::finished_job_retention`. Setting it to `None` keeps finished jobs forever, in which case the application should call `Queue::prune_finished` itself to keep the table bounded.

## Scheduling

Attach `CronJob` values with `Worker::with_crons`. Schedules are recorded in PostgreSQL so multiple application instances coordinate a single enqueue operation. `daily_utc` and `monthly_utc` provide simple UTC schedules, while `daily_in_timezone` and `monthly_in_timezone` accept IANA time-zone names such as `America/New_York`. Callers that already have a Jiff `TimeZone` can use `daily` or `monthly` directly. pgspawn re-exports Jiff as `pgspawn::jiff`, so applications do not need a separate Jiff dependency to name or construct these values. Interval schedules align to Unix epoch boundaries and do not use a time zone.

```rust
use pgspawn::{CronJob, DailyJobSchedule, jiff};

fn weekday_report() -> Result<CronJob, jiff::Error> {
    CronJob::daily_in_timezone(
        "weekday-report",
        "send_report",
        DailyJobSchedule::new(9, 30),
        "America/New_York",
    )
}
```

A larger deployment typically builds its cron list in one function. Identifiers name the occurrence slot and stay stable across releases, while the job name selects the handler, so one handler can back several schedules:

```rust
use std::time::Duration;

use pgspawn::{
    CronJob, DailyJobSchedule, MonthlyJobSchedule,
    jiff::{self, civil::Weekday},
};

const WEEKDAYS: &[Weekday] = &[
    Weekday::Monday,
    Weekday::Tuesday,
    Weekday::Wednesday,
    Weekday::Thursday,
    Weekday::Friday,
];

fn crons() -> Result<Vec<CronJob>, jiff::Error> {
    Ok(vec![
        // After United States markets close, Monday through Friday.
        CronJob::daily_in_timezone(
            "sync-prices-after-close",
            "sync_prices",
            DailyJobSchedule::new(16, 30).weekdays(WEEKDAYS),
            "America/New_York",
        )?,
        // First day of every month at 03:30 UTC.
        CronJob::monthly_utc(
            "rebuild-reference-data-monthly",
            "rebuild_reference_data",
            MonthlyJobSchedule::new(1, 3, 30),
        ),
        // Every 15 minutes, aligned to the clock.
        CronJob::interval(
            "prune-sessions",
            "prune_sessions",
            Duration::from_secs(15 * 60),
        ),
    ])
}
```

Attach the list with `Worker::with_crons(crons()?)`; every worker process can register the same list, and PostgreSQL coordinates a single enqueue per occurrence.

Named-zone schedules follow Jiff's compatible daylight-saving-time disambiguation. A local time skipped by a forward transition moves forward by the gap. A local time repeated by a backward transition runs once at the earlier occurrence.

Named time zones resolve through Jiff's time zone database lookup. On Linux and macOS this reads `/usr/share/zoneinfo`, which minimal container images sometimes omit; install `tzdata` in such images or enable Jiff's bundled database, otherwise `daily_in_timezone` and `monthly_in_timezone` return an error at startup. UTC and interval schedules do not depend on a time zone database.

By default, adding a new cron definition records its current schedule without enqueueing an old occurrence. Call `CronJob::catch_up_on_start` to enqueue the latest due occurrence when the definition is first observed.

Every cron job name must have a registered handler, and cron identifiers must be unique within a worker.

## Shutdown-aware handlers

When a worker shuts down it stops claiming new jobs and gives running handlers `shutdown_grace_period` to finish. Handlers still running when that elapses are aborted, and their jobs are requeued to run again from the start.

Long-running handlers can wind down instead. `pgspawn::is_shutting_down()` reports whether the worker running the current job has begun shutting down, and `pgspawn::shutdown_requested()` resolves when it does. Both work across awaited calls in the handler future without threading a parameter through, and are inert when called outside one. Tokio task-local state is not inherited by tasks created with `tokio::spawn`, so spawned child tasks need their own cancellation mechanism.

```rust,no_run
use pgspawn::Job;

async fn process(_item: u32) -> Result<(), std::io::Error> {
    Ok(())
}

async fn import_items(_job: Job, items: Vec<u32>) -> Result<(), std::io::Error> {
    for item in items {
        if pgspawn::is_shutting_down() {
            // Returning an error retries; returning Ok relies on checkpointed work or an idempotent job.
            return Ok(());
        }
        process(item).await?;
    }
    Ok(())
}
```

A handler that returns before the grace period elapses completes its job normally, so checkpointing progress and returning is preferable to being aborted midway.

## Logging

`pgspawn` logs through the `tracing` crate. With a subscriber at the default `info` level, every job execution is visible: `job started` records the job name, id, attempt, and queue when a worker claims it, and `job succeeded` records the elapsed milliseconds when it completes. A failing attempt logs `job failed and will retry` at `warn` with the error and the backoff delay, and the final attempt logs `job failed permanently` at `error`. A handler stopped after an operator retires its job logs at `info`, while an unexpected nonterminal lock loss remains an `error`. Worker startup and shutdown, cron enqueues, released locks, and recovered jobs are also logged at `info`.

Handlers run inside a `job` tracing span carrying the job name, id, and attempt, so events emitted by application code during a job automatically include job context.

Verbosity is controlled through the subscriber rather than worker configuration. With `tracing_subscriber::EnvFilter`:

- `RUST_LOG=info` shows the full job and worker lifecycle.
- `RUST_LOG=warn,pgspawn=info` keeps job logs while quieting the rest of the application, and `RUST_LOG=info,pgspawn=warn` does the reverse, leaving only retries and failures from `pgspawn`.
- `RUST_LOG=pgspawn=debug` adds routine detail, such as cron occurrences that were already enqueued by another worker.

## Wakeups and polling

Workers learn about new work through two cooperating mechanisms:

- PostgreSQL `LISTEN`/`NOTIFY` wakes claim loops immediately when a job is enqueued, requeued, or when finishing a job frees a named queue.
- Each idle claim loop checks PostgreSQL every `poll_interval` (2 seconds by default), covering delayed jobs, retry backoffs, missed notifications, and listener outages.

Jobs enqueued for immediate execution normally start within milliseconds through notifications. Delayed jobs and retries may start up to one polling interval after `run_at`. After a handler finishes, its claim loop immediately asks for another job without waiting for either mechanism. Lowering `poll_interval` improves scheduled-job precision at the cost of more idle database queries.

If the notification listener loses its connection, workers keep making progress through polling while the listener reconnects with exponential backoff, capped at 30 seconds. On reconnect it wakes all claim loops to catch up on anything missed during the outage.

### Notification cost at high enqueue rates

PostgreSQL takes a global exclusive lock while committing transactions that emit notifications because notifications must be delivered in commit order. At high concurrent enqueue rates, this can impose a commit-throughput ceiling even when the database still has spare CPU or I/O capacity. The same applies to an application transaction that enqueues a job because the whole transaction emits the notification. See [Postgres LISTEN/NOTIFY Actually Scales](https://www.dbos.dev/blog/postgres-listen-notify-scalability) for an analysis of this behavior.

Two properties of this library limit the impact. Notifications carry an empty payload, so PostgreSQL collapses them per transaction: inserting many jobs in one transaction wakes listeners once rather than once per job. Polling is also an independent correctness path, so notifications only affect latency, never whether a job runs.

Applications that enqueue at high rates should therefore batch. `Queue::enqueue_many` uses one transaction and at most one insert statement per job-key mode, so the batch pays the notification-related commit serialization once and produces a single wakeup. Multiple `enqueue_in` calls in one application transaction receive the same notification folding.

## Database connection and schema

Your application creates the connection pool, so it controls credentials, timeouts, and transport security. `pgspawn` does not force a TLS backend; applications can select one through SQLx or use the `pgspawn/tls-rustls` and `pgspawn/tls-native-tls` passthrough features.

When PostgreSQL is reached over an untrusted network, configure certificate verification with `sslmode=verify-full` and a trusted root certificate. SQLx otherwise defaults to `sslmode=prefer`, which can fall back to an unencrypted connection when TLS is unavailable.

The embedded SQLx migrations create a dedicated `pgspawn` schema containing `jobs`, `workers`, `crons`, and `schema_migrations`, along with the required indexes, triggers, and functions. PostgreSQL notifications use the database-wide `pgspawn_jobs` channel.

`pgspawn::db::migrate` creates and upgrades this schema. Applications that manage migration execution centrally can use `pgspawn::db::migrator()` instead. Pgspawn uses its own `pgspawn.schema_migrations` table, so its versions and checksums do not conflict with an application's SQLx migrations. Until the first release, schema changes may revise `0001_init.sql`; recreate development `pgspawn` schemas after checksum changes. Published releases will use append-only migrations.

## License

MIT. See [LICENSE](LICENSE).
