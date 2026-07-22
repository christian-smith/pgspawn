//! Integration tests against a real PostgreSQL server.
//!
//! Set `PGSPAWN_TEST_DATABASE_URL` (or `DATABASE_URL`) to a superuser connection string; each test creates and drops its own database. When neither variable is set every test skips.

use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use pgspawn::{
    CronJob, DailyJobSchedule, EnqueueOptions, EnqueueRequest, Job, JobKeyMode, JobStatus, Queue,
    Registry, Worker, WorkerConfig, db,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{
    PgPool,
    postgres::{PgListener, PgPoolOptions},
};
use uuid::Uuid;

fn admin_url() -> Option<String> {
    std::env::var("PGSPAWN_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()
}

fn with_database(url: &str, database: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((base, query)) => (base, Some(query)),
        None => (url, None),
    };
    let authority_start = base.find("://").expect("test database URL scheme") + 3;
    let base = match base[authority_start..].find('/') {
        Some(slash) => &base[..authority_start + slash],
        None => base,
    };
    match query {
        Some(query) => format!("{base}/{database}?{query}"),
        None => format!("{base}/{database}"),
    }
}

struct TestDb {
    admin_url: String,
    name: String,
    pool: PgPool,
}

impl TestDb {
    async fn create() -> Option<Self> {
        let admin_url = admin_url()?;
        let name = format!("pgspawn_test_{}", Uuid::new_v4().simple());
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("connect to admin database");
        sqlx::query(sqlx::AssertSqlSafe(format!(r#"CREATE DATABASE "{name}""#)))
            .execute(&admin)
            .await
            .expect("create test database");
        admin.close().await;

        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&with_database(&admin_url, &name))
            .await
            .expect("connect to test database");
        db::migrate(&pool).await.expect("run migrations");
        Some(Self {
            admin_url,
            name,
            pool,
        })
    }

    async fn drop(self) {
        self.pool.close().await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.admin_url)
            .await
            .expect("connect to admin database");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            r#"DROP DATABASE "{}" WITH (FORCE)"#,
            self.name
        )))
        .execute(&admin)
        .await
        .expect("drop test database");
        admin.close().await;
    }
}

macro_rules! require_db {
    () => {
        match TestDb::create().await {
            Some(db) => db,
            None => {
                eprintln!("skipping: set PGSPAWN_TEST_DATABASE_URL or DATABASE_URL");
                return;
            }
        }
    };
}

fn fast_config() -> WorkerConfig {
    WorkerConfig {
        concurrency: 4,
        poll_interval: Duration::from_millis(50),
        cron_poll_interval: Duration::from_millis(100),
        heartbeat_interval: Duration::from_millis(100),
        lock_renew_interval: Duration::from_millis(100),
        stale_after: Duration::from_millis(800),
        recovery_interval: Duration::from_millis(100),
        shutdown_grace_period: Duration::from_millis(300),
        finished_job_retention: None,
        ..WorkerConfig::default()
    }
}

struct RunningWorker {
    shutdown: tokio::sync::oneshot::Sender<()>,
    handle: tokio::task::JoinHandle<Result<(), pgspawn::WorkerError>>,
}

fn start_worker(pool: PgPool, registry: Registry, config: WorkerConfig) -> RunningWorker {
    start_worker_with_crons(pool, registry, config, Vec::new())
}

fn start_worker_with_crons(
    pool: PgPool,
    registry: Registry,
    config: WorkerConfig,
    crons: Vec<CronJob>,
) -> RunningWorker {
    let (shutdown, receiver) = tokio::sync::oneshot::channel::<()>();
    let handle = Worker::new(pool, registry)
        .with_config(config)
        .with_crons(crons)
        .start_with_shutdown(async move {
            let _ = receiver.await;
        });
    RunningWorker { shutdown, handle }
}

impl RunningWorker {
    async fn stop(self) {
        let _ = self.shutdown.send(());
        self.handle
            .await
            .expect("worker task join")
            .expect("worker run result");
    }
}

async fn wait_for_status(queue: &Queue, id: Uuid, status: JobStatus, timeout: Duration) -> Job {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(job) = queue.get(id).await.expect("get job")
            && job.status == status
        {
            return job;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for job {id} to reach {status}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_until<F, Fut>(timeout: Duration, description: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting until {description}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct CounterPayload {
    value: i64,
}

async fn wait_for_workers_ready(pool: &PgPool, expected: i64) {
    let pool = pool.clone();
    wait_until(Duration::from_secs(5), "workers registered", || {
        let pool = pool.clone();
        async move {
            let (workers,): (i64,) = sqlx::query_as("SELECT count(*) FROM pgspawn.workers")
                .fetch_one(&pool)
                .await
                .expect("count workers");
            workers >= expected
        }
    })
    .await;
    // Allow each worker's dedicated listener to finish LISTEN setup.
    tokio::time::sleep(Duration::from_millis(250)).await;
}

#[tokio::test]
async fn migrations_apply_and_are_idempotent() {
    let db = require_db!();

    db::migrate(&db.pool).await.expect("second migrate run");
    for table in ["jobs", "workers", "crons", "schema_migrations"] {
        let (exists,): (bool,) = sqlx::query_as("SELECT to_regclass('pgspawn.' || $1) IS NOT NULL")
            .bind(table)
            .fetch_one(&db.pool)
            .await
            .expect("check table");
        assert!(exists, "pgspawn.{table} should exist");
    }

    db.drop().await;
}

#[tokio::test]
async fn enqueue_runs_typed_job_to_success() {
    let db = require_db!();
    let counter = Arc::new(AtomicUsize::new(0));
    let registry = Registry::builder()
        .register_typed("count", {
            let counter = counter.clone();
            move |_job, payload: CounterPayload| {
                let counter = counter.clone();
                async move {
                    assert_eq!(payload.value, 7);
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), String>(())
                }
            }
        })
        .build();
    let worker = start_worker(db.pool.clone(), registry, fast_config());
    let queue = Queue::new(db.pool.clone());

    let id = queue
        .enqueue_typed("count", &CounterPayload { value: 7 })
        .await
        .expect("enqueue");
    let unclaimed = queue
        .enqueue("unregistered", json!({}))
        .await
        .expect("enqueue unregistered");

    let job = wait_for_status(&queue, id, JobStatus::Succeeded, Duration::from_secs(5)).await;
    assert_eq!(job.attempt, 1);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert!(job.finished_at.is_some());
    assert!(job.error.is_none());

    tokio::time::sleep(Duration::from_millis(200)).await;
    let unclaimed = queue.get(unclaimed).await.expect("get").expect("job row");
    assert_eq!(unclaimed.status, JobStatus::Queued);

    worker.stop().await;
    let (workers,): (i64,) = sqlx::query_as("SELECT count(*) FROM pgspawn.workers")
        .fetch_one(&db.pool)
        .await
        .expect("count workers");
    assert_eq!(workers, 0, "worker should deregister on shutdown");
    db.drop().await;
}

#[tokio::test]
async fn failing_job_backs_off_then_fails_permanently() {
    let db = require_db!();
    let registry = Registry::builder()
        .register("explode", |_job| async move { Err::<(), _>("boom") })
        .build();
    let worker = start_worker(db.pool.clone(), registry, fast_config());
    let queue = Queue::new(db.pool.clone());

    let started = tokio::time::Instant::now();
    let id = queue
        .enqueue_with_options(
            "explode",
            json!({}),
            EnqueueOptions {
                max_attempts: 2,
                ..EnqueueOptions::default()
            },
        )
        .await
        .expect("enqueue");

    let job = wait_for_status(&queue, id, JobStatus::Failed, Duration::from_secs(20)).await;
    assert_eq!(job.attempt, 2);
    assert_eq!(job.error.as_deref(), Some("boom"));
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "second attempt should wait for the exponential backoff"
    );

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn cancel_and_retry_round_trip() {
    let db = require_db!();
    let registry = Registry::builder()
        .register("later", |_job| async move { Ok::<(), String>(()) })
        .build();
    let worker = start_worker(db.pool.clone(), registry, fast_config());
    let queue = Queue::new(db.pool.clone());

    let id = queue
        .enqueue_with_options(
            "later",
            json!({}),
            EnqueueOptions {
                run_at: Some(jiff::Timestamp::now() + jiff::SignedDuration::from_hours(1)),
                ..EnqueueOptions::default()
            },
        )
        .await
        .expect("enqueue");

    assert!(queue.cancel(id).await.expect("cancel"));
    assert!(!queue.cancel(id).await.expect("second cancel"));
    let job = queue.get(id).await.expect("get").expect("job row");
    assert_eq!(job.status, JobStatus::Cancelled);

    assert!(queue.retry(id).await.expect("retry"));
    let job = wait_for_status(&queue, id, JobStatus::Succeeded, Duration::from_secs(5)).await;
    assert_eq!(job.attempt, 1);
    assert!(!queue.retry(id).await.expect("retry succeeded job"));

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn job_key_deduplicates_and_replaces_queued_jobs() {
    let db = require_db!();
    let queue = Queue::new(db.pool.clone());
    let delayed = EnqueueOptions {
        run_at: Some(jiff::Timestamp::now() + jiff::SignedDuration::from_hours(1)),
        job_key: Some("singleton".to_owned()),
        ..EnqueueOptions::default()
    };

    let first = queue
        .enqueue_with_options("keyed", json!({"version": 1}), delayed.clone())
        .await
        .expect("enqueue first");
    let second = queue
        .enqueue_with_options("keyed", json!({"version": 2}), delayed.clone())
        .await
        .expect("enqueue duplicate");
    assert_eq!(first, second, "active job key should deduplicate");
    let job = queue.get(first).await.expect("get").expect("job row");
    assert_eq!(job.payload, json!({"version": 1}));

    let replaced = queue
        .enqueue_with_options(
            "keyed",
            json!({"version": 3}),
            EnqueueOptions {
                job_key_mode: JobKeyMode::Replace,
                ..delayed.clone()
            },
        )
        .await
        .expect("enqueue replacement");
    assert_eq!(first, replaced);
    let job = queue.get(first).await.expect("get").expect("job row");
    assert_eq!(job.payload, json!({"version": 3}));

    assert!(queue.cancel(first).await.expect("cancel"));
    let third = queue
        .enqueue_with_options("keyed", json!({"version": 4}), delayed)
        .await
        .expect("enqueue after cancel");
    assert_ne!(first, third, "cancelled jobs should release their job key");

    db.drop().await;
}

#[tokio::test]
async fn named_queue_serializes_execution_across_concurrency() {
    let db = require_db!();
    let running = Arc::new(AtomicUsize::new(0));
    let max_running = Arc::new(AtomicUsize::new(0));
    let registry = Registry::builder()
        .register("serial", {
            let running = running.clone();
            let max_running = max_running.clone();
            move |_job| {
                let running = running.clone();
                let max_running = max_running.clone();
                async move {
                    let current = running.fetch_add(1, Ordering::SeqCst) + 1;
                    max_running.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    running.fetch_sub(1, Ordering::SeqCst);
                    Ok::<(), String>(())
                }
            }
        })
        .build();
    let worker = start_worker(db.pool.clone(), registry, fast_config());
    let queue = Queue::new(db.pool.clone());

    let mut ids = Vec::new();
    for index in 0..3 {
        let id = queue
            .enqueue_with_options(
                "serial",
                json!({ "index": index }),
                EnqueueOptions {
                    queue_name: Some("only-one".to_owned()),
                    ..EnqueueOptions::default()
                },
            )
            .await
            .expect("enqueue");
        ids.push(id);
    }

    for id in ids {
        wait_for_status(&queue, id, JobStatus::Succeeded, Duration::from_secs(10)).await;
    }
    assert_eq!(
        max_running.load(Ordering::SeqCst),
        1,
        "queue name should serialize execution"
    );

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn delayed_job_runs_after_due_time() {
    let db = require_db!();
    let registry = Registry::builder()
        .register("delayed", |_job| async move { Ok::<(), String>(()) })
        .build();
    let worker = start_worker(db.pool.clone(), registry, fast_config());
    wait_for_workers_ready(&db.pool, 1).await;
    let queue = Queue::new(db.pool.clone());

    let id = queue
        .enqueue_with_options(
            "delayed",
            json!({}),
            EnqueueOptions {
                run_at: Some(jiff::Timestamp::now() + jiff::SignedDuration::from_millis(1500)),
                ..EnqueueOptions::default()
            },
        )
        .await
        .expect("enqueue");

    let job = wait_for_status(&queue, id, JobStatus::Succeeded, Duration::from_secs(6)).await;
    assert!(
        job.started_at.expect("started_at") >= job.run_at,
        "delayed job must not start before run_at"
    );

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn transactional_enqueue_follows_the_caller_transaction() {
    let db = require_db!();
    let registry = Registry::builder()
        .register("transactional", |_job| async move { Ok::<(), String>(()) })
        .build();
    let worker = start_worker(db.pool.clone(), registry, fast_config());
    let queue = Queue::new(db.pool.clone());

    let mut rolled_back = db.pool.begin().await.expect("begin");
    let discarded = queue
        .enqueue_in(&mut *rolled_back, "transactional", json!({ "keep": false }))
        .await
        .expect("enqueue in transaction");
    rolled_back.rollback().await.expect("rollback");
    assert!(
        queue.get(discarded).await.expect("get").is_none(),
        "a rolled back transaction must not leave an enqueued job"
    );

    let mut committed = db.pool.begin().await.expect("begin");
    let kept = queue
        .enqueue_in(&mut *committed, "transactional", json!({ "keep": true }))
        .await
        .expect("enqueue in transaction");
    assert!(
        queue.get(kept).await.expect("get").is_none(),
        "an uncommitted job must not be visible to other connections"
    );
    committed.commit().await.expect("commit");

    wait_for_status(&queue, kept, JobStatus::Succeeded, Duration::from_secs(5)).await;

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn enqueue_many_runs_every_job_and_preserves_request_order() {
    let db = require_db!();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let registry = Registry::builder()
        .register_typed("batched", {
            let seen = seen.clone();
            move |_job, payload: CounterPayload| {
                let seen = seen.clone();
                async move {
                    seen.lock().expect("seen lock").push(payload.value);
                    Ok::<(), String>(())
                }
            }
        })
        .build();
    let worker = start_worker(db.pool.clone(), registry, fast_config());
    let queue = Queue::new(db.pool.clone());

    assert!(
        queue
            .enqueue_many(&[])
            .await
            .expect("empty batch")
            .is_empty(),
        "an empty batch should not touch the database"
    );

    let requests: Vec<EnqueueRequest> = (0..50)
        .map(|value| {
            EnqueueRequest::typed("batched", &CounterPayload { value }).expect("typed request")
        })
        .collect();
    let ids = queue.enqueue_many(&requests).await.expect("enqueue many");
    assert_eq!(ids.len(), 50);

    for (index, id) in ids.iter().enumerate() {
        let job = wait_for_status(&queue, *id, JobStatus::Succeeded, Duration::from_secs(10)).await;
        assert_eq!(
            job.payload,
            json!({ "value": index as i64 }),
            "returned ids must line up with the requests that produced them"
        );
    }
    let mut values = seen.lock().expect("seen lock").clone();
    values.sort_unstable();
    assert_eq!(values, (0..50).collect::<Vec<i64>>());

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn enqueue_many_applies_job_key_rules_and_rejects_batch_duplicates() {
    let db = require_db!();
    let queue = Queue::new(db.pool.clone());
    let delayed = || EnqueueOptions {
        run_at: Some(jiff::Timestamp::now() + jiff::SignedDuration::from_hours(1)),
        ..EnqueueOptions::default()
    };

    let existing = queue
        .enqueue_with_options(
            "keyed",
            json!({ "version": 1 }),
            EnqueueOptions {
                job_key: Some("shared".to_owned()),
                ..delayed()
            },
        )
        .await
        .expect("enqueue existing");
    let preserved_run_at = jiff::Timestamp::now() + jiff::SignedDuration::from_hours(2);
    let preserved = queue
        .enqueue_with_options(
            "keyed",
            json!({ "version": 1 }),
            EnqueueOptions {
                run_at: Some(preserved_run_at),
                job_key: Some("preserved".to_owned()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .expect("enqueue preserved");

    let duplicate = queue
        .enqueue_many(&[
            EnqueueRequest::new("keyed", json!({})).with_options(EnqueueOptions {
                job_key: Some("same".to_owned()),
                ..delayed()
            }),
            EnqueueRequest::new("keyed", json!({})).with_options(EnqueueOptions {
                job_key: Some("same".to_owned()),
                ..delayed()
            }),
        ])
        .await;
    assert!(
        matches!(duplicate, Err(pgspawn::EnqueueError::DuplicateJobKey(key)) if key == "same"),
        "a repeated job key within one batch must be rejected"
    );
    let (inserted,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM pgspawn.jobs WHERE job_key = 'same'")
            .fetch_one(&db.pool)
            .await
            .expect("count");
    assert_eq!(inserted, 0, "a rejected batch must insert nothing");

    let ids = queue
        .enqueue_many(&[
            // Deduplicates against the job already queued under this key.
            EnqueueRequest::new("keyed", json!({ "version": 2 })).with_options(EnqueueOptions {
                job_key: Some("shared".to_owned()),
                ..delayed()
            }),
            // Uses Replace mode for a new keyed job.
            EnqueueRequest::new("keyed", json!({ "version": 2 })).with_options(EnqueueOptions {
                job_key: Some("replaceable".to_owned()),
                job_key_mode: JobKeyMode::Replace,
                ..delayed()
            }),
            EnqueueRequest::new("keyed", json!({ "version": 2 })).with_options(EnqueueOptions {
                run_at: Some(jiff::Timestamp::now() + jiff::SignedDuration::from_hours(5)),
                job_key: Some("preserved".to_owned()),
                job_key_mode: JobKeyMode::PreserveRunAt,
                ..EnqueueOptions::default()
            }),
            EnqueueRequest::new("keyed", json!({ "version": 1 })),
        ])
        .await
        .expect("enqueue many");

    assert_eq!(
        ids[0], existing,
        "an existing job key must return its job id"
    );
    let job = queue.get(ids[0]).await.expect("get").expect("job");
    assert_eq!(job.payload, json!({ "version": 1 }), "payload preserved");
    assert_eq!(ids[2], preserved);
    let job = queue.get(ids[2]).await.expect("get").expect("job");
    assert_eq!(job.payload, json!({ "version": 2 }));
    assert_eq!(
        job.run_at.as_second(),
        preserved_run_at.as_second(),
        "batch preserve mode must keep the current schedule"
    );

    let replaced = queue
        .enqueue_many(&[
            EnqueueRequest::new("keyed", json!({ "version": 3 })).with_options(EnqueueOptions {
                job_key: Some("replaceable".to_owned()),
                job_key_mode: JobKeyMode::Replace,
                ..delayed()
            }),
        ])
        .await
        .expect("enqueue replacement");
    assert_eq!(replaced[0], ids[1], "replacement keeps the existing job id");
    let job = queue.get(ids[1]).await.expect("get").expect("job");
    assert_eq!(job.payload, json!({ "version": 3 }), "payload replaced");

    db.drop().await;
}

#[tokio::test]
async fn enqueue_many_in_follows_the_caller_transaction() {
    let db = require_db!();
    let queue = Queue::new(db.pool.clone());
    let requests: Vec<EnqueueRequest> = (0..5)
        .map(|value| {
            EnqueueRequest::typed("batched", &CounterPayload { value }).expect("typed request")
        })
        .collect();

    let mut transaction = db.pool.begin().await.expect("begin");
    let ids = queue
        .enqueue_many_in(&mut transaction, &requests)
        .await
        .expect("enqueue many in transaction");
    transaction.rollback().await.expect("rollback");

    for id in ids {
        assert!(
            queue.get(id).await.expect("get").is_none(),
            "a rolled back batch must leave no jobs"
        );
    }

    db.drop().await;
}

#[tokio::test]
async fn enqueue_many_in_rolls_back_a_mixed_mode_failure_on_a_bare_connection() {
    let db = require_db!();
    let queue = Queue::new(db.pool.clone());
    let mut connection = db.pool.acquire().await.expect("acquire connection");
    let requests = [
        EnqueueRequest::new("batched", json!({})).with_options(EnqueueOptions {
            job_key: Some("valid-first-group".to_owned()),
            ..EnqueueOptions::default()
        }),
        EnqueueRequest::new("batched", json!({})).with_options(EnqueueOptions {
            max_attempts: 0,
            job_key: Some("invalid-second-group".to_owned()),
            job_key_mode: JobKeyMode::Replace,
            ..EnqueueOptions::default()
        }),
    ];

    let result = queue.enqueue_many_in(&mut connection, &requests).await;
    assert!(
        matches!(result, Err(pgspawn::EnqueueError::Database(_))),
        "the invalid max_attempts must reject the batch"
    );
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM pgspawn.jobs WHERE name = 'batched'")
            .fetch_one(&mut *connection)
            .await
            .expect("count jobs after rollback");
    assert_eq!(
        count, 0,
        "an error in a later job-key mode must roll back earlier statements"
    );

    drop(connection);
    db.drop().await;
}

#[tokio::test]
async fn bulk_changes_in_one_transaction_send_a_single_notification() {
    let db = require_db!();
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("pgspawn_jobs").await.expect("listen");

    sqlx::query(
        "INSERT INTO pgspawn.jobs (name, status) SELECT 'bulk', 'queued' FROM generate_series(1, 25)",
    )
    .execute(&db.pool)
    .await
    .expect("bulk insert");

    tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("notification arrives")
        .expect("receive notification");
    let extra = tokio::time::timeout(Duration::from_millis(500), listener.recv()).await;
    assert!(
        extra.is_err(),
        "rows changed in one transaction should collapse into a single notification"
    );

    drop(listener);
    db.drop().await;
}

#[tokio::test]
async fn job_key_mode_preserve_run_at_refreshes_payload_without_postponing() {
    let db = require_db!();
    let queue = Queue::new(db.pool.clone());
    let scheduled = jiff::Timestamp::now() + jiff::SignedDuration::from_hours(1);

    let id = queue
        .enqueue_with_options(
            "debounced",
            json!({ "version": 1 }),
            EnqueueOptions {
                run_at: Some(scheduled),
                job_key: Some("debounce".to_owned()),
                job_key_mode: JobKeyMode::PreserveRunAt,
                ..EnqueueOptions::default()
            },
        )
        .await
        .expect("enqueue");

    let repeated = queue
        .enqueue_with_options(
            "debounced",
            json!({ "version": 2 }),
            EnqueueOptions {
                run_at: Some(jiff::Timestamp::now() + jiff::SignedDuration::from_hours(5)),
                job_key: Some("debounce".to_owned()),
                job_key_mode: JobKeyMode::PreserveRunAt,
                ..EnqueueOptions::default()
            },
        )
        .await
        .expect("enqueue again");

    assert_eq!(repeated, id);
    let job = queue.get(id).await.expect("get").expect("job");
    assert_eq!(job.payload, json!({ "version": 2 }), "payload refreshed");
    assert_eq!(
        job.run_at.as_second(),
        scheduled.as_second(),
        "preserve_run_at must not postpone the original schedule"
    );

    // Replace mode does move the schedule.
    let postponed = jiff::Timestamp::now() + jiff::SignedDuration::from_hours(5);
    queue
        .enqueue_with_options(
            "debounced",
            json!({ "version": 3 }),
            EnqueueOptions {
                run_at: Some(postponed),
                job_key: Some("debounce".to_owned()),
                job_key_mode: JobKeyMode::Replace,
                ..EnqueueOptions::default()
            },
        )
        .await
        .expect("replace");
    let job = queue.get(id).await.expect("get").expect("job");
    assert_eq!(job.run_at.as_second(), postponed.as_second());

    db.drop().await;
}

#[tokio::test]
async fn job_key_modes_never_replace_a_running_job_definition() {
    let db = require_db!();
    let queue = Queue::new(db.pool.clone());
    let original_run_at = jiff::Timestamp::now() - jiff::SignedDuration::from_secs(60);
    let (id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO pgspawn.jobs (name, status, payload, queue_name, priority, run_at, max_attempts, job_key, attempt, locked_by, locked_at)
        VALUES ('original', 'running', '{\"version\":1}', 'serial', 7, $1, 9, 'running-key', 1, 'worker:0:lock', now())
        RETURNING id",
    )
    .bind(jiff_sqlx::Timestamp::from(original_run_at))
    .fetch_one(&db.pool)
    .await
    .expect("insert running job");

    for mode in [
        JobKeyMode::Dedupe,
        JobKeyMode::Replace,
        JobKeyMode::PreserveRunAt,
    ] {
        let returned = queue
            .enqueue_with_options(
                "replacement",
                json!({ "version": 2 }),
                EnqueueOptions {
                    queue_name: Some("other-queue".to_owned()),
                    priority: -10,
                    run_at: Some(jiff::Timestamp::now()),
                    max_attempts: 2,
                    job_key: Some("running-key".to_owned()),
                    job_key_mode: mode,
                },
            )
            .await
            .expect("enqueue conflicting job");
        assert_eq!(returned, id);
    }

    let job = queue.get(id).await.expect("get").expect("running job");
    assert_eq!(job.name, "original");
    assert_eq!(job.status, JobStatus::Running);
    assert_eq!(job.payload, json!({ "version": 1 }));
    assert_eq!(job.queue_name.as_deref(), Some("serial"));
    assert_eq!(job.priority, 7);
    assert_eq!(job.run_at.as_second(), original_run_at.as_second());
    assert_eq!(job.max_attempts, 9);
    assert_eq!(job.locked_by.as_deref(), Some("worker:0:lock"));

    db.drop().await;
}

#[tokio::test]
async fn permanently_fail_and_complete_retire_unfinished_jobs() {
    let db = require_db!();
    let queue = Queue::new(db.pool.clone());
    let delayed = || EnqueueOptions {
        run_at: Some(jiff::Timestamp::now() + jiff::SignedDuration::from_hours(1)),
        ..EnqueueOptions::default()
    };

    let doomed = queue
        .enqueue_with_options("poison", json!({}), delayed())
        .await
        .expect("enqueue");
    assert!(
        queue
            .permanently_fail(doomed, "operator stopped it")
            .await
            .expect("permanently fail")
    );
    let job = queue.get(doomed).await.expect("get").expect("job");
    assert_eq!(job.status, JobStatus::Failed);
    assert_eq!(job.error.as_deref(), Some("operator stopped it"));
    assert!(
        !queue
            .permanently_fail(doomed, "again")
            .await
            .expect("second call"),
        "a finished job cannot be failed again"
    );

    let retired = queue
        .enqueue_with_options("obsolete", json!({}), delayed())
        .await
        .expect("enqueue");
    assert!(queue.complete(retired).await.expect("complete"));
    let job = queue.get(retired).await.expect("get").expect("job");
    assert_eq!(job.status, JobStatus::Succeeded);
    assert!(job.error.is_none());

    db.drop().await;
}

#[tokio::test]
async fn operator_completion_discards_a_running_handler_and_releases_its_slot() {
    let db = require_db!();
    let registry = Registry::builder()
        .register("blocked", |_job| async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<(), String>(())
        })
        .register("next", |_job| async move { Ok::<(), String>(()) })
        .build();
    let worker = start_worker(
        db.pool.clone(),
        registry,
        WorkerConfig {
            concurrency: 1,
            ..fast_config()
        },
    );
    let queue = Queue::new(db.pool.clone());

    let blocked = queue.enqueue("blocked", json!({})).await.expect("enqueue");
    wait_for_status(&queue, blocked, JobStatus::Running, Duration::from_secs(5)).await;
    assert!(queue.complete(blocked).await.expect("complete running job"));

    let next = queue
        .enqueue("next", json!({}))
        .await
        .expect("enqueue next");
    wait_for_status(&queue, next, JobStatus::Succeeded, Duration::from_secs(3)).await;
    let retired = queue.get(blocked).await.expect("get").expect("job");
    assert_eq!(retired.status, JobStatus::Succeeded);
    assert!(retired.locked_by.is_none());

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn force_unlock_worker_releases_jobs_and_deregisters() {
    let db = require_db!();
    let queue = Queue::new(db.pool.clone());

    sqlx::query("INSERT INTO pgspawn.workers (id, task_names) VALUES ('departed', '{stranded}')")
        .execute(&db.pool)
        .await
        .expect("insert worker");
    let (stranded,): (Uuid,) = sqlx::query_as(
        "INSERT INTO pgspawn.jobs (name, status, locked_by, locked_at, attempt)
        VALUES ('stranded', 'running', 'departed:0:lock', now(), 1)
        RETURNING id",
    )
    .fetch_one(&db.pool)
    .await
    .expect("insert running job");

    let workers = queue.workers().await.expect("list workers");
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].id, "departed");
    assert_eq!(workers[0].task_names, vec!["stranded".to_owned()]);

    let released = queue
        .force_unlock_worker("departed")
        .await
        .expect("force unlock");
    assert_eq!(released, 1);
    let job = queue.get(stranded).await.expect("get").expect("job");
    assert_eq!(job.status, JobStatus::Queued);
    assert!(job.locked_by.is_none());
    assert!(queue.workers().await.expect("list workers").is_empty());

    let counts = queue.counts().await.expect("counts");
    assert_eq!(counts.queued, 1);
    assert_eq!(counts.running, 0);

    db.drop().await;
}

#[tokio::test]
async fn reschedule_moves_queued_job_and_notifies_workers() {
    let db = require_db!();
    let registry = Registry::builder()
        .register("rescheduled", |_job| async move { Ok::<(), String>(()) })
        .build();
    // A five-second poll makes the notification path observable within the three-second deadline.
    let worker = start_worker(
        db.pool.clone(),
        registry,
        WorkerConfig {
            poll_interval: Duration::from_secs(5),
            ..fast_config()
        },
    );
    wait_for_workers_ready(&db.pool, 1).await;
    let queue = Queue::new(db.pool.clone());

    let id = queue
        .enqueue_with_options(
            "rescheduled",
            json!({}),
            EnqueueOptions {
                run_at: Some(jiff::Timestamp::now() + jiff::SignedDuration::from_hours(1)),
                ..EnqueueOptions::default()
            },
        )
        .await
        .expect("enqueue");

    assert!(
        queue
            .reschedule(id, jiff::Timestamp::now())
            .await
            .expect("reschedule")
    );
    wait_for_status(&queue, id, JobStatus::Succeeded, Duration::from_secs(3)).await;
    assert!(
        !queue
            .reschedule(id, jiff::Timestamp::now())
            .await
            .expect("reschedule finished job"),
        "finished jobs must not be rescheduled"
    );

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn named_queue_completion_notifies_other_worker() {
    let db = require_db!();
    let first_registry = Registry::builder()
        .register("first", |_job| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<(), String>(())
        })
        .build();
    let second_registry = Registry::builder()
        .register("second", |_job| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<(), String>(())
        })
        .build();
    let slow_config = || WorkerConfig {
        concurrency: 1,
        poll_interval: Duration::from_secs(5),
        ..fast_config()
    };
    let first_worker = start_worker(db.pool.clone(), first_registry, slow_config());
    let second_worker = start_worker(db.pool.clone(), second_registry, slow_config());
    wait_for_workers_ready(&db.pool, 2).await;
    let queue = Queue::new(db.pool.clone());

    let first = queue
        .enqueue_with_options(
            "first",
            json!({}),
            EnqueueOptions {
                queue_name: Some("cross-process".to_owned()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .expect("enqueue first");
    let second = queue
        .enqueue_with_options(
            "second",
            json!({}),
            EnqueueOptions {
                queue_name: Some("cross-process".to_owned()),
                ..EnqueueOptions::default()
            },
        )
        .await
        .expect("enqueue second");

    let started = tokio::time::Instant::now();
    for id in [first, second] {
        wait_for_status(&queue, id, JobStatus::Succeeded, Duration::from_secs(4)).await;
    }
    assert!(
        started.elapsed() < Duration::from_secs(4),
        "serialized queue should advance through a cross-process completion notification"
    );

    first_worker.stop().await;
    second_worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn queue_completion_notification_payload_is_bounded() {
    let db = require_db!();

    // Dropping this index isolates NOTIFY's payload limit from PostgreSQL's B-tree key-size limit.
    sqlx::query("DROP INDEX pgspawn.jobs_queue_running_idx")
        .execute(&db.pool)
        .await
        .expect("drop queue index");
    let queue_name = "q".repeat(8_000);
    let (id,): (Uuid,) = sqlx::query_as(
        "INSERT INTO pgspawn.jobs (name, status, queue_name, locked_by, locked_at) VALUES ('oversized-queue', 'running', $1, 'test-lock', now()) RETURNING id",
    )
    .bind(queue_name)
    .fetch_one(&db.pool)
    .await
    .expect("insert running job");

    let updated = sqlx::query(
        "UPDATE pgspawn.jobs SET status = 'succeeded', finished_at = now() WHERE id = $1",
    )
    .bind(id)
    .execute(&db.pool)
    .await
    .expect("complete job");
    assert_eq!(updated.rows_affected(), 1);

    db.drop().await;
}

#[tokio::test]
async fn priority_orders_available_work() {
    let db = require_db!();
    let order = Arc::new(Mutex::new(Vec::new()));
    let registry = Registry::builder()
        .register_typed("prioritized", {
            let order = order.clone();
            move |_job, payload: CounterPayload| {
                let order = order.clone();
                async move {
                    order.lock().expect("order lock").push(payload.value);
                    Ok::<(), String>(())
                }
            }
        })
        .build();
    let queue = Queue::new(db.pool.clone());

    let mut ids = Vec::new();
    for priority in [10, 0, 5] {
        let id = queue
            .enqueue_typed_with_options(
                "prioritized",
                &CounterPayload {
                    value: priority as i64,
                },
                EnqueueOptions {
                    priority,
                    ..EnqueueOptions::default()
                },
            )
            .await
            .expect("enqueue");
        ids.push(id);
    }

    let worker = start_worker(
        db.pool.clone(),
        registry,
        WorkerConfig {
            concurrency: 1,
            ..fast_config()
        },
    );
    for id in ids {
        wait_for_status(&queue, id, JobStatus::Succeeded, Duration::from_secs(5)).await;
    }
    assert_eq!(
        order.lock().expect("order lock").clone(),
        vec![0, 5, 10],
        "lower priority values should run first"
    );

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn recovers_jobs_from_dead_workers_and_stale_locks() {
    let db = require_db!();
    let queue = Queue::new(db.pool.clone());

    sqlx::query(
        "INSERT INTO pgspawn.workers (id, task_names, last_heartbeat_at)
        VALUES ('ghost', '{rescue}', now() - interval '1 hour')",
    )
    .execute(&db.pool)
    .await
    .expect("insert dead worker");
    let (dead_worker_job,): (Uuid,) = sqlx::query_as(
        "INSERT INTO pgspawn.jobs (name, status, locked_by, locked_at, attempt)
        VALUES ('rescue', 'running', 'ghost:0:lock', now() - interval '1 hour', 1)
        RETURNING id",
    )
    .fetch_one(&db.pool)
    .await
    .expect("insert dead worker job");
    let (stale_job,): (Uuid,) = sqlx::query_as(
        "INSERT INTO pgspawn.jobs (name, status, locked_by, locked_at, attempt)
        VALUES ('rescue', 'running', 'vanished:0:lock', now() - interval '1 hour', 1)
        RETURNING id",
    )
    .fetch_one(&db.pool)
    .await
    .expect("insert stale job");

    let registry = Registry::builder()
        .register("rescue", |_job| async move { Ok::<(), String>(()) })
        .build();
    let worker = start_worker(db.pool.clone(), registry, fast_config());

    for id in [dead_worker_job, stale_job] {
        let job = wait_for_status(&queue, id, JobStatus::Succeeded, Duration::from_secs(5)).await;
        assert_eq!(
            job.attempt, 2,
            "recovered run should count as a new attempt"
        );
    }
    let (ghosts,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM pgspawn.workers WHERE id = 'ghost'")
            .fetch_one(&db.pool)
            .await
            .expect("count ghosts");
    assert_eq!(ghosts, 0, "dead worker registration should be removed");

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn handlers_observe_shutdown_and_can_finish_early() {
    let db = require_db!();
    let wound_down = Arc::new(AtomicUsize::new(0));
    let registry = Registry::builder()
        .register("cooperative", {
            let wound_down = wound_down.clone();
            move |_job| {
                let wound_down = wound_down.clone();
                async move {
                    assert!(
                        !pgspawn::is_shutting_down(),
                        "a freshly claimed job should not see shutdown yet"
                    );
                    // Observe shutdown and exit within the grace period.
                    pgspawn::shutdown_requested().await;
                    assert!(pgspawn::is_shutting_down());
                    wound_down.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), String>(())
                }
            }
        })
        .build();
    let worker = start_worker(
        db.pool.clone(),
        registry,
        WorkerConfig {
            shutdown_grace_period: Duration::from_secs(5),
            ..fast_config()
        },
    );
    let queue = Queue::new(db.pool.clone());

    let id = queue
        .enqueue("cooperative", json!({}))
        .await
        .expect("enqueue");
    wait_for_status(&queue, id, JobStatus::Running, Duration::from_secs(5)).await;

    worker.stop().await;
    assert_eq!(
        wound_down.load(Ordering::SeqCst),
        1,
        "the handler should observe shutdown and return"
    );
    let job = queue.get(id).await.expect("get").expect("job");
    assert_eq!(
        job.status,
        JobStatus::Succeeded,
        "a handler that returns during the grace period still completes its job"
    );

    db.drop().await;
}

#[tokio::test]
async fn handlers_can_request_retry_during_shutdown() {
    let db = require_db!();
    let registry = Registry::builder()
        .register("cooperative-retry", |_job| async move {
            pgspawn::shutdown_requested().await;
            Err::<(), _>("shutdown requested")
        })
        .build();
    let worker = start_worker(
        db.pool.clone(),
        registry,
        WorkerConfig {
            shutdown_grace_period: Duration::from_secs(5),
            ..fast_config()
        },
    );
    let queue = Queue::new(db.pool.clone());

    let id = queue
        .enqueue("cooperative-retry", json!({}))
        .await
        .expect("enqueue");
    wait_for_status(&queue, id, JobStatus::Running, Duration::from_secs(5)).await;

    worker.stop().await;
    let job = queue.get(id).await.expect("get").expect("job");
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(job.attempt, 1);
    assert_eq!(job.error.as_deref(), Some("shutdown requested"));
    assert!(job.locked_by.is_none());

    db.drop().await;
}

#[tokio::test]
async fn shutdown_helpers_are_inert_outside_a_handler() {
    assert!(!pgspawn::is_shutting_down());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), pgspawn::shutdown_requested())
            .await
            .is_err(),
        "shutdown_requested must never resolve outside a handler"
    );
}

#[tokio::test]
async fn graceful_shutdown_releases_unfinished_jobs() {
    let db = require_db!();
    let registry = Registry::builder()
        .register("stuck", |_job| async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            Ok::<(), String>(())
        })
        .build();
    let worker = start_worker(db.pool.clone(), registry, fast_config());
    let queue = Queue::new(db.pool.clone());

    let id = queue.enqueue("stuck", json!({})).await.expect("enqueue");
    wait_for_status(&queue, id, JobStatus::Running, Duration::from_secs(5)).await;

    worker.stop().await;
    let job = queue.get(id).await.expect("get").expect("job row");
    assert_eq!(job.status, JobStatus::Queued);
    assert_eq!(job.attempt, 1);
    assert_eq!(
        job.error.as_deref(),
        Some("released by shutting down worker")
    );
    assert!(job.locked_by.is_none());

    db.drop().await;
}

#[tokio::test]
async fn interval_cron_coordinates_across_workers() {
    let db = require_db!();
    let counter = Arc::new(AtomicUsize::new(0));
    let registry = Registry::builder()
        .register("tick", {
            let counter = counter.clone();
            move |_job| {
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), String>(())
                }
            }
        })
        .build();
    let crons = vec![CronJob::interval("tick", "tick", Duration::from_secs(1))];

    let first = start_worker_with_crons(
        db.pool.clone(),
        registry.clone(),
        fast_config(),
        crons.clone(),
    );
    let second = start_worker_with_crons(db.pool.clone(), registry, fast_config(), crons);

    let pool = db.pool.clone();
    wait_until(Duration::from_secs(10), "two cron ticks enqueued", || {
        let pool = pool.clone();
        async move {
            let (ticks,): (i64,) =
                sqlx::query_as("SELECT count(*) FROM pgspawn.jobs WHERE name = 'tick'")
                    .fetch_one(&pool)
                    .await
                    .expect("count ticks");
            ticks >= 2
        }
    })
    .await;

    let (total, distinct): (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(DISTINCT run_at) FROM pgspawn.jobs WHERE name = 'tick'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("count tick occurrences");
    assert_eq!(
        total, distinct,
        "each interval boundary should enqueue exactly one job across workers"
    );

    first.stop().await;
    second.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn timezone_daily_cron_catches_up_on_start() {
    let db = require_db!();
    let registry = Registry::builder()
        .register("report", |_job| async move { Ok::<(), String>(()) })
        .build();
    let cron = CronJob::daily_in_timezone(
        "daily-report",
        "report",
        DailyJobSchedule::new(0, 0),
        "America/New_York",
    )
    .expect("valid time zone")
    .catch_up_on_start();
    let worker = start_worker_with_crons(db.pool.clone(), registry, fast_config(), vec![cron]);
    let queue = Queue::new(db.pool.clone());

    let pool = db.pool.clone();
    wait_until(Duration::from_secs(10), "catch-up job enqueued", || {
        let pool = pool.clone();
        async move {
            let (count,): (i64,) =
                sqlx::query_as("SELECT count(*) FROM pgspawn.jobs WHERE name = 'report'")
                    .fetch_one(&pool)
                    .await
                    .expect("count report jobs");
            count >= 1
        }
    })
    .await;

    let (id,): (Uuid,) = sqlx::query_as("SELECT id FROM pgspawn.jobs WHERE name = 'report'")
        .fetch_one(&db.pool)
        .await
        .expect("fetch report job");
    let job = wait_for_status(&queue, id, JobStatus::Succeeded, Duration::from_secs(5)).await;

    let new_york = jiff::tz::TimeZone::get("America/New_York").expect("tzdb available");
    let local = new_york.to_datetime(job.run_at);
    assert_eq!(
        (local.hour(), local.minute(), local.second()),
        (0, 0, 0),
        "catch-up occurrence should be local midnight in New York"
    );

    let (consistent,): (bool,) = sqlx::query_as(
        "SELECT last_run_at IS NOT NULL AND next_run_at > last_run_at
        FROM pgspawn.crons WHERE identifier = 'daily-report'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("fetch cron row");
    assert!(consistent, "cron row should track last and next run");

    worker.stop().await;
    db.drop().await;
}

#[tokio::test]
async fn finished_jobs_are_pruned_after_retention() {
    let db = require_db!();
    let registry = Registry::builder()
        .register("ephemeral", |_job| async move { Ok::<(), String>(()) })
        .build();
    let worker = start_worker(
        db.pool.clone(),
        registry,
        WorkerConfig {
            finished_job_retention: Some(Duration::ZERO),
            ..fast_config()
        },
    );
    let queue = Queue::new(db.pool.clone());

    let id = queue
        .enqueue("ephemeral", json!({}))
        .await
        .expect("enqueue");

    let queue_ref = &queue;
    wait_until(
        Duration::from_secs(5),
        "finished job pruned",
        || async move { queue_ref.get(id).await.expect("get job").is_none() },
    )
    .await;

    worker.stop().await;
    db.drop().await;
}
