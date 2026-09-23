# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## Unreleased

### Changed

- Idle claim loops sleep until the earliest runnable job deadline or the 30-second safety poll instead of querying PostgreSQL every two seconds.
- Routine cron enqueue, job start, and job success events are logged at `DEBUG`; startup, summaries, retries, and failures retain their existing levels.
- **Breaking:** cron schedules run only in production and staging unless `CronJob::environments` selects another set, and a worker with cron schedules refuses to start unless `WorkerConfig::environment` is set. Set `environment: Some(Environment::Production)` on deployed workers and opt schedules into other environments with `CronJob::environments`.

### Added

- `Environment`, `WorkerConfig::environment`, and `CronJob::environments` restrict cron schedules to production, staging, development, local, or test workers.
- `RegistryBuilder::register_infallible` and `RegistryBuilder::register_typed_infallible` for handlers that absorb their own failures and return `()`, removing the `Ok::<(), Infallible>(())` boilerplate common to periodic log-and-continue jobs.
- `RegistryBuilder::register_with`, `register_typed_with`, `register_infallible_with`, and `register_typed_infallible_with` so handlers receive a cloned application state without the double-`clone` closure dance.
- `worker_started_at()` reports when the current worker began running, for handlers that skip work until the surrounding process has finished starting. Returns `None` outside a handler.
- The worker logs a startup warning for each interval cron shorter than `WorkerConfig::cron_poll_interval`, since such schedules are silently capped at the poll rate.
- Daily, monthly, and interval `CronJob` constructors set `job_key` to the cron identifier so a still-running or still-queued occurrence is not stacked with the next slot. Override with `.options(...)` when a schedule should allow overlap.
- Durable PostgreSQL-backed jobs with delayed execution, priorities, exponential retries, job keys, named serial queues, and concurrent Tokio workers.
- Typed and untyped enqueue APIs, transactional enqueueing through any SQLx PostgreSQL executor, and atomic batch enqueueing with ids returned in request order.
- `Dedupe`, `Replace`, and `PreserveRunAt` job-key modes for controlling how queued jobs with the same key are handled.
- Low-latency worker wakeups through PostgreSQL `LISTEN`/`NOTIFY`, with deadline-aware sleeping and safety polling for delayed work, missed notifications, and listener outages.
- Daily and monthly schedules in UTC or any IANA time zone, plus epoch-aligned interval schedules coordinated through PostgreSQL.
- Worker heartbeats, lock renewal, stale-work recovery, listener reconnection, graceful shutdown, and shutdown-aware handlers.
- Job management and observability APIs for cancellation, retrying, rescheduling, completion, permanent failure, worker recovery, worker inspection, status counts, and recent jobs.
- Configurable finished-job retention with automatic and manual pruning.
- Structured job lifecycle logging and per-job tracing spans.
- Jiff timestamps in the public API and embedded SQLx migrations for a dedicated `pgspawn` schema.
