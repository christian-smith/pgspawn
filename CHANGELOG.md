# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## Unreleased

### Added

- Durable PostgreSQL-backed jobs with delayed execution, priorities, exponential retries, job keys, named serial queues, and concurrent Tokio workers.
- Typed and untyped enqueue APIs, transactional enqueueing through any SQLx PostgreSQL executor, and atomic batch enqueueing with ids returned in request order.
- `Dedupe`, `Replace`, and `PreserveRunAt` job-key modes for controlling how queued jobs with the same key are handled.
- Low-latency worker wakeups through PostgreSQL `LISTEN`/`NOTIFY`, with fixed polling for delayed work, missed notifications, and listener outages.
- Daily and monthly schedules in UTC or any IANA time zone, plus epoch-aligned interval schedules coordinated through PostgreSQL.
- Worker heartbeats, lock renewal, stale-work recovery, listener reconnection, graceful shutdown, and shutdown-aware handlers.
- Job management and observability APIs for cancellation, retrying, rescheduling, completion, permanent failure, worker recovery, worker inspection, status counts, and recent jobs.
- Configurable finished-job retention with automatic and manual pruning.
- Structured job lifecycle logging and per-job tracing spans.
- Jiff timestamps in the public API and embedded SQLx migrations for a dedicated `pgspawn` schema.
