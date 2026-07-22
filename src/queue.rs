use std::{
    collections::{HashMap, HashSet},
    time::Duration as StdDuration,
};

use jiff::Timestamp;
use jiff_sqlx::Timestamp as SqlxTimestamp;
use serde::Serialize;
use serde_json::Value;
use sqlx::{Connection, PgConnection, PgExecutor, PgPool};
use uuid::Uuid;

use crate::{CronJob, EnqueueError, EnqueueOptions, EnqueueRequest, Job, JobKeyMode, JobStatus};

#[derive(Clone)]
pub struct Queue {
    pub(crate) pool: PgPool,
}

impl Queue {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn enqueue(&self, name: &str, payload: Value) -> Result<Uuid, sqlx::Error> {
        self.enqueue_with_options(name, payload, EnqueueOptions::default())
            .await
    }

    pub async fn enqueue_typed<T>(&self, name: &str, payload: &T) -> Result<Uuid, EnqueueError>
    where
        T: Serialize + ?Sized,
    {
        self.enqueue_typed_with_options(name, payload, EnqueueOptions::default())
            .await
    }

    pub async fn enqueue_typed_with_options<T>(
        &self,
        name: &str,
        payload: &T,
        options: EnqueueOptions,
    ) -> Result<Uuid, EnqueueError>
    where
        T: Serialize + ?Sized,
    {
        let payload = serde_json::to_value(payload)?;
        Ok(self.enqueue_with_options(name, payload, options).await?)
    }

    pub async fn enqueue_with_options(
        &self,
        name: &str,
        payload: Value,
        options: EnqueueOptions,
    ) -> Result<Uuid, sqlx::Error> {
        insert_job(&self.pool, name, payload, options).await
    }

    /// Enqueues many jobs in a single transaction, returning their ids in request order.
    ///
    /// Batching amortizes the commit serialization PostgreSQL applies to notifying transactions and collapses the batch into one worker wakeup, so this is substantially cheaper than enqueueing in a loop. Job keys resolve against existing jobs exactly as [`Queue::enqueue_with_options`] does, so a returned id may belong to a job that already existed. Job keys must be unique within the batch; a repeat returns [`EnqueueError::DuplicateJobKey`] without inserting anything.
    pub async fn enqueue_many(
        &self,
        requests: &[EnqueueRequest],
    ) -> Result<Vec<Uuid>, EnqueueError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut transaction = self.pool.begin().await?;
        let ids = insert_job_batch(&mut transaction, requests).await?;
        transaction.commit().await?;
        Ok(ids)
    }

    /// Enqueues many jobs atomically through the caller's connection or transaction. This method creates a transaction or a savepoint, so an error cannot leave a partially inserted mixed-mode batch. See [`Queue::enqueue_many`] and [`Queue::enqueue_in`].
    pub async fn enqueue_many_in(
        &self,
        connection: &mut PgConnection,
        requests: &[EnqueueRequest],
    ) -> Result<Vec<Uuid>, EnqueueError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let mut transaction = connection.begin().await?;
        let ids = insert_job_batch(&mut transaction, requests).await?;
        transaction.commit().await?;
        Ok(ids)
    }

    /// Enqueues through the given executor instead of the queue's own pool, so a job can be inserted inside a caller's transaction and becomes visible to workers only if that transaction commits.
    ///
    /// ```no_run
    /// # async fn example(pool: sqlx::PgPool, queue: pgspawn::Queue) -> Result<(), sqlx::Error> {
    /// let mut transaction = pool.begin().await?;
    /// // ... application writes on the same transaction ...
    /// queue
    ///     .enqueue_in(&mut *transaction, "send_receipt", serde_json::json!({}))
    ///     .await?;
    /// transaction.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_in<'e, E>(
        &self,
        executor: E,
        name: &str,
        payload: Value,
    ) -> Result<Uuid, sqlx::Error>
    where
        E: PgExecutor<'e>,
    {
        insert_job(executor, name, payload, EnqueueOptions::default()).await
    }

    /// Enqueues with options through the given executor. See [`Queue::enqueue_in`].
    pub async fn enqueue_in_with_options<'e, E>(
        &self,
        executor: E,
        name: &str,
        payload: Value,
        options: EnqueueOptions,
    ) -> Result<Uuid, sqlx::Error>
    where
        E: PgExecutor<'e>,
    {
        insert_job(executor, name, payload, options).await
    }

    /// Enqueues a typed payload through the given executor. See [`Queue::enqueue_in`].
    pub async fn enqueue_typed_in<'e, E, T>(
        &self,
        executor: E,
        name: &str,
        payload: &T,
    ) -> Result<Uuid, EnqueueError>
    where
        E: PgExecutor<'e>,
        T: Serialize + ?Sized,
    {
        self.enqueue_typed_in_with_options(executor, name, payload, EnqueueOptions::default())
            .await
    }

    /// Enqueues a typed payload with options through the given executor. See [`Queue::enqueue_in`].
    pub async fn enqueue_typed_in_with_options<'e, E, T>(
        &self,
        executor: E,
        name: &str,
        payload: &T,
        options: EnqueueOptions,
    ) -> Result<Uuid, EnqueueError>
    where
        E: PgExecutor<'e>,
        T: Serialize + ?Sized,
    {
        let payload = serde_json::to_value(payload)?;
        Ok(insert_job(executor, name, payload, options).await?)
    }

    pub(crate) async fn register_worker(
        &self,
        worker_id: &str,
        task_names: &[String],
        metadata: Value,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "
INSERT INTO pgspawn.workers (id, task_names, metadata)
VALUES ($1, $2, $3)
ON CONFLICT (id)
DO UPDATE SET
    task_names = $2,
    metadata = $3,
    last_heartbeat_at = now()
            ",
        )
        .bind(worker_id)
        .bind(task_names)
        .bind(metadata)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn deregister_worker(&self, worker_id: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM pgspawn.workers WHERE id = $1")
            .bind(worker_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub(crate) async fn release_worker_jobs(&self, worker_id: &str) -> Result<u64, sqlx::Error> {
        let lock_prefix = format!("{worker_id}:");
        let result = sqlx::query(
            "
UPDATE pgspawn.jobs
SET status = 'queued',
    locked_by = NULL,
    locked_at = NULL,
    run_at = now(),
    error = COALESCE(error, 'released by shutting down worker')
WHERE status = 'running'
    AND locked_by IS NOT NULL
    AND left(locked_by, length($1)) = $1
            ",
        )
        .bind(&lock_prefix)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub async fn get(&self, id: Uuid) -> Result<Option<Job>, sqlx::Error> {
        let job = sqlx::query_as::<_, JobRow>(
            "
SELECT id,
    name,
    status,
    payload,
    queue_name,
    priority,
    run_at,
    job_key,
    attempt,
    max_attempts,
    locked_by,
    locked_at,
    error,
    queued_at,
    started_at,
    finished_at,
    created_at,
    updated_at
FROM pgspawn.jobs
WHERE id = $1
            ",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        job.map(JobRow::into_job).transpose()
    }

    pub async fn cancel(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "
UPDATE pgspawn.jobs
SET status = 'cancelled',
    finished_at = now(),
    error = NULL
WHERE id = $1
    AND status = 'queued'
            ",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn retry(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "
UPDATE pgspawn.jobs
SET status = 'queued',
    attempt = 0,
    run_at = now(),
    locked_by = NULL,
    locked_at = NULL,
    error = NULL,
    started_at = NULL,
    finished_at = NULL
WHERE id = $1
    AND status IN ('failed', 'cancelled')
            ",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Marks a queued or running job failed permanently and releases its lock. A running handler is not interrupted, but its result is discarded because the lock no longer belongs to it, so the job will not retry. Returns `false` when the job does not exist or had already finished.
    ///
    /// Releasing a running named-queue job allows the next job in that queue to start before the original handler exits. Only use this operation when that overlap is safe.
    pub async fn permanently_fail(&self, id: Uuid, reason: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "
UPDATE pgspawn.jobs
SET status = 'failed',
    locked_by = NULL,
    locked_at = NULL,
    finished_at = now(),
    error = $2
WHERE id = $1
    AND status IN ('queued', 'running')
            ",
        )
        .bind(id)
        .bind(reason)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Marks a queued or running job succeeded and releases its lock. A running handler is not interrupted, but its result is discarded because the lock no longer belongs to it. Returns `false` when the job does not exist or had already finished.
    ///
    /// Releasing a running named-queue job allows the next job in that queue to start before the original handler exits. Only use this operation when that overlap is safe.
    pub async fn complete(&self, id: Uuid) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "
UPDATE pgspawn.jobs
SET status = 'succeeded',
    locked_by = NULL,
    locked_at = NULL,
    finished_at = now(),
    error = NULL
WHERE id = $1
    AND status IN ('queued', 'running')
            ",
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    /// Requeues every job locked by the given worker and deregisters it atomically, returning the number of jobs released.
    ///
    /// Workers that stop without deregistering are recovered automatically once their heartbeat passes [`WorkerConfig::stale_after`](crate::WorkerConfig::stale_after). This forces that recovery immediately for a worker known to be gone, such as one lost with its host. Calling it for a worker that is still running lets its in-flight jobs be claimed a second time.
    pub async fn force_unlock_worker(&self, worker_id: &str) -> Result<u64, sqlx::Error> {
        let lock_prefix = format!("{worker_id}:");
        let (released, _deregistered): (i64, i64) = sqlx::query_as(
            "
WITH released AS (
    UPDATE pgspawn.jobs
    SET status = 'queued',
        locked_by = NULL,
        locked_at = NULL,
        run_at = now(),
        error = COALESCE(error, 'released by operator')
    WHERE status = 'running'
        AND locked_by IS NOT NULL
        AND left(locked_by, length($2)) = $2
    RETURNING 1
),
deregistered AS (
    DELETE FROM pgspawn.workers
    WHERE id = $1
    RETURNING 1
)
SELECT (SELECT count(*) FROM released),
    (SELECT count(*) FROM deregistered)
            ",
        )
        .bind(worker_id)
        .bind(&lock_prefix)
        .fetch_one(&self.pool)
        .await?;

        Ok(released as u64)
    }

    /// Lists workers registered against this database, including stale registrations not yet removed by recovery.
    pub async fn workers(&self) -> Result<Vec<WorkerInfo>, sqlx::Error> {
        let workers = sqlx::query_as::<_, WorkerRow>(
            "
SELECT id, task_names, started_at, last_heartbeat_at
FROM pgspawn.workers
ORDER BY started_at
            ",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(workers
            .into_iter()
            .map(|row| WorkerInfo {
                id: row.id,
                task_names: row.task_names,
                started_at: row.started_at.to_jiff(),
                last_heartbeat_at: row.last_heartbeat_at.to_jiff(),
            })
            .collect())
    }

    /// Counts all retained jobs by status. This is an exact aggregate over the jobs table, so callers should avoid running it at high frequency on large queues.
    pub async fn counts(&self) -> Result<JobCounts, sqlx::Error> {
        let (queued, running, succeeded, failed, cancelled): (i64, i64, i64, i64, i64) =
            sqlx::query_as(
                "
SELECT count(*) FILTER (WHERE status = 'queued'),
    count(*) FILTER (WHERE status = 'running'),
    count(*) FILTER (WHERE status = 'succeeded'),
    count(*) FILTER (WHERE status = 'failed'),
    count(*) FILTER (WHERE status = 'cancelled')
FROM pgspawn.jobs
                ",
            )
            .fetch_one(&self.pool)
            .await?;

        Ok(JobCounts {
            queued,
            running,
            succeeded,
            failed,
            cancelled,
        })
    }

    /// Moves a queued job to a new `run_at`. Returns `false` when the job does not exist or is no longer queued; running and finished jobs are never rescheduled.
    pub async fn reschedule(&self, id: Uuid, run_at: Timestamp) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "
UPDATE pgspawn.jobs
SET run_at = $2
WHERE id = $1
    AND status = 'queued'
            ",
        )
        .bind(id)
        .bind(SqlxTimestamp::from(run_at))
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub async fn recent(&self, limit: i64) -> Result<Vec<Job>, sqlx::Error> {
        let jobs = sqlx::query_as::<_, JobRow>(
            "
SELECT id,
    name,
    status,
    payload,
    queue_name,
    priority,
    run_at,
    job_key,
    attempt,
    max_attempts,
    locked_by,
    locked_at,
    error,
    queued_at,
    started_at,
    finished_at,
    created_at,
    updated_at
FROM pgspawn.jobs
ORDER BY created_at DESC
LIMIT $1
            ",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        jobs.into_iter().map(JobRow::into_job).collect()
    }

    /// Deletes succeeded, failed, and cancelled jobs that finished more than `retain_for` ago. Returns the number of jobs deleted.
    pub async fn prune_finished(&self, retain_for: StdDuration) -> Result<u64, sqlx::Error> {
        let retain_for_millis = duration_millis_i64(retain_for);
        let result = sqlx::query(
            "
DELETE FROM pgspawn.jobs
WHERE status IN ('succeeded', 'failed', 'cancelled')
    AND finished_at IS NOT NULL
    AND finished_at < now() - ($1 * interval '1 millisecond')
            ",
        )
        .bind(retain_for_millis)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub(crate) async fn recover_stale(&self, stale_after: StdDuration) -> Result<u64, sqlx::Error> {
        let stale_after_millis = duration_millis_i64(stale_after);
        let result = sqlx::query(
            "
UPDATE pgspawn.jobs
SET status = 'queued',
    locked_by = NULL,
    locked_at = NULL,
    run_at = now(),
    error = COALESCE(error, 'recovered stale running job')
WHERE status = 'running'
    AND locked_at IS NOT NULL
    AND locked_at < now() - ($1 * interval '1 millisecond')
            ",
        )
        .bind(stale_after_millis)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    pub(crate) async fn recover_dead_workers(
        &self,
        stale_after: StdDuration,
    ) -> Result<u64, sqlx::Error> {
        let stale_after_millis = duration_millis_i64(stale_after);
        let (recovered_count,) = sqlx::query_as::<_, (i64,)>(
            "
WITH stale_workers AS (
    DELETE FROM pgspawn.workers
    WHERE last_heartbeat_at < now() - ($1 * interval '1 millisecond')
    RETURNING id
),
recovered AS (
    UPDATE pgspawn.jobs
    SET status = 'queued',
        locked_by = NULL,
        locked_at = NULL,
        run_at = now(),
        error = COALESCE(error, 'recovered dead worker job')
    FROM stale_workers
    WHERE jobs.status = 'running'
        AND jobs.locked_by IS NOT NULL
        AND left(jobs.locked_by, length(stale_workers.id) + 1) = stale_workers.id || ':'
    RETURNING jobs.id
)
SELECT COUNT(*)::bigint
FROM recovered
            ",
        )
        .bind(stale_after_millis)
        .fetch_one(&self.pool)
        .await?;

        Ok(recovered_count as u64)
    }

    pub(crate) async fn try_enqueue_cron(
        &self,
        cron: &CronJob,
        due_at: Timestamp,
        next_run_at: Timestamp,
    ) -> Result<CronEnqueueResult, sqlx::Error> {
        let Some((job_id, inserted)) = sqlx::query_as::<_, (Uuid, bool)>(
            "
WITH claimed AS (
    INSERT INTO pgspawn.crons (identifier, name, payload, last_run_at, next_run_at)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (identifier)
    DO UPDATE SET
        name = EXCLUDED.name,
        payload = EXCLUDED.payload,
        last_run_at = EXCLUDED.last_run_at,
        next_run_at = EXCLUDED.next_run_at
    WHERE crons.last_run_at IS NULL
        OR crons.last_run_at < EXCLUDED.last_run_at
    RETURNING 1
),
existing AS (
    SELECT EXISTS (
        SELECT 1
        FROM pgspawn.crons
        WHERE identifier = $1
            AND created_at < now()
    ) AS existed
)
INSERT INTO pgspawn.jobs (name, status, payload, queue_name, priority, run_at, max_attempts, job_key)
SELECT $2, 'queued', $3, $6, $7, $4, $8, $10
FROM claimed, existing
WHERE $9 OR existing.existed
ON CONFLICT (job_key)
WHERE status IN ('queued', 'running') AND job_key IS NOT NULL
DO UPDATE SET
    name = CASE WHEN $11 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.name ELSE jobs.name END,
    payload = CASE WHEN $11 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.payload ELSE jobs.payload END,
    queue_name = CASE WHEN $11 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.queue_name ELSE jobs.queue_name END,
    priority = CASE WHEN $11 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.priority ELSE jobs.priority END,
    run_at = CASE WHEN $11 = 'replace' AND jobs.status = 'queued' THEN EXCLUDED.run_at ELSE jobs.run_at END,
    max_attempts = CASE WHEN $11 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.max_attempts ELSE jobs.max_attempts END,
    error = CASE WHEN $11 <> 'dedupe' AND jobs.status = 'queued' THEN NULL ELSE jobs.error END
RETURNING id, (xmax = 0) AS inserted
            ",
        )
        .bind(&cron.identifier)
        .bind(&cron.name)
        .bind(&cron.payload)
        .bind(SqlxTimestamp::from(due_at))
        .bind(SqlxTimestamp::from(next_run_at))
        .bind(&cron.options.queue_name)
        .bind(cron.options.priority)
        .bind(cron.options.max_attempts)
        .bind(cron.catch_up_on_start)
        .bind(&cron.options.job_key)
        .bind(cron.options.job_key_mode.as_str())
        .fetch_optional(&self.pool)
        .await?
        else {
            return Ok(CronEnqueueResult::Skipped);
        };

        if inserted {
            Ok(CronEnqueueResult::Enqueued(job_id))
        } else {
            Ok(CronEnqueueResult::AlreadyQueued(job_id))
        }
    }

    pub(crate) async fn claim_next(
        &self,
        lock_owner: &str,
        names: &[String],
    ) -> Result<Option<Job>, sqlx::Error> {
        let result = sqlx::query_as::<_, JobRow>(
            "
WITH candidate AS (
    SELECT candidate_jobs.id
    FROM pgspawn.jobs AS candidate_jobs
    WHERE candidate_jobs.status = 'queued'
        AND candidate_jobs.run_at <= now()
        AND candidate_jobs.name = ANY($2::text[])
        AND (
            candidate_jobs.queue_name IS NULL
            OR NOT EXISTS (
                SELECT 1
                FROM pgspawn.jobs AS running_jobs
                WHERE running_jobs.status = 'running'
                    AND running_jobs.queue_name = candidate_jobs.queue_name
            )
        )
    ORDER BY candidate_jobs.priority ASC, candidate_jobs.run_at ASC, candidate_jobs.created_at ASC
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE pgspawn.jobs
SET status = 'running',
    attempt = attempt + 1,
    locked_by = $1,
    locked_at = now(),
    started_at = now(),
    finished_at = NULL,
    error = NULL
FROM candidate
WHERE jobs.id = candidate.id
RETURNING jobs.id,
    jobs.name,
    jobs.status,
    jobs.payload,
    jobs.queue_name,
    jobs.priority,
    jobs.run_at,
    jobs.job_key,
    jobs.attempt,
    jobs.max_attempts,
    jobs.locked_by,
    jobs.locked_at,
    jobs.error,
    jobs.queued_at,
    jobs.started_at,
    jobs.finished_at,
    jobs.created_at,
    jobs.updated_at
            ",
        )
        .bind(lock_owner)
        .bind(names)
        .fetch_optional(&self.pool)
        .await;

        match result {
            Ok(job) => job.map(JobRow::into_job).transpose(),
            Err(err) if is_unique_violation(&err, "jobs_queue_running_idx") => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn complete_locked(
        &self,
        id: Uuid,
        lock_owner: &str,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "
UPDATE pgspawn.jobs
SET status = 'succeeded',
    locked_by = NULL,
    locked_at = NULL,
    finished_at = now(),
    error = NULL
WHERE id = $1
    AND locked_by = $2
            ",
        )
        .bind(id)
        .bind(lock_owner)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub(crate) async fn renew_lock(&self, id: Uuid, lock_owner: &str) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "
UPDATE pgspawn.jobs
SET locked_at = now()
WHERE id = $1
    AND locked_by = $2
    AND status = 'running'
            ",
        )
        .bind(id)
        .bind(lock_owner)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() == 1)
    }

    pub(crate) async fn fail_locked(
        &self,
        id: Uuid,
        lock_owner: &str,
        attempt: i32,
        max_attempts: i32,
        error: &str,
    ) -> Result<bool, sqlx::Error> {
        let result = if attempt >= max_attempts {
            sqlx::query(
                "
UPDATE pgspawn.jobs
SET status = 'failed',
    locked_by = NULL,
    locked_at = NULL,
    finished_at = now(),
    error = $3
WHERE id = $1
    AND locked_by = $2
                ",
            )
            .bind(id)
            .bind(lock_owner)
            .bind(error)
            .execute(&self.pool)
            .await?
        } else {
            sqlx::query(
                "
UPDATE pgspawn.jobs
SET status = 'queued',
    locked_by = NULL,
    locked_at = NULL,
    run_at = now() + (exp(least(attempt, 10)) * interval '1 second'),
    error = $3
WHERE id = $1
    AND locked_by = $2
                ",
            )
            .bind(id)
            .bind(lock_owner)
            .bind(error)
            .execute(&self.pool)
            .await?
        };

        Ok(result.rows_affected() == 1)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CronEnqueueResult {
    Enqueued(Uuid),
    AlreadyQueued(Uuid),
    Skipped,
}

/// A worker registered against the database. See [`Queue::workers`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WorkerInfo {
    pub id: String,
    pub task_names: Vec<String>,
    pub started_at: Timestamp,
    pub last_heartbeat_at: Timestamp,
}

/// Job totals by status. See [`Queue::counts`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct JobCounts {
    pub queued: i64,
    pub running: i64,
    pub succeeded: i64,
    pub failed: i64,
    pub cancelled: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct WorkerRow {
    id: String,
    task_names: Vec<String>,
    started_at: SqlxTimestamp,
    last_heartbeat_at: SqlxTimestamp,
}

#[derive(Debug, sqlx::FromRow)]
struct JobRow {
    id: Uuid,
    name: String,
    status: String,
    payload: Value,
    queue_name: Option<String>,
    priority: i32,
    run_at: SqlxTimestamp,
    job_key: Option<String>,
    attempt: i32,
    max_attempts: i32,
    locked_by: Option<String>,
    locked_at: Option<SqlxTimestamp>,
    error: Option<String>,
    queued_at: SqlxTimestamp,
    started_at: Option<SqlxTimestamp>,
    finished_at: Option<SqlxTimestamp>,
    created_at: SqlxTimestamp,
    updated_at: SqlxTimestamp,
}

impl JobRow {
    fn into_job(self) -> Result<Job, sqlx::Error> {
        let row = self;
        let status = row
            .status
            .parse::<JobStatus>()
            .map_err(|err| sqlx::Error::ColumnDecode {
                index: "status".to_owned(),
                source: Box::new(err),
            })?;
        Ok(Job {
            id: row.id,
            name: row.name,
            status,
            payload: row.payload,
            queue_name: row.queue_name,
            priority: row.priority,
            run_at: row.run_at.to_jiff(),
            job_key: row.job_key,
            attempt: row.attempt,
            max_attempts: row.max_attempts,
            locked_by: row.locked_by,
            locked_at: row.locked_at.map(SqlxTimestamp::to_jiff),
            error: row.error,
            queued_at: row.queued_at.to_jiff(),
            started_at: row.started_at.map(SqlxTimestamp::to_jiff),
            finished_at: row.finished_at.map(SqlxTimestamp::to_jiff),
            created_at: row.created_at.to_jiff(),
            updated_at: row.updated_at.to_jiff(),
        })
    }
}

// Each job-key mode needs a separate statement because `ON CONFLICT DO UPDATE` cannot vary its behavior by source row.
async fn insert_job_batch(
    connection: &mut PgConnection,
    requests: &[EnqueueRequest],
) -> Result<Vec<Uuid>, EnqueueError> {
    let mut batch_keys = HashSet::with_capacity(requests.len());
    for request in requests {
        if let Some(job_key) = request.options.job_key.as_deref()
            && !batch_keys.insert(job_key)
        {
            return Err(EnqueueError::DuplicateJobKey(job_key.to_owned()));
        }
    }

    // Client-generated ids keep the mapping independent of PostgreSQL's RETURNING order.
    let ids: Vec<Uuid> = requests.iter().map(|_| Uuid::new_v4()).collect();
    let mut ids_by_job_key: HashMap<String, Uuid> = HashMap::new();

    for job_key_mode in [
        JobKeyMode::Dedupe,
        JobKeyMode::Replace,
        JobKeyMode::PreserveRunAt,
    ] {
        let selected: Vec<usize> = requests
            .iter()
            .enumerate()
            .filter(|(_, request)| request.options.job_key_mode == job_key_mode)
            .map(|(index, _)| index)
            .collect();
        if selected.is_empty() {
            continue;
        }

        let mut batch_ids = Vec::with_capacity(selected.len());
        let mut names = Vec::with_capacity(selected.len());
        let mut payloads = Vec::with_capacity(selected.len());
        let mut queue_names = Vec::with_capacity(selected.len());
        let mut priorities = Vec::with_capacity(selected.len());
        let mut run_ats = Vec::with_capacity(selected.len());
        let mut max_attempts = Vec::with_capacity(selected.len());
        let mut job_keys = Vec::with_capacity(selected.len());
        for &index in &selected {
            let request = &requests[index];
            batch_ids.push(ids[index]);
            names.push(request.name.clone());
            payloads.push(request.payload.clone());
            queue_names.push(request.options.queue_name.clone());
            priorities.push(request.options.priority);
            run_ats.push(SqlxTimestamp::from(
                request.options.run_at.unwrap_or_else(Timestamp::now),
            ));
            max_attempts.push(request.options.max_attempts);
            job_keys.push(request.options.job_key.clone());
        }

        let rows = sqlx::query_as::<_, (Uuid, Option<String>)>(
            "
INSERT INTO pgspawn.jobs (id, name, status, payload, queue_name, priority, run_at, max_attempts, job_key)
SELECT batch.id, batch.name, 'queued', batch.payload, batch.queue_name, batch.priority, batch.run_at, batch.max_attempts, batch.job_key
FROM unnest($1::uuid[], $2::text[], $3::jsonb[], $4::text[], $5::integer[], $6::timestamptz[], $7::integer[], $8::text[])
    AS batch(id, name, payload, queue_name, priority, run_at, max_attempts, job_key)
ON CONFLICT (job_key)
WHERE status IN ('queued', 'running') AND job_key IS NOT NULL
DO UPDATE SET
    name = CASE WHEN $9 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.name ELSE jobs.name END,
    payload = CASE WHEN $9 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.payload ELSE jobs.payload END,
    queue_name = CASE WHEN $9 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.queue_name ELSE jobs.queue_name END,
    priority = CASE WHEN $9 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.priority ELSE jobs.priority END,
    run_at = CASE WHEN $9 = 'replace' AND jobs.status = 'queued' THEN EXCLUDED.run_at ELSE jobs.run_at END,
    max_attempts = CASE WHEN $9 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.max_attempts ELSE jobs.max_attempts END,
    error = CASE WHEN $9 <> 'dedupe' AND jobs.status = 'queued' THEN NULL ELSE jobs.error END
RETURNING id, job_key
            ",
        )
        .bind(&batch_ids)
        .bind(&names)
        .bind(&payloads)
        .bind(&queue_names)
        .bind(&priorities)
        .bind(&run_ats)
        .bind(&max_attempts)
        .bind(&job_keys)
        .bind(job_key_mode.as_str())
        .fetch_all(&mut *connection)
        .await?;

        for (id, job_key) in rows {
            if let Some(job_key) = job_key {
                ids_by_job_key.insert(job_key, id);
            }
        }
    }

    let mut ordered_ids = Vec::with_capacity(requests.len());
    for (index, request) in requests.iter().enumerate() {
        let id = match request.options.job_key.as_deref() {
            Some(job_key) => ids_by_job_key.get(job_key).copied().ok_or_else(|| {
                sqlx::Error::Protocol(format!(
                    "batch insert returned no row for job key `{job_key}`"
                ))
            })?,
            None => ids[index],
        };
        ordered_ids.push(id);
    }
    Ok(ordered_ids)
}

async fn insert_job<'e, E>(
    executor: E,
    name: &str,
    payload: Value,
    options: EnqueueOptions,
) -> Result<Uuid, sqlx::Error>
where
    E: PgExecutor<'e>,
{
    let run_at = SqlxTimestamp::from(options.run_at.unwrap_or_else(Timestamp::now));
    let (id,) = sqlx::query_as::<_, (Uuid,)>(
        "
INSERT INTO pgspawn.jobs (name, status, payload, queue_name, priority, run_at, max_attempts, job_key)
VALUES ($1, 'queued', $2, $3, $4, $5, $6, $7)
ON CONFLICT (job_key)
WHERE status IN ('queued', 'running') AND job_key IS NOT NULL
DO UPDATE SET
    name = CASE WHEN $8 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.name ELSE jobs.name END,
    payload = CASE WHEN $8 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.payload ELSE jobs.payload END,
    queue_name = CASE WHEN $8 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.queue_name ELSE jobs.queue_name END,
    priority = CASE WHEN $8 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.priority ELSE jobs.priority END,
    run_at = CASE WHEN $8 = 'replace' AND jobs.status = 'queued' THEN EXCLUDED.run_at ELSE jobs.run_at END,
    max_attempts = CASE WHEN $8 <> 'dedupe' AND jobs.status = 'queued' THEN EXCLUDED.max_attempts ELSE jobs.max_attempts END,
    error = CASE WHEN $8 <> 'dedupe' AND jobs.status = 'queued' THEN NULL ELSE jobs.error END
RETURNING id
        ",
    )
    .bind(name)
    .bind(payload)
    .bind(options.queue_name)
    .bind(options.priority)
    .bind(run_at)
    .bind(options.max_attempts)
    .bind(options.job_key)
    .bind(options.job_key_mode.as_str())
    .fetch_one(executor)
    .await?;

    Ok(id)
}

fn duration_millis_i64(duration: StdDuration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

fn is_unique_violation(err: &sqlx::Error, constraint: &str) -> bool {
    err.as_database_error()
        .and_then(|db_err| {
            let code_matches = db_err.code().as_deref() == Some("23505");
            let constraint_matches = db_err.constraint() == Some(constraint);
            (code_matches && constraint_matches).then_some(())
        })
        .is_some()
}
