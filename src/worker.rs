use std::{
    collections::HashSet,
    future::{Future, pending},
    sync::Arc,
    time::Duration as StdDuration,
};

use jiff::Timestamp;
use serde_json::Value;
use sqlx::{PgPool, postgres::PgListener};
use tokio::sync::{Notify, watch};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{Instrument, debug, error, info, info_span, warn};
use uuid::Uuid;

use crate::{CronJob, Job, Queue, Registry, queue::CronEnqueueResult};

const JOB_NOTIFY_CHANNEL: &str = "pgspawn_jobs";

tokio::task_local! {
    static JOB_SHUTDOWN: watch::Receiver<bool>;
}

/// Whether the worker running the current job has begun shutting down.
///
/// Handlers that loop over work can check this between items and finish or return an error for retry before the shutdown grace period aborts them. Returns `false` outside a pgspawn handler.
///
/// The shutdown context follows awaited calls in the handler future but is not inherited by tasks the handler creates with [`tokio::spawn`].
///
/// ```no_run
/// # async fn example(items: Vec<u32>) -> Result<(), std::io::Error> {
/// for item in items {
///     if pgspawn::is_shutting_down() {
///         return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "shutdown requested"));
///     }
///     // ... process item ...
/// }
/// # Ok(())
/// # }
/// ```
pub fn is_shutting_down() -> bool {
    JOB_SHUTDOWN
        .try_with(|shutdown| *shutdown.borrow())
        .unwrap_or(false)
}

/// Resolves when the worker running the current job begins shutting down, for handlers that wait on something cancellable.
///
/// Outside a pgspawn handler this never resolves, so `select!` arms using it stay inert in tests and other direct calls. The shutdown context is not inherited by tasks created with [`tokio::spawn`].
///
/// ```no_run
/// # async fn slow_work() -> Result<(), std::io::Error> { Ok(()) }
/// # async fn example() -> Result<(), std::io::Error> {
/// tokio::select! {
///     _ = pgspawn::shutdown_requested() => Ok(()),
///     result = slow_work() => result,
/// }
/// # }
/// ```
pub async fn shutdown_requested() {
    let Ok(mut shutdown) = JOB_SHUTDOWN.try_with(|shutdown| shutdown.clone()) else {
        pending::<()>().await;
        return;
    };

    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        // A dropped sender means the worker can no longer publish a shutdown state.
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

pub(crate) struct AbortOnDrop<T> {
    handle: JoinHandle<T>,
}

impl<T> AbortOnDrop<T> {
    pub(crate) fn new(handle: JoinHandle<T>) -> Self {
        Self { handle }
    }
}

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub concurrency: usize,
    /// How often idle claim loops poll for delayed work or missed notifications, and how long to wait before retrying after a claim error.
    pub poll_interval: StdDuration,
    pub cron_poll_interval: StdDuration,
    pub heartbeat_interval: StdDuration,
    pub lock_renew_interval: StdDuration,
    pub stale_after: StdDuration,
    pub recovery_interval: StdDuration,
    pub shutdown_grace_period: StdDuration,
    /// How long finished (succeeded, failed, or cancelled) jobs are kept before the recovery loop deletes them. `None` keeps finished jobs forever; the jobs table then grows without bound unless the application prunes it through [`Queue::prune_finished`](crate::Queue::prune_finished).
    pub finished_job_retention: Option<StdDuration>,
}

impl WorkerConfig {
    pub(crate) fn validate(&self) -> Result<(), WorkerError> {
        if self.concurrency == 0 {
            return Err(WorkerError::InvalidConfig(
                "concurrency must be greater than zero",
            ));
        }
        if self.poll_interval.is_zero() {
            return Err(WorkerError::InvalidConfig(
                "poll_interval must be greater than zero",
            ));
        }
        if self.cron_poll_interval.is_zero() {
            return Err(WorkerError::InvalidConfig(
                "cron_poll_interval must be greater than zero",
            ));
        }
        if self.heartbeat_interval.is_zero() {
            return Err(WorkerError::InvalidConfig(
                "heartbeat_interval must be greater than zero",
            ));
        }
        if self.lock_renew_interval.is_zero() {
            return Err(WorkerError::InvalidConfig(
                "lock_renew_interval must be greater than zero",
            ));
        }
        if self.recovery_interval.is_zero() {
            return Err(WorkerError::InvalidConfig(
                "recovery_interval must be greater than zero",
            ));
        }
        if self.stale_after <= self.heartbeat_interval {
            return Err(WorkerError::InvalidConfig(
                "stale_after must be greater than heartbeat_interval",
            ));
        }
        if self.stale_after <= self.lock_renew_interval {
            return Err(WorkerError::InvalidConfig(
                "stale_after must be greater than lock_renew_interval",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    #[error("worker requires at least one registered handler")]
    EmptyRegistry,
    #[error("invalid worker configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("cron job `{0}` has no registered handler")]
    MissingCronHandler(String),
    #[error("duplicate cron identifier `{0}`")]
    DuplicateCronIdentifier(String),
    #[error("worker database operation failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("a supervised worker loop exited unexpectedly")]
    LoopExited,
    #[error("a supervised worker loop failed: {0}")]
    LoopFailed(#[source] tokio::task::JoinError),
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("pgspawn_{}", Uuid::new_v4()),
            concurrency: 4,
            poll_interval: StdDuration::from_secs(2),
            cron_poll_interval: StdDuration::from_secs(30),
            heartbeat_interval: StdDuration::from_secs(15),
            lock_renew_interval: StdDuration::from_secs(30),
            stale_after: StdDuration::from_secs(10 * 60),
            recovery_interval: StdDuration::from_secs(60),
            shutdown_grace_period: StdDuration::from_secs(30),
            finished_job_retention: Some(StdDuration::from_secs(30 * 24 * 60 * 60)),
        }
    }
}

#[derive(Clone)]
pub struct Worker {
    queue: Queue,
    registry: Registry,
    crons: Arc<Vec<CronJob>>,
    config: WorkerConfig,
    notify: Arc<Notify>,
}

impl Worker {
    pub fn new(pool: PgPool, registry: Registry) -> Self {
        Self {
            queue: Queue::new(pool),
            registry,
            crons: Arc::new(Vec::new()),
            config: WorkerConfig::default(),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn with_config(mut self, config: WorkerConfig) -> Self {
        self.config = config;
        self
    }

    pub fn with_crons(mut self, crons: Vec<CronJob>) -> Self {
        self.crons = Arc::new(crons);
        self
    }

    pub fn start(self) -> JoinHandle<Result<(), WorkerError>> {
        tokio::spawn(async move { self.run().await })
    }

    pub fn start_with_shutdown<F>(self, shutdown: F) -> JoinHandle<Result<(), WorkerError>>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(async move { self.run_until_shutdown(shutdown).await })
    }

    pub async fn run(self) -> Result<(), WorkerError> {
        self.run_until_shutdown(pending()).await
    }

    pub async fn run_until_shutdown<F>(self, shutdown: F) -> Result<(), WorkerError>
    where
        F: Future<Output = ()>,
    {
        self.config.validate()?;
        let names = self.registry.names();
        if names.is_empty() {
            return Err(WorkerError::EmptyRegistry);
        }
        let mut cron_identifiers = HashSet::with_capacity(self.crons.len());
        for cron in self.crons.iter() {
            if !names.contains(&cron.name) {
                return Err(WorkerError::MissingCronHandler(cron.name.clone()));
            }
            if !cron_identifiers.insert(&cron.identifier) {
                return Err(WorkerError::DuplicateCronIdentifier(
                    cron.identifier.clone(),
                ));
            }
        }

        self.queue
            .register_worker(
                &self.config.worker_id,
                &names,
                Value::Object(Default::default()),
            )
            .await?;
        info!(
            worker_id = %self.config.worker_id,
            concurrency = self.config.concurrency,
            tasks = names.len(),
            crons = self.crons.len(),
            "job worker started"
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = JoinSet::new();
        let heartbeat_worker = self.clone();
        let heartbeat_names = names.clone();
        let heartbeat_shutdown = shutdown_rx.clone();
        tasks.spawn(async move {
            heartbeat_worker
                .run_heartbeat_loop(heartbeat_names, heartbeat_shutdown)
                .await;
        });

        let recovery_worker = self.clone();
        let recovery_shutdown = shutdown_rx.clone();
        tasks.spawn(async move {
            recovery_worker.run_recovery_loop(recovery_shutdown).await;
        });

        let listener_worker = self.clone();
        let listener_shutdown = shutdown_rx.clone();
        tasks.spawn(async move {
            listener_worker
                .run_notify_listener_loop(listener_shutdown)
                .await;
        });

        if !self.crons.is_empty() {
            let cron_worker = self.clone();
            let cron_shutdown = shutdown_rx.clone();
            tasks.spawn(async move {
                cron_worker.run_cron_loop(cron_shutdown).await;
            });
        }

        for worker_index in 0..self.config.concurrency.max(1) {
            let worker = self.clone();
            let names = names.clone();
            let claim_shutdown = shutdown_rx.clone();
            tasks.spawn(async move {
                worker
                    .run_claim_loop(worker_index, names, claim_shutdown)
                    .await;
            });
        }

        let (requested_shutdown, run_result) = tokio::select! {
            _ = shutdown => {
                info!(worker_id = %self.config.worker_id, "job worker shutdown requested");
                (true, Ok(()))
            }
            result = tasks.join_next() => {
                let error = match result {
                    Some(Ok(())) | None => WorkerError::LoopExited,
                    Some(Err(err)) => WorkerError::LoopFailed(err),
                };
                (false, Err(error))
            }
        };

        let requested_shutdown = requested_shutdown && shutdown_tx.send(true).is_ok();

        if requested_shutdown {
            let drain_result = tokio::time::timeout(self.config.shutdown_grace_period, async {
                while let Some(result) = tasks.join_next().await {
                    if let Err(err) = result {
                        error!(?err, worker_id = %self.config.worker_id, "job worker loop panicked or was cancelled during shutdown");
                    }
                }
            })
            .await;

            if drain_result.is_err() {
                warn!(
                    worker_id = %self.config.worker_id,
                    grace_seconds = self.config.shutdown_grace_period.as_secs(),
                    "job worker shutdown grace period elapsed; releasing remaining locks"
                );
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
            }
        } else {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }

        let mut cleanup_error = None;
        match self.queue.release_worker_jobs(&self.config.worker_id).await {
            Ok(0) => {}
            Ok(count) => {
                info!(count, worker_id = %self.config.worker_id, "released jobs locked by shutting down worker")
            }
            Err(err) => {
                error!(?err, worker_id = %self.config.worker_id, "failed to release jobs locked by worker");
                cleanup_error = Some(err);
            }
        }
        if let Err(err) = self.queue.deregister_worker(&self.config.worker_id).await {
            error!(?err, worker_id = %self.config.worker_id, "failed to deregister job worker");
            if cleanup_error.is_none() {
                cleanup_error = Some(err);
            }
        }

        run_result?;
        if let Some(err) = cleanup_error {
            return Err(WorkerError::Database(err));
        }
        info!(worker_id = %self.config.worker_id, "job worker stopped");
        Ok(())
    }

    async fn run_heartbeat_loop(
        self,
        task_names: Vec<String>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.config.heartbeat_interval) => {}
            }

            if let Err(err) = self
                .queue
                .register_worker(
                    &self.config.worker_id,
                    &task_names,
                    Value::Object(Default::default()),
                )
                .await
            {
                error!(?err, worker_id = %self.config.worker_id, "failed to heartbeat job worker");
            }
        }
    }

    async fn run_recovery_loop(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.config.recovery_interval) => {}
            }

            match self
                .queue
                .recover_dead_workers(self.config.stale_after)
                .await
            {
                Ok(0) => {}
                Ok(count) => {
                    info!(count, "recovered jobs from dead workers");
                    self.notify.notify_waiters();
                }
                Err(err) => error!(?err, "failed to recover dead worker jobs"),
            }
            match self.queue.recover_stale(self.config.stale_after).await {
                Ok(0) => {}
                Ok(count) => {
                    info!(count, "recovered stale jobs");
                    self.notify.notify_waiters();
                }
                Err(err) => error!(?err, "failed to recover stale jobs"),
            }
            if let Some(retention) = self.config.finished_job_retention {
                match self.queue.prune_finished(retention).await {
                    Ok(0) => {}
                    Ok(count) => info!(count, "pruned finished jobs"),
                    Err(err) => error!(?err, "failed to prune finished jobs"),
                }
            }
        }
    }

    async fn run_notify_listener_loop(self, mut shutdown: watch::Receiver<bool>) {
        // Polling preserves progress while exponential backoff limits connection attempts during database outages.
        const MAX_RECONNECT_DELAY: StdDuration = StdDuration::from_secs(30);
        let mut reconnect_delay = self.config.poll_interval;

        loop {
            if *shutdown.borrow() {
                break;
            }

            match PgListener::connect_with(&self.queue.pool).await {
                Ok(mut listener) => match listener.listen(JOB_NOTIFY_CHANNEL).await {
                    Ok(()) => {
                        reconnect_delay = self.config.poll_interval;

                        // Wake claim loops after LISTEN succeeds to cover notifications missed while disconnected.
                        self.notify.notify_waiters();

                        loop {
                            tokio::select! {
                                changed = shutdown.changed() => {
                                    if changed.is_err() || *shutdown.borrow() {
                                        return;
                                    }
                                }
                                result = listener.recv() => {
                                    match result {
                                        Ok(_) => self.notify.notify_waiters(),
                                        Err(err) => {
                                            warn!(?err, "job notification listener failed; reconnecting");
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => warn!(?err, "failed to listen for job notifications"),
                },
                Err(err) => warn!(?err, "failed to connect job notification listener"),
            }

            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(reconnect_delay) => {}
            }
            reconnect_delay = reconnect_delay.saturating_mul(2).min(MAX_RECONNECT_DELAY);
        }
    }

    async fn run_cron_loop(self, mut shutdown: watch::Receiver<bool>) {
        loop {
            if *shutdown.borrow() {
                break;
            }

            self.enqueue_due_crons().await;
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.config.cron_poll_interval) => {}
            }
        }
    }

    async fn enqueue_due_crons(&self) {
        let now = Timestamp::now();
        for cron in self.crons.iter() {
            let Some(due_at) = cron.schedule.latest_due_at(now) else {
                continue;
            };
            let next_run_at = cron.schedule.next_after(due_at);
            match self.queue.try_enqueue_cron(cron, due_at, next_run_at).await {
                Ok(CronEnqueueResult::Enqueued(job_id)) => {
                    info!(%job_id, cron = %cron.identifier, job = %cron.name, "enqueued cron job");
                    self.notify.notify_waiters();
                }
                Ok(CronEnqueueResult::AlreadyQueued(job_id)) => {
                    debug!(%job_id, cron = %cron.identifier, job = %cron.name, "cron job already queued");
                }
                Ok(CronEnqueueResult::Skipped) => {}
                Err(err) => {
                    error!(?err, cron = %cron.identifier, job = %cron.name, "failed to enqueue cron job")
                }
            }
        }
    }

    async fn run_claim_loop(
        self,
        worker_index: usize,
        names: Vec<String>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let lock_owner = format!(
            "{}:{}:{}",
            self.config.worker_id,
            worker_index,
            Uuid::new_v4()
        );

        loop {
            if *shutdown.borrow() {
                break;
            }

            // Register interest before claiming so a notification that arrives during the query is not lost.
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            match self.queue.claim_next(&lock_owner, &names).await {
                Ok(Some(job)) => {
                    self.run_claimed_job(worker_index, &lock_owner, job, &shutdown)
                        .await
                }
                Ok(None) => {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                        _ = notified => {}
                        _ = tokio::time::sleep(self.config.poll_interval) => {}
                    }
                }
                Err(err) => {
                    error!(?err, worker_index, "failed to claim job");
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                        _ = tokio::time::sleep(self.config.poll_interval) => {}
                    }
                }
            }
        }
    }

    async fn run_claimed_job(
        &self,
        worker_index: usize,
        lock_owner: &str,
        job: Job,
        shutdown: &watch::Receiver<bool>,
    ) {
        let Some(handler) = self.registry.get(&job.name) else {
            let error_message = format!("no handler registered for job {}", job.name);
            if let Err(err) = self
                .queue
                .fail_locked(
                    job.id,
                    lock_owner,
                    job.max_attempts,
                    job.max_attempts,
                    &error_message,
                )
                .await
            {
                error!(?err, job_id = %job.id, "failed to mark unknown job failed");
            }
            return;
        };

        info!(
            worker_index,
            job_id = %job.id,
            job = %job.name,
            attempt = job.attempt,
            max_attempts = job.max_attempts,
            queue_name = job.queue_name.as_deref(),
            "job started"
        );

        let job_id = job.id;
        let job_name = job.name.clone();
        let attempt = job.attempt;
        let max_attempts = job.max_attempts;
        let queue_name = job.queue_name.clone();
        let started = tokio::time::Instant::now();

        let span = info_span!("job", job = %job_name, job_id = %job_id, attempt);
        let handler_future = async move { handler(job).await };
        let mut handler_task = AbortOnDrop::new(tokio::spawn(
            JOB_SHUTDOWN
                .scope(shutdown.clone(), handler_future)
                .instrument(span),
        ));
        let result = loop {
            tokio::select! {
                result = &mut handler_task.handle => break Some(result),
                _ = tokio::time::sleep(self.config.lock_renew_interval) => {
                    match self.queue.renew_lock(job_id, lock_owner).await {
                        Ok(true) => {}
                        Ok(false) => {
                            handler_task.handle.abort();
                            self.log_lock_loss(job_id, &job_name, "renewal").await;
                            break None;
                        }
                        Err(err) => {
                            error!(?err, job_id = %job_id, job = %job_name, "failed to renew job lock");
                        }
                    }
                }
            }
        };
        let Some(result) = result else {
            return;
        };
        let elapsed_ms = started.elapsed().as_millis() as u64;
        match result {
            Ok(Ok(())) => match self.queue.complete_locked(job_id, lock_owner).await {
                Ok(true) => info!(job_id = %job_id, job = %job_name, elapsed_ms, "job succeeded"),
                Ok(false) => {
                    self.log_lock_loss(job_id, &job_name, "completion").await;
                }
                Err(err) => {
                    error!(?err, job_id = %job_id, job = %job_name, "failed to complete job")
                }
            },
            Ok(Err(err)) => {
                self.fail_claimed_job(
                    lock_owner,
                    job_id,
                    &job_name,
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    &err.to_string(),
                )
                .await;
            }
            Err(err) => {
                self.fail_claimed_job(
                    lock_owner,
                    job_id,
                    &job_name,
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    &err.to_string(),
                )
                .await;
            }
        }

        // Finishing a serialized job frees its queue, so wake local claim loops without a notification round trip.
        if queue_name.is_some() {
            self.notify.notify_waiters();
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn fail_claimed_job(
        &self,
        lock_owner: &str,
        job_id: Uuid,
        job_name: &str,
        attempt: i32,
        max_attempts: i32,
        elapsed_ms: u64,
        error_message: &str,
    ) {
        match self
            .queue
            .fail_locked(job_id, lock_owner, attempt, max_attempts, error_message)
            .await
        {
            Ok(true) if attempt >= max_attempts => {
                error!(
                    job_id = %job_id,
                    job = job_name,
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    error = error_message,
                    "job failed permanently"
                )
            }
            Ok(true) => {
                let backoff_seconds = f64::exp(f64::from(attempt.min(10)));
                warn!(
                    job_id = %job_id,
                    job = job_name,
                    attempt,
                    max_attempts,
                    elapsed_ms,
                    backoff_seconds,
                    error = error_message,
                    "job failed and will retry"
                )
            }
            Ok(false) => {
                self.log_lock_loss(job_id, job_name, "failure update").await;
            }
            Err(err) => {
                error!(?err, job_id = %job_id, job = job_name, "failed to update failed job")
            }
        }
    }

    async fn log_lock_loss(&self, job_id: Uuid, job_name: &str, phase: &'static str) {
        match self.queue.get(job_id).await {
            Ok(Some(job)) if job.status.is_terminal() => {
                info!(job_id = %job_id, job = job_name, status = %job.status, phase, "job stopped after external completion");
            }
            Ok(Some(job)) => {
                error!(job_id = %job_id, job = job_name, status = %job.status, phase, "job lock lost");
            }
            Ok(None) => {
                error!(job_id = %job_id, job = job_name, phase, "job disappeared while running");
            }
            Err(err) => {
                error!(?err, job_id = %job_id, job = job_name, phase, "failed to inspect lost job lock");
            }
        }
    }
}
