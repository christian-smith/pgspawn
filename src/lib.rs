#![forbid(unsafe_code)]

//! Durable PostgreSQL-backed background jobs for Tokio applications.
//!
//! `pgspawn` stores jobs, schedules, worker heartbeats, and locks in PostgreSQL while running handlers inside the application process. Applications provide an existing SQLx PostgreSQL pool, register handlers with [`Registry`], enqueue work through [`Queue`], and run one or more [`Worker`] instances.
//!
//! # Getting started
//!
//! Run the embedded migrations before starting workers:
//!
//! ```rust,no_run
//! use pgspawn::{Queue, Registry, Worker, db};
//! use serde_json::json;
//! use sqlx::PgPool;
//!
//! async fn run(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
//!     db::migrate(&pool).await?;
//!
//!     let registry = Registry::builder()
//!         .register("send_email", |_job| async {
//!             Ok::<(), std::io::Error>(())
//!         })
//!         .build();
//!
//!     let queue = Queue::new(pool.clone());
//!     queue
//!         .enqueue("send_email", json!({ "address": "person@example.com" }))
//!         .await?;
//!
//!     Worker::new(pool, registry).run().await?;
//!     Ok(())
//! }
//! ```
//!
//! [`Queue::enqueue_in`] and its typed and configurable variants can enqueue through an application transaction, making the job atomic with the database change that produced it. [`CronJob`] adds coordinated daily, monthly, or interval schedules.
//!
//! # Delivery guarantees
//!
//! Jobs use at-least-once delivery. A handler can run more than once if a worker loses ownership after performing side effects but before recording completion. Handlers should be idempotent, transactional, or protected by an application-level idempotency key.

pub mod db;
mod job;
mod queue;
mod registry;
mod schedule;
mod worker;

pub use jiff;
pub use job::{
    EnqueueError, EnqueueOptions, EnqueueRequest, Job, JobKeyMode, JobStatus, ParseJobStatusError,
};
pub use queue::{JobCounts, Queue, WorkerInfo};
pub use registry::{HandlerError, Registry, RegistryBuilder};
pub use schedule::{CronJob, CronSchedule, DailyJobSchedule, MonthlyJobSchedule};
pub use worker::{Worker, WorkerConfig, WorkerError, is_shutting_down, shutdown_requested};

// Compile README examples without adding the README to public crate documentation.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;

#[cfg(test)]
mod tests;
