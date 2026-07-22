use std::{fmt, str::FromStr};

use jiff::Timestamp;
use serde::de::DeserializeOwned;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EnqueueError {
    #[error("failed to serialize job payload: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("failed to enqueue job: {0}")]
    Database(#[from] sqlx::Error),
    #[error("job key `{0}` appears more than once in the same batch")]
    DuplicateJobKey(String),
}

/// What happens when a job is enqueued under a job key that an unfinished job already holds.
///
/// A running job's definition and lock are never modified under any mode, and no follow-up job is created. Callers must not use a shared key when every state change needs an execution after the current handler started.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum JobKeyMode {
    /// Keeps the existing job definition unchanged and returns its id.
    #[default]
    Dedupe,
    /// Overwrites a queued job, including its `run_at`, so the timer restarts on every enqueue.
    Replace,
    /// Overwrites a queued job but keeps its current `run_at`, so repeated enqueues refresh the payload without postponing execution.
    PreserveRunAt,
}

impl JobKeyMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Dedupe => "dedupe",
            Self::Replace => "replace",
            Self::PreserveRunAt => "preserve_run_at",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EnqueueOptions {
    pub queue_name: Option<String>,
    pub priority: i32,
    pub run_at: Option<Timestamp>,
    pub max_attempts: i32,
    pub job_key: Option<String>,
    pub job_key_mode: JobKeyMode,
}

impl Default for EnqueueOptions {
    fn default() -> Self {
        Self {
            queue_name: None,
            priority: 0,
            run_at: None,
            max_attempts: 25,
            job_key: None,
            job_key_mode: JobKeyMode::Dedupe,
        }
    }
}

/// One job in a batch enqueue. See [`Queue::enqueue_many`](crate::Queue::enqueue_many).
#[derive(Clone, Debug)]
pub struct EnqueueRequest {
    pub name: String,
    pub payload: Value,
    pub options: EnqueueOptions,
}

impl EnqueueRequest {
    pub fn new(name: impl Into<String>, payload: Value) -> Self {
        Self {
            name: name.into(),
            payload,
            options: EnqueueOptions::default(),
        }
    }

    pub fn typed<T>(name: impl Into<String>, payload: &T) -> Result<Self, serde_json::Error>
    where
        T: serde::Serialize + ?Sized,
    {
        Ok(Self::new(name, serde_json::to_value(payload)?))
    }

    pub fn with_options(mut self, options: EnqueueOptions) -> Self {
        self.options = options;
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl JobStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether the job has finished and will not run again without an explicit [`Queue::retry`](crate::Queue::retry).
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for JobStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown job status `{0}`")]
pub struct ParseJobStatusError(String);

impl FromStr for JobStatus {
    type Err = ParseJobStatusError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(ParseJobStatusError(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Job {
    pub id: Uuid,
    pub name: String,
    pub status: JobStatus,
    pub payload: Value,
    pub queue_name: Option<String>,
    pub priority: i32,
    pub run_at: Timestamp,
    pub job_key: Option<String>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub locked_by: Option<String>,
    pub locked_at: Option<Timestamp>,
    pub error: Option<String>,
    pub queued_at: Timestamp,
    pub started_at: Option<Timestamp>,
    pub finished_at: Option<Timestamp>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Job {
    pub fn payload_as<T>(&self) -> Result<T, serde_json::Error>
    where
        T: DeserializeOwned,
    {
        serde_json::from_value(self.payload.clone())
    }
}
