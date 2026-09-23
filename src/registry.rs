use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    future::Future,
    pin::Pin,
    sync::Arc,
};

use serde::de::DeserializeOwned;

use crate::Job;

pub(crate) type BoxJobFuture = Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send>>;
pub(crate) type JobHandler = Arc<dyn Fn(Job) -> BoxJobFuture + Send + Sync>;

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct HandlerError {
    message: String,
}

impl HandlerError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Clone, Default)]
pub struct Registry {
    handlers: Arc<HashMap<String, JobHandler>>,
}

impl Registry {
    pub fn builder() -> RegistryBuilder {
        RegistryBuilder::default()
    }

    pub(crate) fn names(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }

    pub(crate) fn get(&self, name: &str) -> Option<JobHandler> {
        self.handlers.get(name).cloned()
    }
}

#[derive(Default)]
pub struct RegistryBuilder {
    handlers: HashMap<String, JobHandler>,
}

impl RegistryBuilder {
    pub fn register<F, Fut, E>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(Job) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Debug + Display + Send + 'static,
    {
        self.handlers.insert(
            name.into(),
            Arc::new(move |job| {
                let future = handler(job);
                Box::pin(async move {
                    future
                        .await
                        .map_err(|err| HandlerError::new(err.to_string()))
                })
            }),
        );
        self
    }

    pub fn register_typed<P, F, Fut, E>(mut self, name: impl Into<String>, handler: F) -> Self
    where
        P: DeserializeOwned + Send + 'static,
        F: Fn(Job, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Debug + Display + Send + 'static,
    {
        self.handlers.insert(
            name.into(),
            Arc::new(move |job| {
                let payload = job.payload_as();
                let future = payload.map(|payload| handler(job, payload));
                Box::pin(async move {
                    let future = future.map_err(|err| HandlerError::new(err.to_string()))?;
                    future
                        .await
                        .map_err(|err| HandlerError::new(err.to_string()))
                })
            }),
        );
        self
    }

    /// Registers a handler together with cloned application state.
    ///
    /// Each invocation receives a fresh `state` clone so the outer
    /// registration does not need the double-`clone` dance that a raw
    /// [`register`](Self::register) closure requires.
    pub fn register_with<S, F, Fut, E>(self, name: impl Into<String>, state: S, handler: F) -> Self
    where
        S: Clone + Send + Sync + 'static,
        F: Fn(Job, S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Debug + Display + Send + 'static,
    {
        self.register(name, move |job| handler(job, state.clone()))
    }

    /// Typed-payload variant of [`register_with`](Self::register_with).
    pub fn register_typed_with<S, P, F, Fut, E>(
        self,
        name: impl Into<String>,
        state: S,
        handler: F,
    ) -> Self
    where
        S: Clone + Send + Sync + 'static,
        P: DeserializeOwned + Send + 'static,
        F: Fn(Job, P, S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: Debug + Display + Send + 'static,
    {
        self.register_typed(name, move |job, payload: P| {
            handler(job, payload, state.clone())
        })
    }

    /// Registers a handler that always succeeds. Suits periodic maintenance
    /// jobs that log and absorb their own failures instead of using the
    /// retry machinery: the handler returns `()`, the job is recorded as
    /// succeeded, and the next occurrence comes from its schedule.
    pub fn register_infallible<F, Fut>(self, name: impl Into<String>, handler: F) -> Self
    where
        F: Fn(Job) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.register(name, move |job| {
            let future = handler(job);
            async move {
                future.await;
                Ok::<(), std::convert::Infallible>(())
            }
        })
    }

    /// [`register_infallible`](Self::register_infallible) with cloned
    /// application state, matching [`register_with`](Self::register_with).
    pub fn register_infallible_with<S, F, Fut>(
        self,
        name: impl Into<String>,
        state: S,
        handler: F,
    ) -> Self
    where
        S: Clone + Send + Sync + 'static,
        F: Fn(Job, S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.register_infallible(name, move |job| handler(job, state.clone()))
    }

    /// Typed-payload variant of
    /// [`register_infallible`](Self::register_infallible). Payload
    /// deserialization failures still fail the job, since the handler never
    /// runs without a payload.
    pub fn register_typed_infallible<P, F, Fut>(self, name: impl Into<String>, handler: F) -> Self
    where
        P: DeserializeOwned + Send + 'static,
        F: Fn(Job, P) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.register_typed(name, move |job, payload: P| {
            let future = handler(job, payload);
            async move {
                future.await;
                Ok::<(), std::convert::Infallible>(())
            }
        })
    }

    /// [`register_typed_infallible`](Self::register_typed_infallible) with
    /// cloned application state.
    pub fn register_typed_infallible_with<S, P, F, Fut>(
        self,
        name: impl Into<String>,
        state: S,
        handler: F,
    ) -> Self
    where
        S: Clone + Send + Sync + 'static,
        P: DeserializeOwned + Send + 'static,
        F: Fn(Job, P, S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.register_typed_infallible(name, move |job, payload: P| {
            handler(job, payload, state.clone())
        })
    }

    pub fn build(self) -> Registry {
        Registry {
            handlers: Arc::new(self.handlers),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::Registry;
    use crate::{Job, JobStatus};

    fn job(name: &str, payload: serde_json::Value) -> Job {
        Job {
            id: uuid::Uuid::nil(),
            name: name.to_string(),
            status: JobStatus::Running,
            payload,
            queue_name: None,
            priority: 0,
            run_at: jiff::Timestamp::UNIX_EPOCH,
            job_key: None,
            attempt: 1,
            max_attempts: 1,
            locked_by: None,
            locked_at: None,
            error: None,
            queued_at: jiff::Timestamp::UNIX_EPOCH,
            started_at: None,
            finished_at: None,
            created_at: jiff::Timestamp::UNIX_EPOCH,
            updated_at: jiff::Timestamp::UNIX_EPOCH,
        }
    }

    #[tokio::test]
    async fn infallible_handlers_always_record_success() {
        let registry = Registry::builder()
            .register_infallible("noop", |_job| async {})
            .build();
        let handler = registry.get("noop").expect("registered handler");
        handler(job("noop", json!({}))).await.expect("always Ok");
    }

    #[tokio::test]
    async fn infallible_with_clones_state_per_invocation() {
        use std::sync::atomic::{AtomicU32, Ordering};

        let hits = std::sync::Arc::new(AtomicU32::new(0));
        let registry = Registry::builder()
            .register_infallible_with("count", hits.clone(), |_job, hits| async move {
                hits.fetch_add(1, Ordering::SeqCst);
            })
            .build();
        let handler = registry.get("count").expect("registered handler");
        handler(job("count", json!({})))
            .await
            .expect("first invocation");
        handler(job("count", json!({})))
            .await
            .expect("second invocation");
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn typed_infallible_handlers_still_fail_on_bad_payloads() {
        #[derive(Deserialize)]
        struct Payload {
            #[allow(dead_code)]
            required: String,
        }

        let registry = Registry::builder()
            .register_typed_infallible("typed", |_job, _payload: Payload| async {})
            .build();
        let handler = registry.get("typed").expect("registered handler");
        handler(job("typed", json!({"required": "present"})))
            .await
            .expect("valid payload succeeds");
        handler(job("typed", json!({})))
            .await
            .expect_err("missing payload field must fail the job");
    }
}
