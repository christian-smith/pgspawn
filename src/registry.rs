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

    pub fn build(self) -> Registry {
        Registry {
            handlers: Arc::new(self.handlers),
        }
    }
}
