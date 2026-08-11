use crate::types::{JobId, QueueName};
use std::future::Future;
use tokio_util::sync::CancellationToken;

/// Error type returned by job handlers.
#[derive(Debug)]
pub enum JobError {
    /// Retryable failure; job will be retried if attempts remain.
    Retryable(Box<dyn std::error::Error + Send + Sync>),
    /// Fatal failure; job moves to Dead immediately.
    Fatal(Box<dyn std::error::Error + Send + Sync>),
}

impl JobError {
    pub fn retryable(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Retryable(Box::new(e))
    }

    pub fn fatal(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Fatal(Box::new(e))
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_))
    }
}

impl std::fmt::Display for JobError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retryable(e) => write!(f, "retryable: {}", e),
            Self::Fatal(e) => write!(f, "fatal: {}", e),
        }
    }
}

impl std::error::Error for JobError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Retryable(e) | Self::Fatal(e) => Some(e.as_ref()),
        }
    }
}

/// Context provided to job handlers during execution.
pub struct JobContext {
    pub id: JobId,
    pub queue: QueueName,
    pub payload: Vec<u8>,
    pub attempt: u32,
    pub max_attempts: u32,
    pub cancellation: CancellationToken,
}

impl JobContext {
    /// Check if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns a future that completes when cancellation is requested.
    pub fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
        self.cancellation.cancelled()
    }
}

/// Trait for job handlers.
pub trait Handler: Send + Sync + 'static {
    /// Handle a job. Returns Ok(()) on success, Err(JobError) on failure.
    fn handle(
        &self,
        ctx: JobContext,
    ) -> impl Future<Output = Result<(), JobError>> + Send + 'static;
}

/// Blanket implementation for async closures.
impl<F, Fut> Handler for F
where
    F: Fn(JobContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), JobError>> + Send + 'static,
{
    fn handle(
        &self,
        ctx: JobContext,
    ) -> impl Future<Output = Result<(), JobError>> + Send + 'static {
        (self)(ctx)
    }
}
