use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("queue not found: {0}")]
    QueueNotFound(String),

    #[error("queue is full: {0}")]
    QueueFull(String),

    #[error("invalid queue name")]
    InvalidQueueName,

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),

    #[error("job not found: {0}")]
    JobNotFound(String),

    #[error("invalid state transition: {from:?} -> {to:?}")]
    InvalidTransition {
        from: crate::types::JobState,
        to: crate::types::JobState,
    },

    #[error("stale lease: job {0} has moved to a newer lease")]
    StaleLease(String),

    #[error("queue system is shutting down")]
    ShuttingDown,
}

pub type Result<T> = std::result::Result<T, Error>;
