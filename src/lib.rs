#![forbid(unsafe_code)]

mod config;
mod coordinator;
mod error;
mod handle;
mod handler;
mod runtime;
mod stats;
mod store;
mod types;
mod worker;

pub use config::{Jitter, QueueConfig, RetryConfig, RuntimeConfig};
pub use error::{Error, Result};
pub use handle::QueueHandle;
pub use handler::{Handler, JobContext, JobError};
pub use runtime::Runtime;
pub use stats::{QueueStats, StatsSnapshot};
pub use types::{JobId, JobRecord, JobSpec, JobState, LeaseId, QueueName};

use coordinator::Coordinator;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

/// Starts the queue system without workers (for backward compatibility).
///
/// Returns a handle for submitting jobs and querying state.
/// Use `Runtime::start` for full worker support.
pub async fn start(config: RuntimeConfig) -> Result<QueueHandle> {
    config.validate()?;

    let mut semaphores = HashMap::new();
    for qc in &config.queues {
        let sem = Arc::new(Semaphore::new(qc.capacity));
        semaphores.insert(qc.name.as_str().to_string(), sem);
    }
    let semaphores = Arc::new(semaphores);

    let shutdown_token = CancellationToken::new();
    let (cmd_tx, cmd_rx) = mpsc::channel(config.channel_capacity);
    let coordinator = Coordinator::new(config, cmd_rx, semaphores.clone(), shutdown_token);

    tokio::spawn(coordinator.run());

    Ok(QueueHandle::new(cmd_tx, semaphores))
}
