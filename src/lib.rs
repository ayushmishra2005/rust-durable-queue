#![forbid(unsafe_code)]

mod config;
mod coordinator;
mod error;
mod handle;
mod stats;
mod store;
mod types;

pub use config::{QueueConfig, RuntimeConfig};
pub use error::{Error, Result};
pub use handle::QueueHandle;
pub use stats::StatsSnapshot;
pub use types::{JobId, JobRecord, JobSpec, JobState, QueueName};

use coordinator::Coordinator;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};

/// Starts the queue system with the given configuration.
///
/// Returns a handle for submitting jobs and querying state.
pub async fn start(config: RuntimeConfig) -> Result<QueueHandle> {
    config.validate()?;

    // Create a semaphore per queue for capacity control.
    let mut semaphores = HashMap::new();
    for qc in &config.queues {
        let sem = Arc::new(Semaphore::new(qc.capacity));
        semaphores.insert(qc.name.as_str().to_string(), sem);
    }
    let semaphores = Arc::new(semaphores);

    let (cmd_tx, cmd_rx) = mpsc::channel(config.channel_capacity);
    let coordinator = Coordinator::new(config, cmd_rx, semaphores.clone());

    tokio::spawn(coordinator.run());

    Ok(QueueHandle::new(cmd_tx, semaphores))
}
