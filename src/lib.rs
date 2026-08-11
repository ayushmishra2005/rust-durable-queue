#![forbid(unsafe_code)]

mod config;
mod coordinator;
mod error;
mod handle;
mod handler;
mod recovery;
mod runtime;
mod stats;
mod store;
mod types;
pub mod wal;
mod worker;

pub use config::{Jitter, QueueConfig, RetryConfig, RuntimeConfig};
pub use error::{Error, Result};
pub use handle::QueueHandle;
pub use handler::{Handler, JobContext, JobError};
pub use runtime::Runtime;
pub use stats::{QueueStats, StatsSnapshot};
pub use store::StorageConfig;
pub use types::{JobId, JobRecord, JobSpec, JobState, LeaseId, QueueName, UnixMillis};

use coordinator::Coordinator;
use recovery::RecoveredState;
use std::collections::HashMap;
use std::sync::Arc;
use store::Storage;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

/// Starts the queue system without workers (for backward compatibility).
///
/// Returns a handle for submitting jobs and querying state.
/// Use `Runtime::start` for full worker support.
pub async fn start(config: RuntimeConfig) -> Result<QueueHandle> {
    config.validate()?;

    let (storage, recovered_state) = Storage::open_with_recovery(&config)?;

    let mut semaphores = HashMap::new();
    for qc in &config.queues {
        let sem = Arc::new(Semaphore::new(qc.capacity));
        semaphores.insert(qc.name.as_str().to_string(), sem);
    }
    let semaphores = Arc::new(semaphores);

    // Reserve permits for recovered jobs.
    if let Some(ref recovered) = recovered_state {
        reserve_permits_for_recovered(&semaphores, recovered)?;
    }

    let shutdown_token = CancellationToken::new();
    let (cmd_tx, cmd_rx) = mpsc::channel(config.channel_capacity);
    let coordinator = Coordinator::new_with_recovery(
        config,
        storage,
        cmd_rx,
        semaphores.clone(),
        shutdown_token,
        recovered_state,
    );

    tokio::spawn(coordinator.run());

    Ok(QueueHandle::new(cmd_tx, semaphores))
}

/// Reserve semaphore permits for recovered jobs.
fn reserve_permits_for_recovered(
    semaphores: &HashMap<String, Arc<Semaphore>>,
    recovered: &RecoveredState,
) -> Result<()> {
    use crate::types::JobState;

    // Count permits needed per queue.
    let mut permits_needed: HashMap<String, usize> = HashMap::new();

    // Queued jobs.
    for (queue, jobs) in &recovered.queued_jobs {
        *permits_needed.entry(queue.clone()).or_default() += jobs.len();
    }

    // Retry jobs.
    for (job_id, _) in &recovered.retry_jobs {
        if let Some(job) = recovered.store.get(*job_id)
            && job.state == JobState::RetryWaiting
        {
            let queue = job.queue.as_str().to_string();
            *permits_needed.entry(queue).or_default() += 1;
        }
    }

    // Reserve permits (they won't be released until job completes).
    for (queue, count) in permits_needed {
        if let Some(sem) = semaphores.get(&queue) {
            for _ in 0..count {
                // try_acquire_owned to reserve the permit.
                let _permit = sem.clone().try_acquire_owned().map_err(|_| {
                    Error::Storage(wal::WalError::RecoveryCapacityExceeded {
                        queue: queue.clone(),
                        recovered: count,
                        capacity: sem.available_permits() + count,
                    })
                })?;
                // Intentionally leak the permit - it will be managed by coordinator.
                std::mem::forget(_permit);
            }
        }
    }

    Ok(())
}
