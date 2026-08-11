use crate::coordinator::Command;
use crate::error::{Error, Result};
use crate::stats::StatsSnapshot;
use crate::types::{JobId, JobRecord, JobSpec, QueueName};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc, oneshot};

/// Handle for interacting with the queue system.
///
/// Cloning produces handles that share the same underlying queue.
#[derive(Clone)]
pub struct QueueHandle {
    cmd_tx: mpsc::Sender<Command>,
    semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
}

impl QueueHandle {
    pub(crate) fn new(
        cmd_tx: mpsc::Sender<Command>,
        semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
    ) -> Self {
        Self { cmd_tx, semaphores }
    }

    /// Closes all capacity semaphores, waking blocked submitters with an error.
    pub fn shutdown(&self) {
        for sem in self.semaphores.values() {
            sem.close();
        }
    }

    /// Submits a job, waiting for capacity if the queue is full.
    pub async fn submit(&self, queue: QueueName, spec: JobSpec) -> Result<JobRecord> {
        let sem = self
            .semaphores
            .get(queue.as_str())
            .ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;

        // Acquire permit; blocks until capacity available or semaphore closed.
        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::ShuttingDown)?;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Submit {
                queue,
                spec,
                permit,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Error::ShuttingDown)?;

        reply_rx.await.map_err(|_| Error::ShuttingDown)?
    }

    /// Attempts to submit a job without waiting. Returns `QueueFull` if at capacity.
    pub async fn try_submit(&self, queue: QueueName, spec: JobSpec) -> Result<JobRecord> {
        let sem = self
            .semaphores
            .get(queue.as_str())
            .ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;

        // Non-blocking permit acquisition.
        let permit = sem
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::QueueFull(queue.to_string()))?;

        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Submit {
                queue,
                spec,
                permit,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Error::ShuttingDown)?;

        reply_rx.await.map_err(|_| Error::ShuttingDown)?
    }

    /// Retrieves the current status of a job.
    pub async fn status(&self, id: JobId) -> Result<JobRecord> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Status {
                id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Error::ShuttingDown)?;

        reply_rx.await.map_err(|_| Error::ShuttingDown)?
    }

    /// Cancels a queued job. Only queued jobs can be cancelled.
    pub async fn cancel(&self, id: JobId) -> Result<JobRecord> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Cancel {
                id,
                reply: reply_tx,
            })
            .await
            .map_err(|_| Error::ShuttingDown)?;

        reply_rx.await.map_err(|_| Error::ShuttingDown)?
    }

    /// Returns a snapshot of current statistics.
    pub async fn stats(&self) -> Result<StatsSnapshot> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::Stats { reply: reply_tx })
            .await
            .map_err(|_| Error::ShuttingDown)?;

        reply_rx.await.map_err(|_| Error::ShuttingDown)
    }
}
