use crate::config::RuntimeConfig;
use crate::coordinator::{Command, Coordinator};
use crate::error::{Error, Result};
use crate::handler::Handler;
use crate::stats::StatsSnapshot;
use crate::store::Storage;
use crate::types::{JobId, JobRecord, JobSpec, QueueName};
use crate::worker::run_workers;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// Runtime managing the queue system.
pub struct Runtime {
    cmd_tx: mpsc::Sender<Command>,
    semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
    shutdown_token: CancellationToken,
    worker_handle: Option<tokio::task::JoinHandle<()>>,
    coordinator_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Runtime {
    /// Starts the runtime with the given handler.
    pub async fn start<H: Handler>(config: RuntimeConfig, handler: H) -> Result<Self> {
        config.validate()?;

        // Open storage based on config.
        let storage = Storage::open(&config.storage)?;

        let mut semaphores = HashMap::new();
        for qc in &config.queues {
            let sem = Arc::new(Semaphore::new(qc.capacity));
            semaphores.insert(qc.name.as_str().to_string(), sem);
        }
        let semaphores = Arc::new(semaphores);

        let shutdown_token = CancellationToken::new();
        let (cmd_tx, cmd_rx) = mpsc::channel(config.channel_capacity);

        let coordinator = Coordinator::new(
            config.clone(),
            storage,
            cmd_rx,
            semaphores.clone(),
            shutdown_token.clone(),
        );

        let coordinator_handle = tokio::spawn(coordinator.run());

        let handler = Arc::new(handler);
        let worker_cmd_tx = cmd_tx.clone();
        let worker_shutdown = shutdown_token.clone();
        let concurrency = config.worker_concurrency;

        let worker_handle = tokio::spawn(async move {
            run_workers(handler, worker_cmd_tx, concurrency, worker_shutdown).await
        });

        Ok(Self {
            cmd_tx,
            semaphores,
            shutdown_token,
            worker_handle: Some(worker_handle),
            coordinator_handle: Some(coordinator_handle),
        })
    }

    /// Submits a job, waiting for capacity if the queue is full.
    pub async fn submit(&self, queue: QueueName, spec: JobSpec) -> Result<JobRecord> {
        let sem = self
            .semaphores
            .get(queue.as_str())
            .ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;

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

    /// Cancels a job in any non-terminal state.
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

    /// Initiates graceful shutdown.
    /// Waits for running jobs to complete up to shutdown_timeout, then cancels remaining.
    pub async fn shutdown(mut self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _ = self
            .cmd_tx
            .send(Command::Shutdown { reply: reply_tx })
            .await;

        let _ = reply_rx.await;

        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.await;
        }

        drop(self.cmd_tx);

        if let Some(handle) = self.coordinator_handle.take() {
            let _ = handle.await;
        }
    }

    /// Returns whether shutdown has been initiated.
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_token.is_cancelled()
    }
}
