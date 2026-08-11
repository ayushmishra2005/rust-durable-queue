use crate::config::RuntimeConfig;
use crate::error::{Error, Result};
use crate::stats::{QueueStats, StatsSnapshot};
use crate::store::MemoryStore;
use crate::types::{JobId, JobRecord, JobSpec, JobState, QueueName};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

/// Commands sent to the coordinator.
pub enum Command {
    Submit {
        queue: QueueName,
        spec: JobSpec,
        permit: OwnedSemaphorePermit,
        reply: oneshot::Sender<Result<JobRecord>>,
    },
    Status {
        id: JobId,
        reply: oneshot::Sender<Result<JobRecord>>,
    },
    Cancel {
        id: JobId,
        reply: oneshot::Sender<Result<JobRecord>>,
    },
    Stats {
        reply: oneshot::Sender<StatsSnapshot>,
    },
}

/// Per-queue state owned by the coordinator.
struct QueueState {
    ready: VecDeque<JobId>,
    stats: QueueStats,
}

impl QueueState {
    fn new() -> Self {
        Self {
            ready: VecDeque::new(),
            stats: QueueStats::default(),
        }
    }
}

/// Coordinator owns all mutable queue state.
pub struct Coordinator {
    store: MemoryStore,
    queues: HashMap<String, QueueState>,
    permits: HashMap<JobId, OwnedSemaphorePermit>,
    semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
    cmd_rx: mpsc::Receiver<Command>,
    global_stats: GlobalStats,
}

#[derive(Default)]
struct GlobalStats {
    submitted: u64,
    cancelled: u64,
}

impl Coordinator {
    pub fn new(
        config: RuntimeConfig,
        cmd_rx: mpsc::Receiver<Command>,
        semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
    ) -> Self {
        let mut queues = HashMap::new();
        for qc in config.queues {
            queues.insert(qc.name.as_str().to_string(), QueueState::new());
        }

        Self {
            store: MemoryStore::new(),
            queues,
            permits: HashMap::new(),
            semaphores,
            cmd_rx,
            global_stats: GlobalStats::default(),
        }
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.cmd_rx.recv().await {
            self.handle(cmd);
        }
        // Close semaphores to wake any blocked submitters.
        for sem in self.semaphores.values() {
            sem.close();
        }
    }

    fn handle(&mut self, cmd: Command) {
        match cmd {
            Command::Submit {
                queue,
                spec,
                permit,
                reply,
            } => {
                let result = self.do_submit(queue, spec, permit);
                let _ = reply.send(result);
            }
            Command::Status { id, reply } => {
                let result = self.do_status(id);
                let _ = reply.send(result);
            }
            Command::Cancel { id, reply } => {
                let result = self.do_cancel(id);
                let _ = reply.send(result);
            }
            Command::Stats { reply } => {
                let stats = self.do_stats();
                let _ = reply.send(stats);
            }
        }
    }

    fn do_submit(
        &mut self,
        queue: QueueName,
        spec: JobSpec,
        permit: OwnedSemaphorePermit,
    ) -> Result<JobRecord> {
        let qs = self
            .queues
            .get_mut(queue.as_str())
            .ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;

        let id = JobId::new();
        let record = self.store.insert(id, queue, spec);

        // Store permit; released when job reaches terminal state.
        self.permits.insert(id, permit);

        qs.ready.push_back(id);
        qs.stats.submitted += 1;
        qs.stats.queued += 1;
        self.global_stats.submitted += 1;

        Ok(record)
    }

    fn do_status(&self, id: JobId) -> Result<JobRecord> {
        self.store
            .get(id)
            .cloned()
            .ok_or_else(|| Error::JobNotFound(id.to_string()))
    }

    fn do_cancel(&mut self, id: JobId) -> Result<JobRecord> {
        let record = self.store.transition(id, JobState::Cancelled)?;

        if let Some(qs) = self.queues.get_mut(record.queue.as_str()) {
            qs.ready.retain(|&jid| jid != id);
            qs.stats.queued = qs.stats.queued.saturating_sub(1);
            qs.stats.cancelled += 1;
        }

        // Release capacity by dropping the permit.
        self.permits.remove(&id);

        self.global_stats.cancelled += 1;

        Ok(record)
    }

    fn do_stats(&self) -> StatsSnapshot {
        let mut queued = 0u64;
        let mut per_queue = HashMap::new();

        for (name, qs) in &self.queues {
            queued += qs.stats.queued;
            per_queue.insert(name.clone(), qs.stats.clone());
        }

        StatsSnapshot {
            submitted: self.global_stats.submitted,
            queued,
            cancelled: self.global_stats.cancelled,
            per_queue,
        }
    }
}
