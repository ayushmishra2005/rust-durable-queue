use crate::config::RuntimeConfig;
use crate::error::{Error, Result};
use crate::handler::JobContext;
use crate::stats::{QueueStats, StatsSnapshot};
use crate::store::MemoryStore;
use crate::types::{JobId, JobRecord, JobSpec, JobState, LeaseId, QueueName};
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

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
    FetchWork {
        reply: oneshot::Sender<Option<LeasedJob>>,
    },
    WorkerOutcome {
        id: JobId,
        lease_id: LeaseId,
        outcome: WorkerOutcome,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

/// Outcome reported by a worker.
#[derive(Debug)]
pub enum WorkerOutcome {
    Success,
    Retryable,
    Fatal,
    Panic,
}

/// A job leased to a worker for execution.
pub struct LeasedJob {
    pub context: JobContext,
    pub lease_id: LeaseId,
}

/// Scheduled retry entry for the delay queue.
#[derive(Debug, Eq, PartialEq)]
struct RetryEntry {
    available_at: Instant,
    job_id: JobId,
}

impl Ord for RetryEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for min-heap behavior
        other.available_at.cmp(&self.available_at)
    }
}

impl PartialOrd for RetryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Active lease state for a running job.
struct ActiveLease {
    lease_id: LeaseId,
    deadline: Instant,
    cancellation: CancellationToken,
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

/// Global statistics.
#[derive(Default)]
struct GlobalStats {
    submitted: u64,
    completed: u64,
    dead: u64,
    cancelled: u64,
    retried: u64,
    stale_outcomes: u64,
}

/// Coordinator owns all mutable queue state.
pub struct Coordinator {
    store: MemoryStore,
    queues: HashMap<String, QueueState>,
    permits: HashMap<JobId, OwnedSemaphorePermit>,
    leases: HashMap<JobId, ActiveLease>,
    retry_queue: BinaryHeap<RetryEntry>,
    semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
    cmd_rx: mpsc::Receiver<Command>,
    config: RuntimeConfig,
    next_lease_epoch: u64,
    global_stats: GlobalStats,
    shutting_down: bool,
    shutdown_token: CancellationToken,
}

impl Coordinator {
    pub fn new(
        config: RuntimeConfig,
        cmd_rx: mpsc::Receiver<Command>,
        semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
        shutdown_token: CancellationToken,
    ) -> Self {
        let mut queues = HashMap::new();
        for qc in &config.queues {
            queues.insert(qc.name.as_str().to_string(), QueueState::new());
        }

        Self {
            store: MemoryStore::new(),
            queues,
            permits: HashMap::new(),
            leases: HashMap::new(),
            retry_queue: BinaryHeap::new(),
            semaphores,
            cmd_rx,
            config,
            next_lease_epoch: 1,
            global_stats: GlobalStats::default(),
            shutting_down: false,
            shutdown_token,
        }
    }

    pub async fn run(mut self) {
        loop {
            // Process any due timers on each iteration.
            self.process_due_retries();
            self.check_lease_timeouts();

            let next_deadline = self.next_retry_deadline();

            tokio::select! {
                biased;

                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            let should_exit = self.handle(cmd);
                            if should_exit {
                                break;
                            }
                        }
                        None => break,
                    }
                }

                _ = Self::sleep_until_opt(next_deadline) => {
                    // Timer expired, loop will process on next iteration.
                }
            }
        }

        // Shutdown: close semaphores to wake blocked submitters.
        for sem in self.semaphores.values() {
            sem.close();
        }
    }

    async fn sleep_until_opt(deadline: Option<Instant>) {
        match deadline {
            Some(d) => tokio::time::sleep_until(d).await,
            None => std::future::pending().await,
        }
    }

    fn next_retry_deadline(&self) -> Option<Instant> {
        let retry_deadline = self.retry_queue.peek().map(|e| e.available_at);
        let lease_deadline = self.leases.values().map(|l| l.deadline).min();

        match (retry_deadline, lease_deadline) {
            (Some(r), Some(l)) => Some(r.min(l)),
            (Some(r), None) => Some(r),
            (None, Some(l)) => Some(l),
            (None, None) => None,
        }
    }

    fn handle(&mut self, cmd: Command) -> bool {
        match cmd {
            Command::Submit {
                queue,
                spec,
                permit,
                reply,
            } => {
                let result = if self.shutting_down {
                    Err(Error::ShuttingDown)
                } else {
                    self.do_submit(queue, spec, permit)
                };
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
            Command::FetchWork { reply } => {
                let result = if self.shutting_down {
                    None
                } else {
                    self.do_fetch_work()
                };
                let _ = reply.send(result);
            }
            Command::WorkerOutcome {
                id,
                lease_id,
                outcome,
            } => {
                self.do_worker_outcome(id, lease_id, outcome);
            }
            Command::Shutdown { reply } => {
                self.shutting_down = true;
                self.shutdown_token.cancel();
                // Cancel all active leases.
                for lease in self.leases.values() {
                    lease.cancellation.cancel();
                }
                let _ = reply.send(());
                return true;
            }
        }
        false
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
        let job = self
            .store
            .get(id)
            .ok_or_else(|| Error::JobNotFound(id.to_string()))?;

        let from_state = job.state;
        let queue_name = job.queue.clone();

        // Transition to Cancelled.
        let record = self.store.transition(id, JobState::Cancelled)?;

        // Update queue state based on previous state.
        if let Some(qs) = self.queues.get_mut(queue_name.as_str()) {
            match from_state {
                JobState::Queued => {
                    qs.ready.retain(|&jid| jid != id);
                    qs.stats.queued = qs.stats.queued.saturating_sub(1);
                }
                JobState::Running => {
                    // Cancel the active lease.
                    if let Some(lease) = self.leases.remove(&id) {
                        lease.cancellation.cancel();
                    }
                    qs.stats.running = qs.stats.running.saturating_sub(1);
                }
                JobState::RetryWaiting => {
                    qs.stats.retrying = qs.stats.retrying.saturating_sub(1);
                }
                _ => {}
            }
            qs.stats.cancelled += 1;
        }

        // Release capacity permit.
        self.permits.remove(&id);
        self.global_stats.cancelled += 1;

        Ok(record)
    }

    fn do_stats(&self) -> StatsSnapshot {
        let mut queued = 0u64;
        let mut running = 0u64;
        let mut retrying = 0u64;
        let mut completed = 0u64;
        let mut dead = 0u64;
        let mut cancelled = 0u64;
        let mut per_queue = HashMap::new();

        for (name, qs) in &self.queues {
            queued += qs.stats.queued;
            running += qs.stats.running;
            retrying += qs.stats.retrying;
            completed += qs.stats.completed;
            dead += qs.stats.dead;
            cancelled += qs.stats.cancelled;
            per_queue.insert(name.clone(), qs.stats.clone());
        }

        StatsSnapshot {
            submitted: self.global_stats.submitted,
            queued,
            running,
            retrying,
            completed,
            dead,
            cancelled,
            retried: self.global_stats.retried,
            stale_outcomes: self.global_stats.stale_outcomes,
            per_queue,
        }
    }

    fn do_fetch_work(&mut self) -> Option<LeasedJob> {
        // Find a ready job from any queue.
        for qs in self.queues.values_mut() {
            if let Some(job_id) = qs.ready.pop_front() {
                let job = self.store.get_mut(job_id)?;

                // Increment attempts and transition to Running.
                job.attempts += 1;
                job.state = JobState::Running;

                let lease_id = LeaseId::new(self.next_lease_epoch);
                self.next_lease_epoch += 1;

                let cancellation = CancellationToken::new();
                let deadline = Instant::now() + self.config.visibility_timeout;

                self.leases.insert(
                    job_id,
                    ActiveLease {
                        lease_id,
                        deadline,
                        cancellation: cancellation.clone(),
                    },
                );

                qs.stats.queued = qs.stats.queued.saturating_sub(1);
                qs.stats.running += 1;

                let context = JobContext {
                    id: job.id,
                    queue: job.queue.clone(),
                    payload: job.spec.payload.clone(),
                    attempt: job.attempts,
                    max_attempts: job.spec.max_attempts,
                    cancellation,
                };

                return Some(LeasedJob { context, lease_id });
            }
        }
        None
    }

    fn do_worker_outcome(&mut self, id: JobId, lease_id: LeaseId, outcome: WorkerOutcome) {
        // Validate lease.
        let Some(lease) = self.leases.get(&id) else {
            // Job might be cancelled or already processed.
            self.global_stats.stale_outcomes += 1;
            return;
        };

        if lease.lease_id != lease_id {
            // Stale lease: reject outcome.
            self.global_stats.stale_outcomes += 1;
            return;
        }

        // Remove lease.
        self.leases.remove(&id);

        let Some(job) = self.store.get(id) else {
            return;
        };

        let queue_name = job.queue.clone();
        let attempts = job.attempts;
        let max_attempts = job.spec.max_attempts;

        match outcome {
            WorkerOutcome::Success => {
                self.complete_job(id, &queue_name);
            }
            WorkerOutcome::Fatal => {
                self.dead_job(id, &queue_name);
            }
            WorkerOutcome::Retryable | WorkerOutcome::Panic => {
                if attempts >= max_attempts {
                    self.dead_job(id, &queue_name);
                } else {
                    self.schedule_retry(id, &queue_name, attempts);
                }
            }
        }
    }

    fn complete_job(&mut self, id: JobId, queue_name: &QueueName) {
        if let Ok(_record) = self.store.transition(id, JobState::Completed) {
            if let Some(qs) = self.queues.get_mut(queue_name.as_str()) {
                qs.stats.running = qs.stats.running.saturating_sub(1);
                qs.stats.completed += 1;
            }
            self.permits.remove(&id);
            self.global_stats.completed += 1;
        }
    }

    fn dead_job(&mut self, id: JobId, queue_name: &QueueName) {
        if let Ok(_record) = self.store.transition(id, JobState::Dead) {
            if let Some(qs) = self.queues.get_mut(queue_name.as_str()) {
                qs.stats.running = qs.stats.running.saturating_sub(1);
                qs.stats.dead += 1;
            }
            self.permits.remove(&id);
            self.global_stats.dead += 1;
        }
    }

    fn schedule_retry(&mut self, id: JobId, queue_name: &QueueName, attempt: u32) {
        if let Ok(_record) = self.store.transition(id, JobState::RetryWaiting) {
            if let Some(qs) = self.queues.get_mut(queue_name.as_str()) {
                qs.stats.running = qs.stats.running.saturating_sub(1);
                qs.stats.retrying += 1;
            }

            let delay = self.config.retry.delay_for_attempt(attempt);
            // Apply jitter using a simple deterministic approach for now.
            // In production with Jitter::Full, this would use rand.
            let jittered = self.config.retry.apply_jitter(delay, 1.0);
            let available_at = Instant::now() + jittered;

            self.retry_queue.push(RetryEntry {
                available_at,
                job_id: id,
            });

            self.global_stats.retried += 1;
        }
    }

    fn process_due_retries(&mut self) {
        let now = Instant::now();

        while let Some(entry) = self.retry_queue.peek() {
            if entry.available_at > now {
                break;
            }

            let entry = self.retry_queue.pop().unwrap();
            let job_id = entry.job_id;

            // Check job is still in RetryWaiting.
            let Some(job) = self.store.get(job_id) else {
                continue;
            };

            if job.state != JobState::RetryWaiting {
                continue;
            }

            let queue_name = job.queue.clone();

            // Transition back to Queued.
            if let Ok(_record) = self.store.transition(job_id, JobState::Queued)
                && let Some(qs) = self.queues.get_mut(queue_name.as_str())
            {
                qs.stats.retrying = qs.stats.retrying.saturating_sub(1);
                qs.stats.queued += 1;
                qs.ready.push_back(job_id);
            }
        }
    }

    fn check_lease_timeouts(&mut self) {
        let now = Instant::now();
        let mut timed_out = Vec::new();

        for (&job_id, lease) in &self.leases {
            if lease.deadline <= now {
                timed_out.push(job_id);
            }
        }

        for job_id in timed_out {
            if let Some(lease) = self.leases.remove(&job_id) {
                lease.cancellation.cancel();

                let Some(job) = self.store.get(job_id) else {
                    continue;
                };

                let queue_name = job.queue.clone();
                let attempts = job.attempts;
                let max_attempts = job.spec.max_attempts;

                // Treat timeout as retryable failure.
                if attempts >= max_attempts {
                    self.dead_job(job_id, &queue_name);
                } else {
                    self.schedule_retry(job_id, &queue_name, attempts);
                }
            }
        }
    }
}
