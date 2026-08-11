use crate::config::RuntimeConfig;
use crate::error::{Error, Result};
use crate::handler::JobContext;
use crate::stats::{QueueStats, StatsSnapshot};
use crate::store::MemoryStore;
use crate::types::{JobId, JobRecord, JobSpec, JobState, LeaseId, QueueName};
use rand::SeedableRng;
use rand::rngs::SmallRng;
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
    // Queue names in config order for round-robin scheduling.
    queue_order: Vec<String>,
    // Next queue index for round-robin.
    next_queue_idx: usize,
    permits: HashMap<JobId, OwnedSemaphorePermit>,
    leases: HashMap<JobId, ActiveLease>,
    retry_queue: BinaryHeap<RetryEntry>,
    // Parked worker requests. Bounded by worker_concurrency (one per worker).
    parked_workers: VecDeque<oneshot::Sender<Option<LeasedJob>>>,
    semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
    cmd_rx: mpsc::Receiver<Command>,
    config: RuntimeConfig,
    rng: SmallRng,
    next_lease_epoch: u64,
    global_stats: GlobalStats,
    shutting_down: bool,
    shutdown_deadline: Option<Instant>,
    shutdown_reply: Option<oneshot::Sender<()>>,
    shutdown_token: CancellationToken,
}

impl Coordinator {
    pub fn new(
        config: RuntimeConfig,
        cmd_rx: mpsc::Receiver<Command>,
        semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self::with_rng(
            config,
            cmd_rx,
            semaphores,
            shutdown_token,
            SmallRng::from_rng(&mut rand::rng()),
        )
    }

    pub fn with_rng(
        config: RuntimeConfig,
        cmd_rx: mpsc::Receiver<Command>,
        semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
        shutdown_token: CancellationToken,
        rng: SmallRng,
    ) -> Self {
        let mut queues = HashMap::new();
        let mut queue_order = Vec::with_capacity(config.queues.len());
        for qc in &config.queues {
            let name = qc.name.as_str().to_string();
            queues.insert(name.clone(), QueueState::new());
            queue_order.push(name);
        }

        Self {
            store: MemoryStore::new(),
            queues,
            queue_order,
            next_queue_idx: 0,
            permits: HashMap::new(),
            leases: HashMap::new(),
            retry_queue: BinaryHeap::new(),
            parked_workers: VecDeque::new(),
            semaphores,
            cmd_rx,
            config,
            rng,
            next_lease_epoch: 1,
            global_stats: GlobalStats::default(),
            shutting_down: false,
            shutdown_deadline: None,
            shutdown_reply: None,
            shutdown_token,
        }
    }

    pub async fn run(mut self) {
        loop {
            self.process_due_retries();
            self.check_lease_timeouts();
            self.try_dispatch_parked_workers();

            // Check if graceful shutdown can complete.
            if self.shutting_down && self.leases.is_empty() {
                self.complete_shutdown();
                break;
            }

            // Check shutdown timeout.
            if let Some(deadline) = self.shutdown_deadline
                && Instant::now() >= deadline
            {
                self.force_shutdown();
                break;
            }

            let next_deadline = self.next_deadline();

            tokio::select! {
                biased;

                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle(cmd),
                        None => break,
                    }
                }

                _ = Self::sleep_until_opt(next_deadline) => {
                    // Timer expired, loop processes on next iteration.
                }
            }
        }

        // Close semaphores to wake blocked submitters.
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

    fn next_deadline(&self) -> Option<Instant> {
        let retry_deadline = self.retry_queue.peek().map(|e| e.available_at);
        let lease_deadline = self.leases.values().map(|l| l.deadline).min();

        let mut deadline = match (retry_deadline, lease_deadline) {
            (Some(r), Some(l)) => Some(r.min(l)),
            (Some(r), None) => Some(r),
            (None, Some(l)) => Some(l),
            (None, None) => None,
        };

        // Include shutdown deadline.
        if let Some(sd) = self.shutdown_deadline {
            deadline = Some(deadline.map_or(sd, |d| d.min(sd)));
        }

        deadline
    }

    fn handle(&mut self, cmd: Command) {
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
                self.do_fetch_work(reply);
            }
            Command::WorkerOutcome {
                id,
                lease_id,
                outcome,
            } => {
                self.do_worker_outcome(id, lease_id, outcome);
            }
            Command::Shutdown { reply } => {
                self.start_shutdown(reply);
            }
        }
    }

    fn start_shutdown(&mut self, reply: oneshot::Sender<()>) {
        if self.shutting_down {
            let _ = reply.send(());
            return;
        }

        self.shutting_down = true;
        self.shutdown_token.cancel();

        // Close semaphores to wake blocked submitters.
        for sem in self.semaphores.values() {
            sem.close();
        }

        // Wake all parked workers with None (no more work).
        while let Some(worker) = self.parked_workers.pop_front() {
            let _ = worker.send(None);
        }

        // If no running jobs, complete immediately.
        if self.leases.is_empty() {
            let _ = reply.send(());
            return;
        }

        // Set deadline and save reply for later.
        self.shutdown_deadline = Some(Instant::now() + self.config.shutdown_timeout);
        self.shutdown_reply = Some(reply);
    }

    fn complete_shutdown(&mut self) {
        if let Some(reply) = self.shutdown_reply.take() {
            let _ = reply.send(());
        }
    }

    fn force_shutdown(&mut self) {
        // Cancel all remaining leases after timeout.
        let job_ids: Vec<_> = self.leases.keys().copied().collect();
        for job_id in job_ids {
            if let Some(lease) = self.leases.remove(&job_id) {
                lease.cancellation.cancel();
            }
        }
        self.complete_shutdown();
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

        // Try to wake a parked worker.
        self.try_dispatch_parked_workers();

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

        let record = self.store.transition(id, JobState::Cancelled)?;

        if let Some(qs) = self.queues.get_mut(queue_name.as_str()) {
            match from_state {
                JobState::Queued => {
                    qs.ready.retain(|&jid| jid != id);
                    qs.stats.queued = qs.stats.queued.saturating_sub(1);
                }
                JobState::Running => {
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

    fn do_fetch_work(&mut self, reply: oneshot::Sender<Option<LeasedJob>>) {
        if self.shutting_down {
            let _ = reply.send(None);
            return;
        }

        // Try to assign work immediately.
        if let Some(job) = self.try_lease_job() {
            let _ = reply.send(Some(job));
            return;
        }

        // Park the worker until work is available.
        self.parked_workers.push_back(reply);
    }

    /// Round-robin selection across queues.
    fn try_lease_job(&mut self) -> Option<LeasedJob> {
        let n = self.queue_order.len();
        if n == 0 {
            return None;
        }

        for _ in 0..n {
            let queue_name = self.queue_order[self.next_queue_idx].clone();
            self.next_queue_idx = (self.next_queue_idx + 1) % n;

            let job_id = match self.queues.get_mut(&queue_name) {
                Some(qs) => match qs.ready.pop_front() {
                    Some(id) => id,
                    None => continue,
                },
                None => continue,
            };

            return self.lease_job(job_id, &queue_name);
        }
        None
    }

    fn lease_job(&mut self, job_id: JobId, queue_name: &str) -> Option<LeasedJob> {
        let job = self.store.get_mut(job_id)?;

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

        if let Some(qs) = self.queues.get_mut(queue_name) {
            qs.stats.queued = qs.stats.queued.saturating_sub(1);
            qs.stats.running += 1;
        }

        let context = JobContext {
            id: job.id,
            queue: job.queue.clone(),
            payload: job.spec.payload.clone(),
            attempt: job.attempts,
            max_attempts: job.spec.max_attempts,
            cancellation,
        };

        Some(LeasedJob { context, lease_id })
    }

    /// Dispatch work to parked workers if available.
    fn try_dispatch_parked_workers(&mut self) {
        if self.shutting_down {
            return;
        }

        while !self.parked_workers.is_empty() {
            let Some(job) = self.try_lease_job() else {
                break;
            };
            if let Some(worker) = self.parked_workers.pop_front()
                && worker.send(Some(job)).is_err()
            {
                continue;
            }
        }
    }

    fn do_worker_outcome(&mut self, id: JobId, lease_id: LeaseId, outcome: WorkerOutcome) {
        let Some(lease) = self.leases.get(&id) else {
            self.global_stats.stale_outcomes += 1;
            return;
        };

        if lease.lease_id != lease_id {
            self.global_stats.stale_outcomes += 1;
            return;
        }

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

            let delay = self.config.retry.delay_with_rng(attempt, &mut self.rng);
            let available_at = Instant::now() + delay;

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

            let Some(job) = self.store.get(job_id) else {
                continue;
            };

            if job.state != JobState::RetryWaiting {
                continue;
            }

            let queue_name = job.queue.clone();

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

                if attempts >= max_attempts {
                    self.dead_job(job_id, &queue_name);
                } else {
                    self.schedule_retry(job_id, &queue_name, attempts);
                }
            }
        }
    }
}
