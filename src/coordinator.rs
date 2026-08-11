use crate::config::RuntimeConfig;
use crate::error::{Error, Result};
use crate::handler::JobContext;
use crate::recovery::RecoveredState;
use crate::stats::{QueueStats, StatsSnapshot};
use crate::store::{MemoryStore, Storage};
use crate::types::{
    JobId, JobRecord, JobSpec, JobState, LeaseId, MAX_PAYLOAD_SIZE, QueueName, UnixMillis,
};
use crate::wal::WalRecord;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
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
    wall_clock_available_at: UnixMillis, // Persisted time for future replay.
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
    storage: Storage,
    queues: HashMap<String, QueueState>,
    queue_order: Vec<String>,
    next_queue_idx: usize,
    permits: HashMap<JobId, OwnedSemaphorePermit>,
    leases: HashMap<JobId, ActiveLease>,
    retry_queue: BinaryHeap<RetryEntry>,
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
    #[allow(dead_code)] // Used in tests and start() without recovery.
    pub fn new(
        config: RuntimeConfig,
        storage: Storage,
        cmd_rx: mpsc::Receiver<Command>,
        semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
        shutdown_token: CancellationToken,
    ) -> Self {
        Self::new_with_recovery(config, storage, cmd_rx, semaphores, shutdown_token, None)
    }

    pub fn new_with_recovery(
        config: RuntimeConfig,
        storage: Storage,
        cmd_rx: mpsc::Receiver<Command>,
        semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
        shutdown_token: CancellationToken,
        recovered: Option<RecoveredState>,
    ) -> Self {
        Self::with_rng_and_recovery(
            config,
            storage,
            cmd_rx,
            semaphores,
            shutdown_token,
            SmallRng::from_rng(&mut rand::rng()),
            recovered,
        )
    }

    #[allow(dead_code)] // Used in tests.
    pub fn with_rng(
        config: RuntimeConfig,
        storage: Storage,
        cmd_rx: mpsc::Receiver<Command>,
        semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
        shutdown_token: CancellationToken,
        rng: SmallRng,
    ) -> Self {
        Self::with_rng_and_recovery(
            config,
            storage,
            cmd_rx,
            semaphores,
            shutdown_token,
            rng,
            None,
        )
    }

    pub fn with_rng_and_recovery(
        config: RuntimeConfig,
        storage: Storage,
        cmd_rx: mpsc::Receiver<Command>,
        semaphores: Arc<HashMap<String, Arc<Semaphore>>>,
        shutdown_token: CancellationToken,
        rng: SmallRng,
        recovered: Option<RecoveredState>,
    ) -> Self {
        let mut queues = HashMap::new();
        let mut queue_order = Vec::with_capacity(config.queues.len());
        for qc in &config.queues {
            let name = qc.name.as_str().to_string();
            queues.insert(name.clone(), QueueState::new());
            queue_order.push(name);
        }

        // Initialize from recovered state if present.
        let (store, next_lease_epoch, global_stats, retry_queue) = if let Some(rec) = recovered {
            let store = rec.store;
            let next_lease_epoch = rec.next_lease_epoch;
            let now = Instant::now();
            let wall_now = UnixMillis::now();

            // Rebuild ready queues from recovered queued jobs.
            for (queue_name, job_ids) in &rec.queued_jobs {
                if let Some(qs) = queues.get_mut(queue_name) {
                    for &job_id in job_ids {
                        if let Some(job) = store.get(job_id)
                            && job.state == JobState::Queued
                        {
                            qs.ready.push_back(job_id);
                            qs.stats.queued += 1;
                        }
                    }
                }
            }

            // Build retry queue from recovered retry jobs.
            let mut retry_queue = BinaryHeap::new();
            for (job_id, available_at) in &rec.retry_jobs {
                if let Some(job) = store.get(*job_id)
                    && job.state == JobState::RetryWaiting
                {
                    // Calculate monotonic deadline from wall-clock delta.
                    let remaining_ms = (available_at.as_millis() - wall_now.as_millis()).max(0);
                    let delay = Duration::from_millis(remaining_ms as u64);
                    let deadline = now + delay;

                    retry_queue.push(RetryEntry {
                        available_at: deadline,
                        wall_clock_available_at: *available_at,
                        job_id: *job_id,
                    });

                    if let Some(qs) = queues.get_mut(job.queue.as_str()) {
                        qs.stats.retrying += 1;
                    }
                }
            }

            // Rebuild per-queue stats for terminal states.
            for job in store.jobs() {
                if let Some(qs) = queues.get_mut(job.queue.as_str()) {
                    match job.state {
                        JobState::Completed => qs.stats.completed += 1,
                        JobState::Dead => qs.stats.dead += 1,
                        JobState::Cancelled => qs.stats.cancelled += 1,
                        _ => {}
                    }
                }
            }

            let global_stats = GlobalStats {
                submitted: rec.submitted,
                completed: rec.completed,
                dead: rec.dead,
                cancelled: rec.cancelled,
                retried: rec.retried,
                stale_outcomes: 0,
            };

            (store, next_lease_epoch, global_stats, retry_queue)
        } else {
            (
                MemoryStore::new(),
                1,
                GlobalStats::default(),
                BinaryHeap::new(),
            )
        };

        Self {
            store,
            storage,
            queues,
            queue_order,
            next_queue_idx: 0,
            permits: HashMap::new(),
            leases: HashMap::new(),
            retry_queue,
            parked_workers: VecDeque::new(),
            semaphores,
            cmd_rx,
            config,
            rng,
            next_lease_epoch,
            global_stats,
            shutting_down: false,
            shutdown_deadline: None,
            shutdown_reply: None,
            shutdown_token,
        }
    }

    pub async fn run(mut self) {
        loop {
            self.process_due_retries();
            self.check_lease_timeouts().await;
            self.try_dispatch_parked_workers().await;

            if self.shutting_down && self.leases.is_empty() {
                self.complete_shutdown();
                break;
            }

            if let Some(deadline) = self.shutdown_deadline
                && Instant::now() >= deadline
            {
                self.force_shutdown().await;
                break;
            }

            let next_deadline = self.next_deadline();

            tokio::select! {
                biased;

                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(cmd) => self.handle(cmd).await,
                        None => break,
                    }
                }

                _ = Self::sleep_until_opt(next_deadline) => {
                }
            }
        }

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

        if let Some(sd) = self.shutdown_deadline {
            deadline = Some(deadline.map_or(sd, |d| d.min(sd)));
        }

        deadline
    }

    async fn handle(&mut self, cmd: Command) {
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
                    self.do_submit(queue, spec, permit).await
                };
                let _ = reply.send(result);
            }
            Command::Status { id, reply } => {
                let result = self.do_status(id);
                let _ = reply.send(result);
            }
            Command::Cancel { id, reply } => {
                let result = self.do_cancel(id).await;
                let _ = reply.send(result);
            }
            Command::Stats { reply } => {
                let stats = self.do_stats();
                let _ = reply.send(stats);
            }
            Command::FetchWork { reply } => {
                self.do_fetch_work(reply).await;
            }
            Command::WorkerOutcome {
                id,
                lease_id,
                outcome,
            } => {
                self.do_worker_outcome(id, lease_id, outcome).await;
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

        for sem in self.semaphores.values() {
            sem.close();
        }

        while let Some(worker) = self.parked_workers.pop_front() {
            let _ = worker.send(None);
        }

        if self.leases.is_empty() {
            let _ = reply.send(());
            return;
        }

        self.shutdown_deadline = Some(Instant::now() + self.config.shutdown_timeout);
        self.shutdown_reply = Some(reply);
    }

    fn complete_shutdown(&mut self) {
        if let Some(reply) = self.shutdown_reply.take() {
            let _ = reply.send(());
        }
    }

    async fn force_shutdown(&mut self) {
        let job_ids: Vec<_> = self.leases.keys().copied().collect();
        for job_id in job_ids {
            if let Some(lease) = self.leases.remove(&job_id) {
                lease.cancellation.cancel();
            }
        }
        self.complete_shutdown();
    }

    /// SUBMIT ordering: validate -> build record -> persist -> apply -> expose.
    async fn do_submit(
        &mut self,
        queue: QueueName,
        spec: JobSpec,
        permit: OwnedSemaphorePermit,
    ) -> Result<JobRecord> {
        // 1. Validate queue exists and payload size.
        let qs = self
            .queues
            .get_mut(queue.as_str())
            .ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;

        if spec.payload.len() > MAX_PAYLOAD_SIZE {
            return Err(Error::PayloadTooLarge(spec.payload.len(), MAX_PAYLOAD_SIZE));
        }

        // 2. Build WAL record.
        let id = JobId::new();
        let created_at = UnixMillis::now();
        let wal_record = WalRecord::JobSubmitted {
            id,
            queue: queue.clone(),
            spec: spec.clone(),
            created_at,
        };

        // 3. Persist (WAL append + sync).
        self.storage.persist(wal_record.clone()).await?;

        // 4. Apply to memory.
        self.store.apply_record(&wal_record)?;
        let record = self.store.get(id).cloned().unwrap();

        // 5. Expose (update stats, queue, hold permit).
        self.permits.insert(id, permit);
        qs.ready.push_back(id);
        qs.stats.submitted += 1;
        qs.stats.queued += 1;
        self.global_stats.submitted += 1;

        self.try_dispatch_parked_workers().await;

        Ok(record)
    }

    fn do_status(&self, id: JobId) -> Result<JobRecord> {
        self.store
            .get(id)
            .cloned()
            .ok_or_else(|| Error::JobNotFound(id.to_string()))
    }

    /// CANCEL ordering: validate -> build record -> persist -> apply -> expose.
    async fn do_cancel(&mut self, id: JobId) -> Result<JobRecord> {
        // 1. Validate job exists and can be cancelled.
        let job = self
            .store
            .get(id)
            .ok_or_else(|| Error::JobNotFound(id.to_string()))?;

        let from_state = job.state;
        let queue_name = job.queue.clone();

        if from_state.is_terminal() {
            return Err(Error::InvalidTransition {
                from: from_state,
                to: JobState::Cancelled,
            });
        }

        // 2. Build WAL record.
        let cancelled_at = UnixMillis::now();
        let wal_record = WalRecord::JobCancelled { id, cancelled_at };

        // 3. Persist.
        self.storage.persist(wal_record.clone()).await?;

        // 4. Apply to memory.
        self.store.apply_record(&wal_record)?;
        let record = self.store.get(id).cloned().unwrap();

        // 5. Expose (update stats, release resources).
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

    async fn do_fetch_work(&mut self, reply: oneshot::Sender<Option<LeasedJob>>) {
        if self.shutting_down {
            let _ = reply.send(None);
            return;
        }

        if let Some(job) = self.try_lease_job().await {
            let _ = reply.send(Some(job));
            return;
        }

        self.parked_workers.push_back(reply);
    }

    /// LEASE ordering: choose job -> build record -> persist -> apply -> expose.
    async fn try_lease_job(&mut self) -> Option<LeasedJob> {
        let n = self.queue_order.len();
        if n == 0 {
            return None;
        }

        for _ in 0..n {
            let queue_name = self.queue_order[self.next_queue_idx].clone();
            self.next_queue_idx = (self.next_queue_idx + 1) % n;

            let job_id = match self.queues.get_mut(&queue_name) {
                Some(qs) => match qs.ready.front().copied() {
                    Some(id) => id,
                    None => continue,
                },
                None => continue,
            };

            // Check job is still valid for leasing.
            let job = match self.store.get(job_id) {
                Some(j) if j.state == JobState::Queued => j,
                _ => continue,
            };

            // Build lease parameters.
            let attempt = job.attempts + 1;
            let lease_id = LeaseId::new(self.next_lease_epoch);
            let leased_at = UnixMillis::now();

            // Build WAL record.
            let wal_record = WalRecord::JobLeased {
                id: job_id,
                lease_id,
                attempt,
                leased_at,
            };

            // Persist.
            if let Err(e) = self.storage.persist(wal_record.clone()).await {
                // Persist failed - job stays Queued, not exposed to worker.
                log_persist_error("lease", &e);
                continue;
            }

            // Now commit: remove from ready queue.
            if let Some(qs) = self.queues.get_mut(&queue_name) {
                qs.ready.pop_front();
            }

            // Apply to memory.
            if self.store.apply_record(&wal_record).is_err() {
                continue;
            }
            self.next_lease_epoch += 1;

            // Setup lease tracking.
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

            if let Some(qs) = self.queues.get_mut(&queue_name) {
                qs.stats.queued = qs.stats.queued.saturating_sub(1);
                qs.stats.running += 1;
            }

            let job = self.store.get(job_id)?;
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
        None
    }

    async fn try_dispatch_parked_workers(&mut self) {
        if self.shutting_down {
            return;
        }

        while !self.parked_workers.is_empty() {
            let Some(job) = self.try_lease_job().await else {
                break;
            };
            if let Some(worker) = self.parked_workers.pop_front()
                && worker.send(Some(job)).is_err()
            {
                continue;
            }
        }
    }

    async fn do_worker_outcome(&mut self, id: JobId, lease_id: LeaseId, outcome: WorkerOutcome) {
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
                self.complete_job(id, lease_id, &queue_name).await;
            }
            WorkerOutcome::Fatal => {
                self.dead_job(id, lease_id, &queue_name).await;
            }
            WorkerOutcome::Retryable | WorkerOutcome::Panic => {
                if attempts >= max_attempts {
                    self.dead_job(id, lease_id, &queue_name).await;
                } else {
                    self.schedule_retry(id, lease_id, &queue_name, attempts)
                        .await;
                }
            }
        }
    }

    /// COMPLETION ordering: build record -> persist -> apply -> release capacity.
    async fn complete_job(&mut self, id: JobId, lease_id: LeaseId, queue_name: &QueueName) {
        let completed_at = UnixMillis::now();
        let wal_record = WalRecord::JobCompleted {
            id,
            lease_id,
            completed_at,
        };

        if let Err(e) = self.storage.persist(wal_record.clone()).await {
            log_persist_error("complete", &e);
            return;
        }

        if self.store.apply_record(&wal_record).is_err() {
            return;
        }

        if let Some(qs) = self.queues.get_mut(queue_name.as_str()) {
            qs.stats.running = qs.stats.running.saturating_sub(1);
            qs.stats.completed += 1;
        }
        self.permits.remove(&id);
        self.global_stats.completed += 1;
    }

    /// DEAD ordering: build record -> persist -> apply -> release capacity.
    async fn dead_job(&mut self, id: JobId, lease_id: LeaseId, queue_name: &QueueName) {
        let dead_at = UnixMillis::now();
        let wal_record = WalRecord::JobDead {
            id,
            lease_id,
            dead_at,
        };

        if let Err(e) = self.storage.persist(wal_record.clone()).await {
            log_persist_error("dead", &e);
            return;
        }

        if self.store.apply_record(&wal_record).is_err() {
            return;
        }

        if let Some(qs) = self.queues.get_mut(queue_name.as_str()) {
            qs.stats.running = qs.stats.running.saturating_sub(1);
            qs.stats.dead += 1;
        }
        self.permits.remove(&id);
        self.global_stats.dead += 1;
    }

    /// RETRY ordering: calculate delay -> build record -> persist -> apply -> set timer.
    async fn schedule_retry(
        &mut self,
        id: JobId,
        lease_id: LeaseId,
        queue_name: &QueueName,
        attempt: u32,
    ) {
        // Calculate delay and wall-clock available_at before persist.
        let delay = self.config.retry.delay_with_rng(attempt, &mut self.rng);
        let wall_clock_available_at =
            UnixMillis::from_millis(UnixMillis::now().as_millis() + delay.as_millis() as i64);

        let wal_record = WalRecord::JobRetryScheduled {
            id,
            lease_id,
            attempt,
            available_at: wall_clock_available_at,
        };

        if let Err(e) = self.storage.persist(wal_record.clone()).await {
            log_persist_error("retry", &e);
            return;
        }

        if self.store.apply_record(&wal_record).is_err() {
            return;
        }

        if let Some(qs) = self.queues.get_mut(queue_name.as_str()) {
            qs.stats.running = qs.stats.running.saturating_sub(1);
            qs.stats.retrying += 1;
        }

        // Derive monotonic timer from persisted wall-clock time.
        let available_at = Instant::now() + delay;

        self.retry_queue.push(RetryEntry {
            available_at,
            wall_clock_available_at,
            job_id: id,
        });

        self.global_stats.retried += 1;
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

            if self.store.transition_to_queued(job_id).is_ok()
                && let Some(qs) = self.queues.get_mut(queue_name.as_str())
            {
                qs.stats.retrying = qs.stats.retrying.saturating_sub(1);
                qs.stats.queued += 1;
                qs.ready.push_back(job_id);
            }
        }
    }

    async fn check_lease_timeouts(&mut self) {
        let now = Instant::now();
        let mut timed_out = Vec::new();

        for (&job_id, lease) in &self.leases {
            if lease.deadline <= now {
                timed_out.push((job_id, lease.lease_id));
            }
        }

        for (job_id, lease_id) in timed_out {
            if let Some(lease) = self.leases.remove(&job_id) {
                lease.cancellation.cancel();

                let Some(job) = self.store.get(job_id) else {
                    continue;
                };

                let queue_name = job.queue.clone();
                let attempts = job.attempts;
                let max_attempts = job.spec.max_attempts;

                if attempts >= max_attempts {
                    self.dead_job(job_id, lease_id, &queue_name).await;
                } else {
                    self.schedule_retry(job_id, lease_id, &queue_name, attempts)
                        .await;
                }
            }
        }
    }
}

fn log_persist_error(operation: &str, err: &impl std::fmt::Debug) {
    tracing::error!(operation = operation, error = ?err, "WAL persist failed");
}
