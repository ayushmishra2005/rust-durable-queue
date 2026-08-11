//! WAL recovery and crash reconciliation.

use crate::config::RuntimeConfig;
use crate::store::MemoryStore;
use crate::types::{JobId, JobRecord, JobState, LeaseId, QueueName, UnixMillis};
use crate::wal::{
    WalError, WalFrame, WalHeader, WalRecord, WalResult, repair_truncated_tail, scan_wal,
};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

const WAL_FILE: &str = "wal.log";
const LOCK_FILE: &str = "LOCK";

/// State recovered from WAL replay.
pub struct RecoveredState {
    /// In-memory job store.
    pub store: MemoryStore,
    /// Jobs in Queued state per queue (in submission order).
    pub queued_jobs: HashMap<String, Vec<JobId>>,
    /// Jobs in RetryWaiting state with their wall-clock available_at.
    pub retry_jobs: Vec<(JobId, UnixMillis)>,
    /// Next sequence number for WAL writer.
    pub next_sequence: u64,
    /// Next lease epoch.
    pub next_lease_epoch: u64,
    /// Stats: submitted count.
    pub submitted: u64,
    /// Stats: completed count.
    pub completed: u64,
    /// Stats: dead count.
    pub dead: u64,
    /// Stats: cancelled count.
    pub cancelled: u64,
    /// Stats: retried count (number of retry records).
    pub retried: u64,
}

/// Tracking data for recovery.
struct RecoveryTracker {
    /// Jobs that have been submitted.
    submitted_jobs: HashMap<JobId, (QueueName, u32)>, // (queue, max_attempts)
    /// Jobs currently in Running state (need reconciliation if crash).
    running_jobs: HashMap<JobId, (LeaseId, u32)>, // (lease_id, attempts)
    /// Jobs in RetryWaiting with their available_at.
    retry_waiting: HashMap<JobId, UnixMillis>,
    /// Queued jobs in submission order per queue.
    queued_order: HashMap<String, Vec<JobId>>,
    /// Max lease epoch seen.
    max_lease_epoch: u64,
}

impl RecoveryTracker {
    fn new() -> Self {
        Self {
            submitted_jobs: HashMap::new(),
            running_jobs: HashMap::new(),
            retry_waiting: HashMap::new(),
            queued_order: HashMap::new(),
            max_lease_epoch: 0,
        }
    }

    fn track_record(&mut self, record: &WalRecord) {
        match record {
            WalRecord::JobSubmitted {
                id, queue, spec, ..
            } => {
                self.submitted_jobs
                    .insert(*id, (queue.clone(), spec.max_attempts));
                self.queued_order
                    .entry(queue.as_str().to_string())
                    .or_default()
                    .push(*id);
            }
            WalRecord::JobLeased {
                id,
                lease_id,
                attempt,
                ..
            } => {
                self.max_lease_epoch = self.max_lease_epoch.max(lease_id.epoch());
                self.running_jobs.insert(*id, (*lease_id, *attempt));
                // Remove from queued.
                if let Some((queue, _)) = self.submitted_jobs.get(id)
                    && let Some(q) = self.queued_order.get_mut(queue.as_str())
                {
                    q.retain(|&jid| jid != *id);
                }
                self.retry_waiting.remove(id);
            }
            WalRecord::JobRetryScheduled {
                id, available_at, ..
            } => {
                self.running_jobs.remove(id);
                self.retry_waiting.insert(*id, *available_at);
            }
            WalRecord::JobCompleted { id, .. }
            | WalRecord::JobDead { id, .. }
            | WalRecord::JobCancelled { id, .. } => {
                self.running_jobs.remove(id);
                self.retry_waiting.remove(id);
                // Remove from queued if somehow still there.
                if let Some((queue, _)) = self.submitted_jobs.get(id)
                    && let Some(q) = self.queued_order.get_mut(queue.as_str())
                {
                    q.retain(|&jid| jid != *id);
                }
            }
        }
    }
}

/// Perform WAL recovery.
///
/// 1. Acquire exclusive lock
/// 2. Scan and validate WAL
/// 3. Repair truncated tail if needed
/// 4. Replay records to rebuild state
/// 5. Reconcile crash-lost Running jobs
/// 6. Validate against config
pub fn recover(path: &Path, config: &RuntimeConfig) -> WalResult<(RecoveredState, File, File)> {
    std::fs::create_dir_all(path)?;

    let lock_path = path.join(LOCK_FILE);
    let wal_path = path.join(WAL_FILE);

    // Acquire exclusive lock.
    let lock_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    fs4::fs_std::FileExt::try_lock_exclusive(&lock_file).map_err(|_| WalError::StoreLocked)?;

    // Open or create WAL file.
    let wal_exists = wal_path.exists();
    let mut wal_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&wal_path)?;

    if !wal_exists || wal_file.metadata()?.len() == 0 {
        // New WAL: write header.
        let header = WalHeader::new();
        wal_file.write_all(&header.encode())?;
        wal_file.sync_data()?;

        return Ok((
            RecoveredState {
                store: MemoryStore::new(),
                queued_jobs: HashMap::new(),
                retry_jobs: Vec::new(),
                next_sequence: 1,
                next_lease_epoch: 1,
                submitted: 0,
                completed: 0,
                dead: 0,
                cancelled: 0,
                retried: 0,
            },
            wal_file,
            lock_file,
        ));
    }

    // Scan WAL.
    let scan_result = scan_wal(&wal_path)?;

    // Repair truncated tail if needed.
    if scan_result.had_truncated_tail {
        repair_truncated_tail(&wal_path, scan_result.last_valid_offset)?;
    }

    // Replay records to build state.
    let mut store = MemoryStore::new();
    let mut tracker = RecoveryTracker::new();
    let mut submitted_count = 0u64;
    let mut completed_count = 0u64;
    let mut dead_count = 0u64;
    let mut cancelled_count = 0u64;
    let mut retried_count = 0u64;

    for record in &scan_result.records {
        // Track for recovery.
        tracker.track_record(record);

        // Count stats.
        match record {
            WalRecord::JobSubmitted { .. } => submitted_count += 1,
            WalRecord::JobCompleted { .. } => completed_count += 1,
            WalRecord::JobDead { .. } => dead_count += 1,
            WalRecord::JobCancelled { .. } => cancelled_count += 1,
            WalRecord::JobRetryScheduled { .. } => retried_count += 1,
            WalRecord::JobLeased { .. } => {}
        }

        // Apply record to store (relaxed validation for replay).
        apply_record_for_recovery(&mut store, record)?;
    }

    // Determine next sequence.
    let mut next_sequence = scan_result
        .last_sequence
        .checked_add(1)
        .ok_or(WalError::SequenceExhausted)?;

    // Determine next lease epoch.
    let next_lease_epoch = tracker
        .max_lease_epoch
        .checked_add(1)
        .ok_or(WalError::LeaseEpochExhausted)?;

    // Reconcile crash-lost Running jobs.
    let reconciliation_records = reconcile_running_jobs(&store, &tracker)?;

    // Reopen file for appending reconciliation records.
    drop(wal_file);
    let mut wal_file = OpenOptions::new().read(true).append(true).open(&wal_path)?;

    // Write reconciliation records.
    for record in &reconciliation_records {
        let frame = WalFrame::new(next_sequence, record.clone());
        wal_file.write_all(&frame.encode()?)?;
        next_sequence = next_sequence
            .checked_add(1)
            .ok_or(WalError::SequenceExhausted)?;
    }
    wal_file.sync_data()?;

    // Apply reconciliation records to store.
    let mut new_tracker = tracker;
    for record in &reconciliation_records {
        new_tracker.track_record(record);
        match record {
            WalRecord::JobRetryScheduled { .. } => retried_count += 1,
            WalRecord::JobDead { .. } => dead_count += 1,
            _ => {}
        }
        apply_record_for_recovery(&mut store, record)?;
    }

    // Build final queued_jobs from store state.
    let mut queued_jobs: HashMap<String, Vec<JobId>> = HashMap::new();
    for (queue_name, job_ids) in &new_tracker.queued_order {
        let mut valid_queued = Vec::new();
        for &job_id in job_ids {
            if let Some(job) = store.get(job_id)
                && job.state == JobState::Queued
            {
                valid_queued.push(job_id);
            }
        }
        if !valid_queued.is_empty() {
            queued_jobs.insert(queue_name.clone(), valid_queued);
        }
    }

    // Build retry_jobs.
    let mut retry_jobs = Vec::new();
    for (&job_id, &available_at) in &new_tracker.retry_waiting {
        if let Some(job) = store.get(job_id)
            && job.state == JobState::RetryWaiting
        {
            retry_jobs.push((job_id, available_at));
        }
    }

    // Validate against config.
    validate_recovered_state(&store, &queued_jobs, &retry_jobs, config)?;

    // Seek to end for appending.
    wal_file.seek(SeekFrom::End(0))?;

    Ok((
        RecoveredState {
            store,
            queued_jobs,
            retry_jobs,
            next_sequence,
            next_lease_epoch,
            submitted: submitted_count,
            completed: completed_count,
            dead: dead_count,
            cancelled: cancelled_count,
            retried: retried_count,
        },
        wal_file,
        lock_file,
    ))
}

/// Apply record during recovery (relaxed state validation).
fn apply_record_for_recovery(store: &mut MemoryStore, record: &WalRecord) -> WalResult<()> {
    match record {
        WalRecord::JobSubmitted {
            id,
            queue,
            spec,
            created_at,
        } => {
            let job = JobRecord::new(*id, queue.clone(), spec.clone(), *created_at);
            store.insert_job(job);
        }
        WalRecord::JobLeased { id, attempt, .. } => {
            if let Some(job) = store.get_mut(*id) {
                job.state = JobState::Running;
                job.attempts = *attempt;
            }
        }
        WalRecord::JobRetryScheduled { id, .. } => {
            if let Some(job) = store.get_mut(*id) {
                job.state = JobState::RetryWaiting;
            }
        }
        WalRecord::JobCompleted { id, .. } => {
            if let Some(job) = store.get_mut(*id) {
                job.state = JobState::Completed;
            }
        }
        WalRecord::JobDead { id, .. } => {
            if let Some(job) = store.get_mut(*id) {
                job.state = JobState::Dead;
            }
        }
        WalRecord::JobCancelled { id, .. } => {
            if let Some(job) = store.get_mut(*id) {
                job.state = JobState::Cancelled;
            }
        }
    }
    Ok(())
}

/// Generate reconciliation records for crash-lost Running jobs.
fn reconcile_running_jobs(
    store: &MemoryStore,
    tracker: &RecoveryTracker,
) -> WalResult<Vec<WalRecord>> {
    let mut records = Vec::new();
    let now = UnixMillis::now();

    for (&job_id, &(lease_id, _attempts)) in &tracker.running_jobs {
        let Some(job) = store.get(job_id) else {
            continue;
        };

        if job.state != JobState::Running {
            continue;
        }

        // Job was Running when crash occurred - reconcile it.
        if job.attempts < job.spec.max_attempts {
            // Schedule immediate retry.
            records.push(WalRecord::JobRetryScheduled {
                id: job_id,
                lease_id,
                attempt: job.attempts,
                available_at: now,
            });
        } else {
            // Exhausted - mark dead.
            records.push(WalRecord::JobDead {
                id: job_id,
                lease_id,
                dead_at: now,
            });
        }
    }

    Ok(records)
}

/// Validate recovered state against current config.
fn validate_recovered_state(
    store: &MemoryStore,
    queued_jobs: &HashMap<String, Vec<JobId>>,
    retry_jobs: &[(JobId, UnixMillis)],
    config: &RuntimeConfig,
) -> WalResult<()> {
    // Build config queue map.
    let config_queues: HashMap<&str, usize> = config
        .queues
        .iter()
        .map(|q| (q.name.as_str(), q.capacity))
        .collect();

    // Count live jobs per queue.
    let mut live_per_queue: HashMap<String, usize> = HashMap::new();

    // Check queued jobs.
    for (queue_name, job_ids) in queued_jobs {
        if !config_queues.contains_key(queue_name.as_str()) {
            return Err(WalError::RecoveryQueueNotFound(queue_name.clone()));
        }
        *live_per_queue.entry(queue_name.clone()).or_default() += job_ids.len();
    }

    // Check retry jobs.
    for (job_id, _) in retry_jobs {
        if let Some(job) = store.get(*job_id) {
            let queue_name = job.queue.as_str().to_string();
            if !config_queues.contains_key(queue_name.as_str()) {
                return Err(WalError::RecoveryQueueNotFound(queue_name));
            }
            *live_per_queue.entry(queue_name).or_default() += 1;
        }
    }

    // Validate capacity.
    for (queue_name, count) in &live_per_queue {
        if let Some(&capacity) = config_queues.get(queue_name.as_str())
            && *count > capacity
        {
            return Err(WalError::RecoveryCapacityExceeded {
                queue: queue_name.clone(),
                recovered: *count,
                capacity,
            });
        }
    }

    Ok(())
}
