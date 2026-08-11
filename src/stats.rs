use std::collections::HashMap;

/// Per-queue statistics.
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    pub submitted: u64,
    pub queued: u64,
    pub running: u64,
    pub retrying: u64,
    pub completed: u64,
    pub dead: u64,
    pub cancelled: u64,
}

/// Snapshot of queue system statistics.
#[derive(Debug, Clone, Default)]
pub struct StatsSnapshot {
    pub submitted: u64,
    pub queued: u64,
    pub running: u64,
    pub retrying: u64,
    pub completed: u64,
    pub dead: u64,
    pub cancelled: u64,
    pub retried: u64,
    pub stale_outcomes: u64,
    pub per_queue: HashMap<String, QueueStats>,
}
