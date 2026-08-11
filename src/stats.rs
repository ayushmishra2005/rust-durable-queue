use std::collections::HashMap;

/// Per-queue statistics.
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    pub submitted: u64,
    pub queued: u64,
    pub cancelled: u64,
}

/// Snapshot of queue system statistics.
#[derive(Debug, Clone, Default)]
pub struct StatsSnapshot {
    pub submitted: u64,
    pub queued: u64,
    pub cancelled: u64,
    pub per_queue: HashMap<String, QueueStats>,
}
