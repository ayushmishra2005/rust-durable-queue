use crate::types::{JobId, JobSpec, LeaseId, QueueName, UnixMillis};
use serde::{Deserialize, Serialize};

/// WAL record types for durable state transitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalRecord {
    JobSubmitted {
        id: JobId,
        queue: QueueName,
        spec: JobSpec,
        created_at: UnixMillis,
    },
    JobLeased {
        id: JobId,
        lease_id: LeaseId,
        attempt: u32,
        leased_at: UnixMillis,
    },
    JobRetryScheduled {
        id: JobId,
        lease_id: LeaseId,
        attempt: u32,
        available_at: UnixMillis,
    },
    JobCompleted {
        id: JobId,
        lease_id: LeaseId,
        completed_at: UnixMillis,
    },
    JobDead {
        id: JobId,
        lease_id: LeaseId,
        dead_at: UnixMillis,
    },
    JobCancelled {
        id: JobId,
        cancelled_at: UnixMillis,
    },
}

impl WalRecord {
    pub fn job_id(&self) -> JobId {
        match self {
            WalRecord::JobSubmitted { id, .. } => *id,
            WalRecord::JobLeased { id, .. } => *id,
            WalRecord::JobRetryScheduled { id, .. } => *id,
            WalRecord::JobCompleted { id, .. } => *id,
            WalRecord::JobDead { id, .. } => *id,
            WalRecord::JobCancelled { id, .. } => *id,
        }
    }

    pub fn record_type(&self) -> u8 {
        match self {
            WalRecord::JobSubmitted { .. } => 1,
            WalRecord::JobLeased { .. } => 2,
            WalRecord::JobRetryScheduled { .. } => 3,
            WalRecord::JobCompleted { .. } => 4,
            WalRecord::JobDead { .. } => 5,
            WalRecord::JobCancelled { .. } => 6,
        }
    }
}
