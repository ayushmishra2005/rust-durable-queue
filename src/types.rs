use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Unix timestamp in milliseconds for persistent wall-clock time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct UnixMillis(i64);

impl UnixMillis {
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Self(duration.as_millis() as i64)
    }

    pub fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    pub fn as_millis(&self) -> i64 {
        self.0
    }
}

impl Default for UnixMillis {
    fn default() -> Self {
        Self::now()
    }
}

/// Unique identifier for a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JobId(Uuid);

impl JobId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for JobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique lease identity for fencing stale worker outcomes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LeaseId(u64);

impl LeaseId {
    pub fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    pub fn epoch(&self) -> u64 {
        self.0
    }
}

/// Name identifying a queue.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueueName(String);

impl QueueName {
    pub fn new(name: impl Into<String>) -> Option<Self> {
        let name = name.into();
        if name.is_empty() {
            None
        } else {
            Some(Self(name))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for QueueName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Current state of a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Queued,
    Running,
    RetryWaiting,
    Completed,
    Dead,
    Cancelled,
}

impl JobState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Dead | Self::Cancelled)
    }
}

/// Maximum payload size (1MB).
pub const MAX_PAYLOAD_SIZE: usize = 1024 * 1024;

/// Job specification provided at submission time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobSpec {
    pub payload: Vec<u8>,
    pub max_attempts: u32,
}

impl JobSpec {
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            payload: payload.into(),
            max_attempts: 3,
        }
    }

    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }
}

/// Complete record of a job including immutable spec and mutable state.
#[derive(Debug, Clone)]
pub struct JobRecord {
    pub id: JobId,
    pub queue: QueueName,
    pub spec: JobSpec,
    pub state: JobState,
    pub attempts: u32,
    pub created_at: UnixMillis,
}

impl JobRecord {
    pub fn new(id: JobId, queue: QueueName, spec: JobSpec, created_at: UnixMillis) -> Self {
        Self {
            id,
            queue,
            spec,
            state: JobState::Queued,
            attempts: 0,
            created_at,
        }
    }
}
