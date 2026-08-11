use crate::error::{Error, Result};
use crate::types::{JobId, JobRecord, JobSpec, JobState, QueueName};
use std::collections::HashMap;

/// In-memory job store. Provides the boundary for future persistence.
pub struct MemoryStore {
    jobs: HashMap<JobId, JobRecord>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: JobId, queue: QueueName, spec: JobSpec) -> JobRecord {
        let record = JobRecord::new(id, queue, spec);
        self.jobs.insert(id, record.clone());
        record
    }

    pub fn get(&self, id: JobId) -> Option<&JobRecord> {
        self.jobs.get(&id)
    }

    pub fn get_mut(&mut self, id: JobId) -> Option<&mut JobRecord> {
        self.jobs.get_mut(&id)
    }

    pub fn transition(&mut self, id: JobId, to: JobState) -> Result<JobRecord> {
        let record = self
            .jobs
            .get_mut(&id)
            .ok_or_else(|| Error::JobNotFound(id.to_string()))?;

        let from = record.state;
        if !Self::is_valid_transition(from, to) {
            return Err(Error::InvalidTransition { from, to });
        }

        record.state = to;
        Ok(record.clone())
    }

    fn is_valid_transition(from: JobState, to: JobState) -> bool {
        use JobState::*;
        matches!(
            (from, to),
            (Queued, Running)
                | (Queued, Cancelled)
                | (Running, Completed)
                | (Running, RetryWaiting)
                | (Running, Dead)
                | (Running, Cancelled)
                | (RetryWaiting, Queued)
                | (RetryWaiting, Dead)
                | (RetryWaiting, Cancelled)
        )
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}
