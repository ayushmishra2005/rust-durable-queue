use crate::error::{Error, Result};
use crate::types::{JobId, JobRecord, JobState};
use crate::wal::{WalRecord, WalWriter};
use std::collections::HashMap;

/// In-memory job store.
pub struct MemoryStore {
    jobs: HashMap<JobId, JobRecord>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
        }
    }

    pub fn get(&self, id: JobId) -> Option<&JobRecord> {
        self.jobs.get(&id)
    }

    #[allow(dead_code)] // For future replay.
    pub fn get_mut(&mut self, id: JobId) -> Option<&mut JobRecord> {
        self.jobs.get_mut(&id)
    }

    /// Apply a WAL record to in-memory state.
    /// This is the shared apply function for both live execution and future replay.
    pub fn apply_record(&mut self, record: &WalRecord) -> Result<()> {
        match record {
            WalRecord::JobSubmitted {
                id,
                queue,
                spec,
                created_at,
            } => {
                let job = JobRecord::new(*id, queue.clone(), spec.clone(), *created_at);
                self.jobs.insert(*id, job);
            }
            WalRecord::JobLeased { id, attempt, .. } => {
                let job = self
                    .jobs
                    .get_mut(id)
                    .ok_or_else(|| Error::JobNotFound(id.to_string()))?;

                if !Self::is_valid_transition(job.state, JobState::Running) {
                    return Err(Error::InvalidTransition {
                        from: job.state,
                        to: JobState::Running,
                    });
                }

                job.state = JobState::Running;
                job.attempts = *attempt;
            }
            WalRecord::JobRetryScheduled { id, .. } => {
                let job = self
                    .jobs
                    .get_mut(id)
                    .ok_or_else(|| Error::JobNotFound(id.to_string()))?;

                if !Self::is_valid_transition(job.state, JobState::RetryWaiting) {
                    return Err(Error::InvalidTransition {
                        from: job.state,
                        to: JobState::RetryWaiting,
                    });
                }

                job.state = JobState::RetryWaiting;
            }
            WalRecord::JobCompleted { id, .. } => {
                let job = self
                    .jobs
                    .get_mut(id)
                    .ok_or_else(|| Error::JobNotFound(id.to_string()))?;

                if !Self::is_valid_transition(job.state, JobState::Completed) {
                    return Err(Error::InvalidTransition {
                        from: job.state,
                        to: JobState::Completed,
                    });
                }

                job.state = JobState::Completed;
            }
            WalRecord::JobDead { id, .. } => {
                let job = self
                    .jobs
                    .get_mut(id)
                    .ok_or_else(|| Error::JobNotFound(id.to_string()))?;

                if !Self::is_valid_transition(job.state, JobState::Dead) {
                    return Err(Error::InvalidTransition {
                        from: job.state,
                        to: JobState::Dead,
                    });
                }

                job.state = JobState::Dead;
            }
            WalRecord::JobCancelled { id, .. } => {
                let job = self
                    .jobs
                    .get_mut(id)
                    .ok_or_else(|| Error::JobNotFound(id.to_string()))?;

                if !Self::is_valid_transition(job.state, JobState::Cancelled) {
                    return Err(Error::InvalidTransition {
                        from: job.state,
                        to: JobState::Cancelled,
                    });
                }

                job.state = JobState::Cancelled;
            }
        }
        Ok(())
    }

    /// Transition RetryWaiting -> Queued for due retries.
    pub fn transition_to_queued(&mut self, id: JobId) -> Result<JobRecord> {
        let job = self
            .jobs
            .get_mut(&id)
            .ok_or_else(|| Error::JobNotFound(id.to_string()))?;

        if !Self::is_valid_transition(job.state, JobState::Queued) {
            return Err(Error::InvalidTransition {
                from: job.state,
                to: JobState::Queued,
            });
        }

        job.state = JobState::Queued;
        Ok(job.clone())
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

/// Storage configuration.
#[derive(Debug, Clone, Default)]
pub enum StorageConfig {
    #[default]
    Memory,
    Wal {
        path: std::path::PathBuf,
    },
}

/// Storage backend abstraction.
pub enum Storage {
    Memory,
    Wal(WalWriter),
}

impl Storage {
    pub fn open(config: &StorageConfig) -> crate::wal::WalResult<Self> {
        match config {
            StorageConfig::Memory => Ok(Self::Memory),
            StorageConfig::Wal { path } => Ok(Self::Wal(WalWriter::open(path)?)),
        }
    }

    #[allow(dead_code)] // For future use.
    pub fn is_durable(&self) -> bool {
        matches!(self, Self::Wal(_))
    }

    /// Persist a record. Returns sequence number for durable storage.
    pub async fn persist(&self, record: WalRecord) -> crate::wal::WalResult<Option<u64>> {
        match self {
            Storage::Memory => Ok(None),
            Storage::Wal(writer) => {
                let seq = writer.append(record).await?;
                Ok(Some(seq))
            }
        }
    }
}

/// Testable storage with fault injection.
#[allow(dead_code)] // For future fault injection tests.
pub enum TestableStorage {
    Memory,
    Wal(crate::wal::TestableWalWriter),
}

#[allow(dead_code)] // For future fault injection tests.
impl TestableStorage {
    pub fn open(config: &StorageConfig) -> crate::wal::WalResult<Self> {
        match config {
            StorageConfig::Memory => Ok(Self::Memory),
            StorageConfig::Wal { path } => {
                Ok(Self::Wal(crate::wal::TestableWalWriter::open(path)?))
            }
        }
    }

    pub fn is_durable(&self) -> bool {
        matches!(self, Self::Wal(_))
    }

    pub async fn persist(&self, record: WalRecord) -> crate::wal::WalResult<Option<u64>> {
        match self {
            TestableStorage::Memory => Ok(None),
            TestableStorage::Wal(writer) => {
                let seq = writer.append(record).await?;
                Ok(Some(seq))
            }
        }
    }

    pub fn fail_next_append(&self) {
        if let TestableStorage::Wal(writer) = self {
            writer.fail_next_append();
        }
    }

    pub fn fail_next_sync(&self) {
        if let TestableStorage::Wal(writer) = self {
            writer.fail_next_sync();
        }
    }
}
