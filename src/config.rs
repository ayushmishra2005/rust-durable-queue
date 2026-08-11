use crate::error::{Error, Result};
use crate::types::QueueName;
use std::collections::HashSet;

/// Configuration for a single queue.
#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub name: QueueName,
    pub capacity: usize,
}

impl QueueConfig {
    pub fn new(name: QueueName, capacity: usize) -> Self {
        Self { name, capacity }
    }
}

/// Runtime configuration for the queue system.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub queues: Vec<QueueConfig>,
    pub channel_capacity: usize,
}

impl RuntimeConfig {
    pub fn new(queues: Vec<QueueConfig>, channel_capacity: usize) -> Self {
        Self {
            queues,
            channel_capacity,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.channel_capacity == 0 {
            return Err(Error::InvalidConfiguration(
                "channel capacity must be positive".into(),
            ));
        }

        let mut seen = HashSet::new();
        for queue in &self.queues {
            if queue.capacity == 0 {
                return Err(Error::InvalidConfiguration(format!(
                    "queue '{}' has zero capacity",
                    queue.name
                )));
            }
            if !seen.insert(queue.name.as_str()) {
                return Err(Error::InvalidConfiguration(format!(
                    "duplicate queue name: '{}'",
                    queue.name
                )));
            }
        }

        Ok(())
    }
}
