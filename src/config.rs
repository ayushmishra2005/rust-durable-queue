use crate::error::{Error, Result};
use crate::types::QueueName;
use rand::Rng;
use std::collections::HashSet;
use std::time::Duration;

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

/// Jitter mode for retry delays.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Jitter {
    #[default]
    None,
    Full,
}

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub jitter: Jitter,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(300),
            jitter: Jitter::None,
        }
    }
}

impl RetryConfig {
    pub fn new(base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            base_delay,
            max_delay,
            jitter: Jitter::None,
        }
    }

    pub fn with_jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Calculate the capped backoff delay (before jitter) for a given attempt.
    pub fn cap_for_attempt(&self, attempt: u32) -> Duration {
        let exp = attempt.saturating_sub(1).min(30);
        let multiplier = 1u64.checked_shl(exp).unwrap_or(u64::MAX);
        let delay_ms = self.base_delay.as_millis() as u64;
        let backoff_ms = delay_ms.saturating_mul(multiplier);
        let capped_ms = backoff_ms.min(self.max_delay.as_millis() as u64);
        Duration::from_millis(capped_ms)
    }

    /// Calculate retry delay with jitter using provided RNG.
    pub fn delay_with_rng(&self, attempt: u32, rng: &mut impl Rng) -> Duration {
        let cap = self.cap_for_attempt(attempt);
        match self.jitter {
            Jitter::None => cap,
            Jitter::Full => {
                let cap_ms = cap.as_millis() as u64;
                if cap_ms == 0 {
                    return Duration::ZERO;
                }
                let jittered_ms = rng.random_range(0..=cap_ms);
                Duration::from_millis(jittered_ms)
            }
        }
    }
}

/// Runtime configuration for the queue system.
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub queues: Vec<QueueConfig>,
    pub channel_capacity: usize,
    pub worker_concurrency: usize,
    pub visibility_timeout: Duration,
    pub retry: RetryConfig,
    pub shutdown_timeout: Duration,
}

impl RuntimeConfig {
    pub fn new(queues: Vec<QueueConfig>, channel_capacity: usize) -> Self {
        Self {
            queues,
            channel_capacity,
            worker_concurrency: 4,
            visibility_timeout: Duration::from_secs(30),
            retry: RetryConfig::default(),
            shutdown_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_worker_concurrency(mut self, n: usize) -> Self {
        self.worker_concurrency = n;
        self
    }

    pub fn with_visibility_timeout(mut self, timeout: Duration) -> Self {
        self.visibility_timeout = timeout;
        self
    }

    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }

    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.channel_capacity == 0 {
            return Err(Error::InvalidConfiguration(
                "channel capacity must be positive".into(),
            ));
        }

        if self.worker_concurrency == 0 {
            return Err(Error::InvalidConfiguration(
                "worker concurrency must be positive".into(),
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
