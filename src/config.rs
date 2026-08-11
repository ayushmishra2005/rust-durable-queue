use crate::error::{Error, Result};
use crate::store::StorageConfig;
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

    /// Validate retry configuration.
    pub fn validate(&self) -> Result<()> {
        if self.base_delay.is_zero() {
            return Err(Error::InvalidConfiguration(
                "retry base_delay must be positive".into(),
            ));
        }
        if self.max_delay.is_zero() {
            return Err(Error::InvalidConfiguration(
                "retry max_delay must be positive".into(),
            ));
        }
        if self.base_delay > self.max_delay {
            return Err(Error::InvalidConfiguration(
                "retry base_delay must not exceed max_delay".into(),
            ));
        }
        Ok(())
    }

    /// Calculate the capped backoff delay (before jitter) for a given attempt.
    /// Uses saturating arithmetic to avoid overflow.
    pub fn cap_for_attempt(&self, attempt: u32) -> Duration {
        // Exponent is (attempt - 1) capped at 63 to avoid shift overflow.
        let exp = attempt.saturating_sub(1).min(63);

        // Use Duration's checked_mul to safely compute base_delay * 2^exp.
        let mut backoff = self.base_delay;
        for _ in 0..exp {
            backoff = backoff.saturating_mul(2);
            // Early exit if we've hit the cap.
            if backoff >= self.max_delay {
                return self.max_delay;
            }
        }

        backoff.min(self.max_delay)
    }

    /// Calculate retry delay with jitter using provided RNG.
    pub fn delay_with_rng(&self, attempt: u32, rng: &mut impl Rng) -> Duration {
        let cap = self.cap_for_attempt(attempt);
        match self.jitter {
            Jitter::None => cap,
            Jitter::Full => {
                // Use nanos for precision, saturate on overflow.
                let cap_nanos = cap.as_nanos();
                if cap_nanos == 0 {
                    return Duration::ZERO;
                }
                // Safe: cap_nanos fits in u128, we sample in [0, cap_nanos].
                let jittered_nanos = if cap_nanos <= u64::MAX as u128 {
                    rng.random_range(0..=cap_nanos as u64) as u128
                } else {
                    // Very large durations: sample u64 and scale.
                    let scale = cap_nanos / u64::MAX as u128;
                    let base = rng.random_range(0..=u64::MAX) as u128;
                    (base * scale).min(cap_nanos)
                };
                Duration::from_nanos(jittered_nanos.min(u64::MAX as u128) as u64)
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
    pub storage: StorageConfig,
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
            storage: StorageConfig::default(),
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

    pub fn with_storage(mut self, storage: StorageConfig) -> Self {
        self.storage = storage;
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

        if self.visibility_timeout.is_zero() {
            return Err(Error::InvalidConfiguration(
                "visibility_timeout must be positive".into(),
            ));
        }

        if self.shutdown_timeout.is_zero() {
            return Err(Error::InvalidConfiguration(
                "shutdown_timeout must be positive".into(),
            ));
        }

        self.retry.validate()?;

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
