//! WAL durability tests.
//!
//! These tests do NOT use paused time since the blocking WAL writer thread
//! doesn't participate in Tokio's time mock.

use rust_durable_queue::{
    Error, JobContext, JobError, JobSpec, JobState, QueueConfig, QueueName, RetryConfig, Runtime,
    RuntimeConfig, StorageConfig,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tempfile::TempDir;

fn queue_name(s: &str) -> QueueName {
    QueueName::new(s).unwrap()
}

fn wal_config(dir: &TempDir, name: &str, capacity: usize) -> RuntimeConfig {
    RuntimeConfig::new(vec![QueueConfig::new(queue_name(name), capacity)], 64).with_storage(
        StorageConfig::Wal {
            path: dir.path().to_path_buf(),
        },
    )
}

// ============================================================
// SUBMIT DURABILITY TESTS
// ============================================================

#[tokio::test]
async fn wal_submit_creates_job() {
    let dir = TempDir::new().unwrap();
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = wal_config(&dir, "q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    assert_eq!(job.state, JobState::Queued);
    assert!(dir.path().join("wal.log").exists());

    runtime.shutdown().await;
}

#[tokio::test]
async fn wal_submit_visible_after_persist() {
    let dir = TempDir::new().unwrap();
    let executed = Arc::new(AtomicU32::new(0));
    let executed2 = executed.clone();

    let handler = move |_ctx: JobContext| {
        let e = executed2.clone();
        async move {
            e.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = wal_config(&dir, "q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    // Wait for job to complete.
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(executed.load(Ordering::SeqCst), 1);

    runtime.shutdown().await;
}

#[tokio::test]
async fn wal_multiple_submits_work() {
    let dir = TempDir::new().unwrap();
    let executed = Arc::new(AtomicU32::new(0));
    let executed2 = executed.clone();

    let handler = move |_ctx: JobContext| {
        let e = executed2.clone();
        async move {
            e.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = wal_config(&dir, "q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    for _ in 0..5 {
        runtime
            .submit(queue_name("q"), JobSpec::new(b"test"))
            .await
            .unwrap();
    }

    // Wait for all jobs to complete.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(executed.load(Ordering::SeqCst), 5);

    runtime.shutdown().await;
}

// ============================================================
// LEASE DURABILITY TESTS
// ============================================================

#[tokio::test]
async fn wal_lease_before_handler() {
    let dir = TempDir::new().unwrap();
    let seen_attempt = Arc::new(AtomicU32::new(0));
    let seen2 = seen_attempt.clone();

    let handler = move |ctx: JobContext| {
        let s = seen2.clone();
        async move {
            s.store(ctx.attempt, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = wal_config(&dir, "q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Handler saw attempt 1, meaning lease was recorded before handler ran.
    assert_eq!(seen_attempt.load(Ordering::SeqCst), 1);

    runtime.shutdown().await;
}

#[tokio::test]
async fn wal_retries_increment_attempt() {
    let dir = TempDir::new().unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst) + 1;
            if n < ctx.max_attempts {
                Err(JobError::retryable(std::io::Error::other("temp")))
            } else {
                Ok(())
            }
        }
    };

    let retry = RetryConfig::new(Duration::from_millis(50), Duration::from_secs(1));
    let config = wal_config(&dir, "q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    // Wait for all attempts.
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    runtime.shutdown().await;
}

// ============================================================
// COMPLETION DURABILITY TESTS
// ============================================================

#[tokio::test]
async fn wal_completion_transitions_state() {
    let dir = TempDir::new().unwrap();
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = wal_config(&dir, "q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Completed);

    runtime.shutdown().await;
}

#[tokio::test]
async fn wal_completion_releases_capacity() {
    let dir = TempDir::new().unwrap();
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = wal_config(&dir, "q", 1).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should be able to submit another after completion.
    runtime
        .try_submit(queue_name("q"), JobSpec::new(b"2"))
        .await
        .unwrap();

    runtime.shutdown().await;
}

#[tokio::test]
async fn wal_dead_releases_capacity() {
    let dir = TempDir::new().unwrap();
    let handler =
        |_ctx: JobContext| async move { Err(JobError::fatal(std::io::Error::other("fatal"))) };

    let config = wal_config(&dir, "q", 1).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    runtime
        .try_submit(queue_name("q"), JobSpec::new(b"2"))
        .await
        .unwrap();

    runtime.shutdown().await;
}

// ============================================================
// RETRY DURABILITY TESTS
// ============================================================

#[tokio::test]
async fn wal_retry_transitions_to_waiting() {
    let dir = TempDir::new().unwrap();
    let handler =
        |_ctx: JobContext| async move { Err(JobError::retryable(std::io::Error::other("temp"))) };

    let retry = RetryConfig::new(Duration::from_secs(10), Duration::from_secs(60));
    let config = wal_config(&dir, "q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::RetryWaiting);

    runtime.shutdown().await;
}

#[tokio::test]
async fn wal_retry_occurs_after_delay() {
    let dir = TempDir::new().unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst) + 1;
            if n < ctx.max_attempts {
                Err(JobError::retryable(std::io::Error::other("temp")))
            } else {
                Ok(())
            }
        }
    };

    let retry = RetryConfig::new(Duration::from_millis(50), Duration::from_millis(100));
    let config = wal_config(&dir, "q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(2))
        .await
        .unwrap();

    // First attempt.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // After retry delay.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    runtime.shutdown().await;
}

// ============================================================
// CANCEL DURABILITY TESTS
// ============================================================

#[tokio::test]
async fn wal_cancel_transitions_state() {
    let dir = TempDir::new().unwrap();
    let handler =
        |_ctx: JobContext| async move { Err(JobError::retryable(std::io::Error::other("temp"))) };

    let retry = RetryConfig::new(Duration::from_secs(60), Duration::from_secs(120));
    let config = wal_config(&dir, "q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::RetryWaiting);

    let cancelled = runtime.cancel(job.id).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);

    runtime.shutdown().await;
}

#[tokio::test]
async fn wal_cancel_releases_capacity() {
    let dir = TempDir::new().unwrap();
    let handler =
        |_ctx: JobContext| async move { Err(JobError::retryable(std::io::Error::other("temp"))) };

    let retry = RetryConfig::new(Duration::from_secs(60), Duration::from_secs(120));
    let config = wal_config(&dir, "q", 1)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"1").with_max_attempts(3))
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    runtime.cancel(job.id).await.unwrap();

    // Capacity should be released.
    runtime
        .try_submit(queue_name("q"), JobSpec::new(b"2"))
        .await
        .unwrap();

    runtime.shutdown().await;
}

// ============================================================
// PAYLOAD SIZE TESTS
// ============================================================

#[tokio::test]
async fn wal_rejects_oversized_payload() {
    let dir = TempDir::new().unwrap();
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = wal_config(&dir, "q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    // Create payload larger than MAX_PAYLOAD_SIZE (1MB).
    let big_payload = vec![0u8; 2 * 1024 * 1024];

    let result = runtime
        .submit(queue_name("q"), JobSpec::new(big_payload))
        .await;

    assert!(matches!(result, Err(Error::PayloadTooLarge(_, _))));

    runtime.shutdown().await;
}

// ============================================================
// WAL FILE LOCKING TESTS
// ============================================================

#[tokio::test]
async fn wal_exclusive_lock() {
    let dir = TempDir::new().unwrap();
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config1 = wal_config(&dir, "q", 10).with_worker_concurrency(1);
    let _runtime1 = Runtime::start(config1, handler).await.unwrap();

    // Second runtime should fail to start.
    let handler2 = |_ctx: JobContext| async move { Ok(()) };
    let config2 = wal_config(&dir, "q", 10).with_worker_concurrency(1);
    let result = Runtime::start(config2, handler2).await;

    assert!(matches!(result, Err(Error::Storage(_))));
}

// ============================================================
// EXHAUSTED RETRIES TESTS
// ============================================================

#[tokio::test]
async fn wal_exhausted_retries_becomes_dead() {
    let dir = TempDir::new().unwrap();
    let handler =
        |_ctx: JobContext| async move { Err(JobError::retryable(std::io::Error::other("temp"))) };

    let retry = RetryConfig::new(Duration::from_millis(20), Duration::from_millis(50));
    let config = wal_config(&dir, "q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(2))
        .await
        .unwrap();

    // Wait for all attempts to complete.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Dead);

    runtime.shutdown().await;
}

// ============================================================
// VISIBILITY TIMEOUT WITH WAL
// ============================================================

#[tokio::test]
async fn wal_visibility_timeout_triggers_retry() {
    let dir = TempDir::new().unwrap();
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |_ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            // Hang forever.
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok(())
        }
    };

    let retry = RetryConfig::new(Duration::from_millis(20), Duration::from_millis(50));
    let config = wal_config(&dir, "q", 10)
        .with_worker_concurrency(2)
        .with_visibility_timeout(Duration::from_millis(100))
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    // First attempt.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // Visibility timeout + retry.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(attempts.load(Ordering::SeqCst) >= 2);

    runtime.shutdown().await;
}

// ============================================================
// MEMORY MODE STILL WORKS
// ============================================================

#[tokio::test(start_paused = true)]
async fn memory_mode_still_works() {
    let handler = |_ctx: JobContext| async move { Ok(()) };

    // No storage config = memory mode.
    let config = RuntimeConfig::new(vec![QueueConfig::new(queue_name("q"), 10)], 64)
        .with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    // Advance paused time.
    tokio::time::advance(Duration::from_millis(100)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_nanos(1)).await;
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Completed);

    runtime.shutdown().await;
}

// ============================================================
// SEQUENCE NUMBER TESTS
// ============================================================

#[tokio::test]
async fn wal_sequence_increments() {
    let dir = TempDir::new().unwrap();
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = wal_config(&dir, "q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    // Submit multiple jobs.
    for _ in 0..3 {
        runtime
            .submit(queue_name("q"), JobSpec::new(b"test"))
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // WAL file should have grown.
    let wal_size = std::fs::metadata(dir.path().join("wal.log")).unwrap().len();
    assert!(wal_size > 100); // More than just header.

    runtime.shutdown().await;
}

// ============================================================
// REOPEN CONTINUES SEQUENCE
// ============================================================

#[tokio::test]
async fn wal_reopen_continues_sequence() {
    let dir = TempDir::new().unwrap();

    // First runtime.
    {
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let config = wal_config(&dir, "q", 10).with_worker_concurrency(1);
        let runtime = Runtime::start(config, handler).await.unwrap();

        for _ in 0..3 {
            runtime
                .submit(queue_name("q"), JobSpec::new(b"data"))
                .await
                .unwrap();
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
        runtime.shutdown().await;
    }

    let size_after_first = std::fs::metadata(dir.path().join("wal.log")).unwrap().len();

    // Second runtime.
    {
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let config = wal_config(&dir, "q", 10).with_worker_concurrency(1);
        let runtime = Runtime::start(config, handler).await.unwrap();

        runtime
            .submit(queue_name("q"), JobSpec::new(b"more"))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;
        runtime.shutdown().await;
    }

    let size_after_second = std::fs::metadata(dir.path().join("wal.log")).unwrap().len();

    assert!(size_after_second > size_after_first);
}
