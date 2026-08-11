//! WAL recovery and crash reconciliation tests.

use rust_durable_queue::{
    JobContext, JobError, JobId, JobSpec, JobState, QueueConfig, QueueName, Runtime, RuntimeConfig,
    StorageConfig,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::Mutex;

fn queue_name(s: &str) -> QueueName {
    QueueName::new(s).unwrap()
}

fn wal_config(dir: &TempDir, name: &str, capacity: usize) -> RuntimeConfig {
    let queues = vec![QueueConfig::new(queue_name(name), capacity)];
    RuntimeConfig::new(queues, 64)
        .with_worker_concurrency(2)
        .with_visibility_timeout(Duration::from_secs(30))
        .with_storage(StorageConfig::Wal {
            path: dir.path().to_path_buf(),
        })
}

// ============================================================
// QUEUED RECOVERY TESTS
// ============================================================

#[tokio::test]
async fn queued_job_restored_after_restart() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Submit job and let it complete.
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"payload1");
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        // Wait for completion.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Completed);

        rt.shutdown().await;
    }

    // Restart and verify job state is preserved.
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Completed);

        rt.shutdown().await;
    }
}

#[tokio::test]
async fn queued_job_executes_after_restart() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Submit job and let it start but hang, then force shutdown.
    {
        let started = Arc::new(AtomicU32::new(0));
        let started2 = started.clone();

        let handler = move |_ctx: JobContext| {
            let s = started2.clone();
            async move {
                s.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10).with_shutdown_timeout(Duration::from_millis(100));
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test_payload");
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        // Wait for handler to start (job becomes Running).
        while started.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Force shutdown.
        rt.shutdown().await;
    }

    // Restart - job should be recovered (was Running, now should complete).
    {
        let executed = Arc::new(AtomicU32::new(0));
        let executed2 = executed.clone();
        let handler = move |_ctx: JobContext| {
            let e = executed2.clone();
            async move {
                e.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        // Wait for execution - may need to wait for retry queue processing.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let record = rt.status(job_id).await.unwrap();
            if record.state == JobState::Completed {
                break;
            }
        }

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Completed);
        assert!(executed.load(Ordering::SeqCst) >= 1);

        rt.shutdown().await;
    }
}

#[tokio::test]
async fn payload_preserved_after_restart() {
    let dir = TempDir::new().unwrap();
    let expected_payload = b"my_unique_payload_data_12345";
    let job_id;

    // Submit job, let it hang and force shutdown.
    {
        let started = Arc::new(AtomicU32::new(0));
        let started2 = started.clone();

        let handler = move |_ctx: JobContext| {
            let s = started2.clone();
            async move {
                s.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10).with_shutdown_timeout(Duration::from_millis(100));
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(expected_payload.to_vec());
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        // Wait for job to start.
        while started.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        rt.shutdown().await;
    }

    // Restart and verify payload is preserved.
    {
        let payloads: Arc<Mutex<Vec<Vec<u8>>>> = Arc::new(Mutex::new(Vec::new()));
        let payloads2 = payloads.clone();
        let handler = move |ctx: JobContext| {
            let p = payloads2.clone();
            async move {
                p.lock().await.push(ctx.payload.clone());
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        // Wait for execution - may need to wait for retry queue processing.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let record = rt.status(job_id).await.unwrap();
            if record.state == JobState::Completed {
                break;
            }
        }

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Completed);

        let received = payloads.lock().await;
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], expected_payload.to_vec());

        rt.shutdown().await;
    }
}

#[tokio::test]
async fn max_attempts_preserved_after_restart() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Submit job with max_attempts = 1, let it start but hang, then shutdown.
    {
        let started = Arc::new(AtomicU32::new(0));
        let started2 = started.clone();

        let handler = move |_ctx: JobContext| {
            let s = started2.clone();
            async move {
                s.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10).with_shutdown_timeout(Duration::from_millis(100));
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test").with_max_attempts(1);
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        // Wait for job to start.
        while started.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        rt.shutdown().await;
    }

    // Restart with retrying handler - job was Running with 1 attempt used,
    // so recovery should mark it Dead (attempts exhausted).
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        // Wait for recovery to complete.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let record = rt.status(job_id).await.unwrap();
        // Job was Running with 1 attempt (which is >= max_attempts=1), so it becomes Dead.
        assert_eq!(record.state, JobState::Dead);

        rt.shutdown().await;
    }
}

// ============================================================
// COMPLETED RECOVERY TESTS
// ============================================================

#[tokio::test]
async fn completed_job_remains_completed_after_restart() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Run job to completion.
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test");
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        // Wait for completion.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Completed);

        rt.shutdown().await;
    }

    // Restart and verify still completed.
    {
        let executed = Arc::new(AtomicU32::new(0));
        let executed2 = executed.clone();
        let handler = move |_ctx: JobContext| {
            let e = executed2.clone();
            async move {
                e.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Completed);

        tokio::time::sleep(Duration::from_millis(100)).await;
        // Should not have executed again.
        assert_eq!(executed.load(Ordering::SeqCst), 0);

        rt.shutdown().await;
    }
}

#[tokio::test]
async fn completed_job_does_not_consume_capacity() {
    let dir = TempDir::new().unwrap();

    // Complete a job.
    {
        let config = wal_config(&dir, "test", 1);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"job1");
        rt.submit(queue_name("test"), spec).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        rt.shutdown().await;
    }

    // Restart with capacity 1 - should be able to submit new job.
    {
        let config = wal_config(&dir, "test", 1);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        // Should succeed despite capacity = 1 because completed doesn't consume.
        let spec = JobSpec::new(b"new_job");
        let result = rt.try_submit(queue_name("test"), spec).await;
        assert!(result.is_ok());

        rt.shutdown().await;
    }
}

// ============================================================
// DEAD RECOVERY TESTS
// ============================================================

#[tokio::test]
async fn dead_job_remains_dead_after_restart() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Create a dead job.
    {
        let handler = |_ctx: JobContext| async move {
            Err(JobError::retryable(std::io::Error::other("retry")))
        };

        let config = wal_config(&dir, "test", 10).with_retry(rust_durable_queue::RetryConfig::new(
            Duration::from_millis(1),
            Duration::from_millis(1),
        ));
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test").with_max_attempts(1);
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        tokio::time::sleep(Duration::from_millis(200)).await;
        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Dead);

        rt.shutdown().await;
    }

    // Restart and verify still dead.
    {
        let executed = Arc::new(AtomicU32::new(0));
        let executed2 = executed.clone();
        let handler = move |_ctx: JobContext| {
            let e = executed2.clone();
            async move {
                e.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Dead);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(executed.load(Ordering::SeqCst), 0);

        rt.shutdown().await;
    }
}

// ============================================================
// CANCELLED RECOVERY TESTS
// ============================================================

#[tokio::test]
async fn cancelled_job_remains_cancelled_after_restart() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Submit, cancel, then shutdown.
    {
        let barrier = Arc::new(tokio::sync::Barrier::new(100));
        let barrier2 = barrier.clone();
        let handler = move |_ctx: JobContext| {
            let b = barrier2.clone();
            async move {
                b.wait().await;
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test");
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        // Cancel before it can be processed.
        rt.cancel(job_id).await.unwrap();
        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Cancelled);

        rt.shutdown().await;
    }

    // Restart and verify still cancelled.
    {
        let executed = Arc::new(AtomicU32::new(0));
        let executed2 = executed.clone();
        let handler = move |_ctx: JobContext| {
            let e = executed2.clone();
            async move {
                e.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Cancelled);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(executed.load(Ordering::SeqCst), 0);

        rt.shutdown().await;
    }
}

// ============================================================
// RETRY WAITING RECOVERY TESTS
// ============================================================

#[tokio::test]
async fn retry_waiting_job_remains_waiting_if_deadline_in_future() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Create a retry waiting job with far future deadline.
    {
        let handler = |_ctx: JobContext| async move {
            Err(JobError::retryable(std::io::Error::other("retry")))
        };

        let config = wal_config(&dir, "test", 10).with_retry(rust_durable_queue::RetryConfig::new(
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        ));
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test").with_max_attempts(3);
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        tokio::time::sleep(Duration::from_millis(200)).await;
        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::RetryWaiting);

        rt.shutdown().await;
    }

    // Restart immediately - should still be waiting.
    {
        let executed = Arc::new(AtomicU32::new(0));
        let executed2 = executed.clone();
        let handler = move |_ctx: JobContext| {
            let e = executed2.clone();
            async move {
                e.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10).with_retry(rust_durable_queue::RetryConfig::new(
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        ));
        let rt = Runtime::start(config, handler).await.unwrap();

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::RetryWaiting);

        // Should not execute yet.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(executed.load(Ordering::SeqCst), 0);

        rt.shutdown().await;
    }
}

#[tokio::test]
async fn overdue_retry_becomes_immediately_ready() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Create a retry waiting job with very short deadline.
    {
        let handler = |_ctx: JobContext| async move {
            Err(JobError::retryable(std::io::Error::other("retry")))
        };

        let config = wal_config(&dir, "test", 10).with_retry(rust_durable_queue::RetryConfig::new(
            Duration::from_millis(50),
            Duration::from_millis(50),
        ));
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test").with_max_attempts(3);
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        // Wait for first attempt to fail and job to enter RetryWaiting.
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let record = rt.status(job_id).await.unwrap();
            if record.state == JobState::RetryWaiting {
                break;
            }
        }

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::RetryWaiting);

        rt.shutdown().await;
    }

    // Wait a bit so deadline is overdue.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Restart - should become ready immediately.
    {
        let executed = Arc::new(AtomicU32::new(0));
        let executed2 = executed.clone();
        let handler = move |_ctx: JobContext| {
            let e = executed2.clone();
            async move {
                e.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        // Should execute soon since deadline is overdue.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(executed.load(Ordering::SeqCst) >= 1);

        rt.shutdown().await;
    }
}

// ============================================================
// RUNNING / CRASH RECOVERY TESTS
// ============================================================

#[tokio::test]
async fn running_job_never_restored_as_running() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Start a job, let it become Running, then shutdown gracefully but while it's running.
    // The job will be in Running state when the WAL is written.
    {
        let started = Arc::new(AtomicU32::new(0));
        let started2 = started.clone();

        let handler = move |_ctx: JobContext| {
            let s = started2.clone();
            async move {
                s.fetch_add(1, Ordering::SeqCst);
                // Hang forever.
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10)
            .with_shutdown_timeout(Duration::from_millis(100))
            .with_visibility_timeout(Duration::from_secs(3600)); // Very long so timeout doesn't interfere

        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test").with_max_attempts(3);
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        // Wait for handler to start.
        while started.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Running);

        // Graceful shutdown (will force-cancel after timeout).
        rt.shutdown().await;
    }

    // Restart - job should NOT be Running.
    {
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        // Wait a bit for recovery/execution.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let record = rt.status(job_id).await.unwrap();
        // Should be Completed (recovered and re-executed), RetryWaiting, or Queued.
        // NOT Running since old lease is invalidated.
        assert!(
            record.state == JobState::Completed
                || record.state == JobState::RetryWaiting
                || record.state == JobState::Queued,
            "running job should never be restored as running, got {:?}",
            record.state
        );

        rt.shutdown().await;
    }
}

#[tokio::test]
async fn crash_lost_running_with_attempts_remaining_is_retried() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Start a job, let it become Running, then shutdown while running.
    {
        let started = Arc::new(AtomicU32::new(0));
        let started2 = started.clone();

        let handler = move |_ctx: JobContext| {
            let s = started2.clone();
            async move {
                s.fetch_add(1, Ordering::SeqCst);
                // Hang forever.
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10)
            .with_shutdown_timeout(Duration::from_millis(100))
            .with_visibility_timeout(Duration::from_secs(3600));

        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test").with_max_attempts(3);
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        while started.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Graceful shutdown forces cancellation after timeout.
        rt.shutdown().await;
    }

    // Restart - job should be rescheduled and eventually complete.
    {
        let executed = Arc::new(AtomicU32::new(0));
        let executed2 = executed.clone();
        let handler = move |_ctx: JobContext| {
            let e = executed2.clone();
            async move {
                e.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        // Wait for execution - may need to wait for retry queue processing.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let record = rt.status(job_id).await.unwrap();
            if record.state == JobState::Completed {
                break;
            }
        }

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.state, JobState::Completed);
        assert!(executed.load(Ordering::SeqCst) >= 1);

        rt.shutdown().await;
    }
}

#[tokio::test]
async fn recovery_does_not_increment_attempts() {
    let dir = TempDir::new().unwrap();
    let job_id;

    // Submit job and let it complete.
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test").with_max_attempts(3);
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        job_id = record.id;

        // Wait for completion.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.attempts, 1); // Executed once.

        rt.shutdown().await;
    }

    // Restart multiple times - attempts should stay 1, not increase.
    for _ in 0..3 {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        let record = rt.status(job_id).await.unwrap();
        assert_eq!(record.attempts, 1, "recovery should not increment attempts");
        assert_eq!(record.state, JobState::Completed);

        rt.shutdown().await;
    }
}

// ============================================================
// SEQUENCE TESTS
// ============================================================

#[tokio::test]
async fn sequence_continues_after_restart() {
    let dir = TempDir::new().unwrap();

    // Submit some jobs and let them complete.
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        for i in 0..5 {
            let spec = JobSpec::new(format!("job{}", i).as_bytes());
            rt.submit(queue_name("test"), spec).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
        rt.shutdown().await;
    }

    // Restart and submit more - should not fail.
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        for i in 5..10 {
            let spec = JobSpec::new(format!("job{}", i).as_bytes());
            rt.submit(queue_name("test"), spec).await.unwrap();
        }

        rt.shutdown().await;
    }
}

// ============================================================
// CONFIGURATION MISMATCH TESTS
// ============================================================

#[tokio::test]
async fn missing_queue_fails_startup() {
    let dir = TempDir::new().unwrap();

    // Submit to "test" queue (job stays queued via blocking handler).
    {
        let barrier = Arc::new(tokio::sync::Barrier::new(100));
        let barrier2 = barrier.clone();
        let handler = move |_ctx: JobContext| {
            let b = barrier2.clone();
            async move {
                b.wait().await;
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"test");
        rt.submit(queue_name("test"), spec).await.unwrap();
        rt.shutdown().await;
    }

    // Try to restart with different queue name.
    {
        let config = wal_config(&dir, "different", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let result = Runtime::start(config, handler).await;
        assert!(result.is_err(), "should fail with queue not found");
    }
}

#[tokio::test]
async fn reduced_capacity_below_live_jobs_fails_startup() {
    let dir = TempDir::new().unwrap();

    // Submit 5 jobs (blocking handler keeps them queued).
    {
        let barrier = Arc::new(tokio::sync::Barrier::new(100));
        let barrier2 = barrier.clone();
        let handler = move |_ctx: JobContext| {
            let b = barrier2.clone();
            async move {
                b.wait().await;
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        for i in 0..5 {
            let spec = JobSpec::new(format!("job{}", i).as_bytes());
            rt.submit(queue_name("test"), spec).await.unwrap();
        }

        rt.shutdown().await;
    }

    // Try to restart with capacity 2.
    {
        let config = wal_config(&dir, "test", 2);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let result = Runtime::start(config, handler).await;
        assert!(result.is_err(), "should fail with capacity exceeded");
    }
}

#[tokio::test]
async fn exact_capacity_restart_succeeds() {
    let dir = TempDir::new().unwrap();

    // Submit 5 jobs (blocking handler keeps them queued).
    {
        let barrier = Arc::new(tokio::sync::Barrier::new(100));
        let barrier2 = barrier.clone();
        let handler = move |_ctx: JobContext| {
            let b = barrier2.clone();
            async move {
                b.wait().await;
                Ok(())
            }
        };

        let config = wal_config(&dir, "test", 10);
        let rt = Runtime::start(config, handler).await.unwrap();

        for i in 0..5 {
            let spec = JobSpec::new(format!("job{}", i).as_bytes());
            rt.submit(queue_name("test"), spec).await.unwrap();
        }

        rt.shutdown().await;
    }

    // Restart with exactly 5 capacity.
    {
        let config = wal_config(&dir, "test", 5);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();
        rt.shutdown().await;
    }
}

// ============================================================
// LOCKING TESTS
// ============================================================

#[tokio::test]
async fn second_runtime_cannot_open_same_store() {
    let dir = TempDir::new().unwrap();

    let config = wal_config(&dir, "test", 10);
    let handler = |_ctx: JobContext| async move { Ok(()) };
    let rt1 = Runtime::start(config.clone(), handler).await.unwrap();

    let handler2 = |_ctx: JobContext| async move { Ok(()) };
    let result = Runtime::start(config, handler2).await;
    assert!(
        result.is_err(),
        "second runtime should fail with lock error"
    );

    rt1.shutdown().await;
}

#[tokio::test]
async fn store_can_reopen_after_clean_shutdown() {
    let dir = TempDir::new().unwrap();

    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();
        rt.shutdown().await;
    }

    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();
        rt.shutdown().await;
    }
}

// ============================================================
// TRUNCATION RECOVERY TESTS
// ============================================================

#[tokio::test]
async fn truncated_wal_recovered() {
    use rust_durable_queue::wal::{WalFrame, WalHeader, WalRecord};
    use std::io::Write;

    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("wal.log");

    // Create valid WAL with one record, then append truncated frame.
    {
        let mut file = std::fs::File::create(&wal_path).unwrap();
        file.write_all(&WalHeader::new().encode()).unwrap();

        let record = WalRecord::JobSubmitted {
            id: JobId::default(),
            queue: queue_name("test"),
            spec: JobSpec::new(b"payload"),
            created_at: rust_durable_queue::UnixMillis::default(),
        };
        let frame = WalFrame::new(1, record);
        file.write_all(&frame.encode().unwrap()).unwrap();

        // Append partial frame (truncated).
        file.write_all(&[50, 0, 0, 0, 1, 1, 0, 0]).unwrap();
        file.sync_all().unwrap();
    }

    // Should recover successfully.
    let config = wal_config(&dir, "test", 10);
    let handler = |_ctx: JobContext| async move { Ok(()) };
    let result = Runtime::start(config, handler).await;
    assert!(result.is_ok(), "should recover from truncated WAL");
    result.unwrap().shutdown().await;
}

#[tokio::test]
async fn writer_can_append_after_truncation_repair() {
    use rust_durable_queue::wal::{WalFrame, WalHeader, WalRecord};
    use std::io::Write;

    let dir = TempDir::new().unwrap();
    let wal_path = dir.path().join("wal.log");
    let new_job_id;

    // Create valid WAL then truncate.
    {
        let mut file = std::fs::File::create(&wal_path).unwrap();
        file.write_all(&WalHeader::new().encode()).unwrap();

        let record = WalRecord::JobSubmitted {
            id: JobId::default(),
            queue: queue_name("test"),
            spec: JobSpec::new(b"original"),
            created_at: rust_durable_queue::UnixMillis::default(),
        };
        let frame = WalFrame::new(1, record);
        file.write_all(&frame.encode().unwrap()).unwrap();
        file.write_all(&[50, 0, 0, 0]).unwrap();
        file.sync_all().unwrap();
    }

    // Recover and submit new job. Jobs will complete.
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        let spec = JobSpec::new(b"new_job");
        let record = rt.submit(queue_name("test"), spec).await.unwrap();
        new_job_id = record.id;

        // Wait for jobs to complete.
        tokio::time::sleep(Duration::from_millis(300)).await;

        rt.shutdown().await;
    }

    // Restart again to verify the new job was persisted and completed.
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        // Verify job state was persisted.
        let record = rt.status(new_job_id).await.unwrap();
        assert_eq!(record.state, JobState::Completed);

        rt.shutdown().await;
    }
}

// ============================================================
// STATS RECOVERY TEST
// ============================================================

#[tokio::test]
async fn stats_reflect_recovered_state() {
    let dir = TempDir::new().unwrap();

    // Submit and complete some jobs.
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        for i in 0..3 {
            let spec = JobSpec::new(format!("job{}", i).as_bytes());
            rt.submit(queue_name("test"), spec).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(300)).await;

        let stats = rt.stats().await.unwrap();
        assert_eq!(stats.submitted, 3);
        assert_eq!(stats.completed, 3);
        assert_eq!(stats.running, 0);

        rt.shutdown().await;
    }

    // Restart and verify stats.
    {
        let config = wal_config(&dir, "test", 10);
        let handler = |_ctx: JobContext| async move { Ok(()) };
        let rt = Runtime::start(config, handler).await.unwrap();

        let stats = rt.stats().await.unwrap();
        assert_eq!(stats.submitted, 3);
        assert_eq!(stats.completed, 3);
        assert_eq!(stats.running, 0);

        rt.shutdown().await;
    }
}
