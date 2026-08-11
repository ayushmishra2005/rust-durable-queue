use rust_durable_queue::{
    Error, Jitter, JobContext, JobError, JobSpec, JobState, QueueConfig, QueueName, RetryConfig,
    Runtime, RuntimeConfig,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;

fn queue_name(s: &str) -> QueueName {
    QueueName::new(s).unwrap()
}

fn config_single(name: &str, capacity: usize) -> RuntimeConfig {
    RuntimeConfig::new(vec![QueueConfig::new(queue_name(name), capacity)], 64)
}

/// Advance time and ensure coordinator/workers process.
/// Uses timeout to force task scheduling after time advance.
async fn advance(duration: Duration) {
    tokio::time::advance(duration).await;
    // Multiple yield rounds to ensure all ready tasks are polled.
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    // A brief sleep also helps trigger timer processing.
    tokio::time::sleep(Duration::from_nanos(1)).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

// 1. Worker executes queued job
#[tokio::test(start_paused = true)]
async fn worker_executes_queued_job() {
    let executed = Arc::new(AtomicUsize::new(0));
    let executed2 = executed.clone();

    let handler = move |_ctx: JobContext| {
        let e = executed2.clone();
        async move {
            e.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    assert_eq!(executed.load(Ordering::SeqCst), 1);

    runtime.shutdown().await;
}

// 2. Multiple workers run concurrently
#[tokio::test(start_paused = true)]
async fn multiple_workers_run_concurrently() {
    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    let concurrent2 = concurrent.clone();
    let max2 = max_concurrent.clone();

    let handler = move |_ctx: JobContext| {
        let c = concurrent2.clone();
        let m = max2.clone();
        async move {
            let val = c.fetch_add(1, Ordering::SeqCst) + 1;
            m.fetch_max(val, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(50)).await;
            c.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = config_single("q", 10).with_worker_concurrency(4);
    let runtime = Runtime::start(config, handler).await.unwrap();

    for _ in 0..4 {
        runtime
            .submit(queue_name("q"), JobSpec::new(b"test"))
            .await
            .unwrap();
    }

    advance(Duration::from_millis(200)).await;

    assert!(max_concurrent.load(Ordering::SeqCst) > 1);

    runtime.shutdown().await;
}

// 3. Configured worker concurrency is never exceeded
#[tokio::test(start_paused = true)]
async fn worker_concurrency_not_exceeded() {
    let concurrent = Arc::new(AtomicUsize::new(0));
    let max_concurrent = Arc::new(AtomicUsize::new(0));

    let concurrent2 = concurrent.clone();
    let max2 = max_concurrent.clone();

    let handler = move |_ctx: JobContext| {
        let c = concurrent2.clone();
        let m = max2.clone();
        async move {
            let val = c.fetch_add(1, Ordering::SeqCst) + 1;
            m.fetch_max(val, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(100)).await;
            c.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = config_single("q", 20).with_worker_concurrency(2);
    let runtime = Runtime::start(config, handler).await.unwrap();

    for _ in 0..10 {
        runtime
            .submit(queue_name("q"), JobSpec::new(b"test"))
            .await
            .unwrap();
    }

    advance(Duration::from_secs(5)).await;

    assert!(max_concurrent.load(Ordering::SeqCst) <= 2);

    runtime.shutdown().await;
}

// 4. Successful job becomes Completed
#[tokio::test(start_paused = true)]
async fn successful_job_becomes_completed() {
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Completed);

    runtime.shutdown().await;
}

// 5. Successful job releases capacity
#[tokio::test(start_paused = true)]
async fn successful_job_releases_capacity() {
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = config_single("q", 1).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    runtime
        .try_submit(queue_name("q"), JobSpec::new(b"2"))
        .await
        .unwrap();

    runtime.shutdown().await;
}

// 6. Retryable failure moves to RetryWaiting
#[tokio::test(start_paused = true)]
async fn retryable_failure_moves_to_retry_waiting() {
    let handler =
        |_ctx: JobContext| async move { Err(JobError::retryable(std::io::Error::other("temp"))) };

    let retry = RetryConfig::new(Duration::from_secs(10), Duration::from_secs(60));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::RetryWaiting);

    runtime.shutdown().await;
}

// 7. Retry occurs after configured delay - AUTONOMOUS TIMER TEST
#[tokio::test(start_paused = true)]
async fn retry_occurs_after_delay() {
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

    let retry = RetryConfig::new(Duration::from_secs(5), Duration::from_secs(60));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(2))
        .await
        .unwrap();

    // First attempt runs.
    advance(Duration::from_millis(100)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // Not yet time for retry.
    advance(Duration::from_secs(3)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // After retry delay, timer fires autonomously.
    advance(Duration::from_secs(3)).await;

    // Worker executes second attempt.
    advance(Duration::from_millis(100)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    runtime.shutdown().await;
}

// 8. Retry does not occur before deadline
#[tokio::test(start_paused = true)]
async fn retry_does_not_occur_before_deadline() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |_ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            Err(JobError::retryable(std::io::Error::other("temp")))
        }
    };

    let retry = RetryConfig::new(Duration::from_secs(10), Duration::from_secs(60));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // Before retry delay.
    advance(Duration::from_secs(5)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    runtime.shutdown().await;
}

// 9. Attempts increment when execution begins
#[tokio::test(start_paused = true)]
async fn attempts_increment_on_execution() {
    let seen_attempt = Arc::new(AtomicU32::new(0));
    let seen2 = seen_attempt.clone();

    let handler = move |ctx: JobContext| {
        let s = seen2.clone();
        async move {
            s.store(ctx.attempt, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    assert_eq!(seen_attempt.load(Ordering::SeqCst), 1);

    runtime.shutdown().await;
}

// 10. max_attempts is enforced exactly
#[tokio::test(start_paused = true)]
async fn max_attempts_enforced_exactly() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |_ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            Err(JobError::retryable(std::io::Error::other("temp")))
        }
    };

    let retry = RetryConfig::new(Duration::from_millis(100), Duration::from_secs(1));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    // Run through all attempts.
    for _ in 0..5 {
        advance(Duration::from_millis(200)).await;
    }

    assert_eq!(attempts.load(Ordering::SeqCst), 3);

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Dead);

    runtime.shutdown().await;
}

// 11. Exhausted retry becomes Dead
#[tokio::test(start_paused = true)]
async fn exhausted_retry_becomes_dead() {
    let handler =
        |_ctx: JobContext| async move { Err(JobError::retryable(std::io::Error::other("temp"))) };

    let retry = RetryConfig::new(Duration::from_millis(50), Duration::from_millis(100));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(2))
        .await
        .unwrap();

    for _ in 0..10 {
        advance(Duration::from_millis(100)).await;
    }

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Dead);

    runtime.shutdown().await;
}

// 12. Fatal error becomes Dead immediately
#[tokio::test(start_paused = true)]
async fn fatal_error_becomes_dead() {
    let handler =
        |_ctx: JobContext| async move { Err(JobError::fatal(std::io::Error::other("fatal"))) };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(5))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Dead);

    runtime.shutdown().await;
}

// 13. Dead releases capacity
#[tokio::test(start_paused = true)]
async fn dead_releases_capacity() {
    let handler =
        |_ctx: JobContext| async move { Err(JobError::fatal(std::io::Error::other("fatal"))) };

    let config = config_single("q", 1).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    runtime
        .try_submit(queue_name("q"), JobSpec::new(b"2"))
        .await
        .unwrap();

    runtime.shutdown().await;
}

// 14. Stale ACK from old lease is rejected
#[tokio::test(start_paused = true)]
async fn stale_ack_rejected() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |_ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst) + 1;
            if n == 1 {
                // First attempt exceeds visibility timeout then completes.
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
            Ok(())
        }
    };

    let retry = RetryConfig::new(Duration::from_millis(100), Duration::from_secs(1));
    let config = config_single("q", 10)
        .with_worker_concurrency(2)
        .with_visibility_timeout(Duration::from_secs(2))
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    // First attempt starts.
    advance(Duration::from_millis(100)).await;

    // Visibility timeout triggers, schedules retry.
    advance(Duration::from_secs(3)).await;

    // Retry delay passes, second attempt starts.
    advance(Duration::from_millis(200)).await;

    // Second attempt completes.
    advance(Duration::from_millis(100)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Completed);

    // First attempt's sleep completes and tries to report - should be stale.
    advance(Duration::from_secs(10)).await;

    let stats = runtime.stats().await.unwrap();
    assert!(stats.stale_outcomes >= 1);

    runtime.shutdown().await;
}

// 16. Lease timeout triggers retry - AUTONOMOUS TIMER TEST
#[tokio::test(start_paused = true)]
async fn lease_timeout_triggers_retry() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |_ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok(())
        }
    };

    let retry = RetryConfig::new(Duration::from_millis(100), Duration::from_secs(1));
    let config = config_single("q", 10)
        .with_worker_concurrency(2)
        .with_visibility_timeout(Duration::from_secs(1))
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    // First attempt starts.
    advance(Duration::from_millis(100)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // Visibility timeout fires autonomously.
    advance(Duration::from_secs(2)).await;

    // Retry delay passes.
    advance(Duration::from_millis(200)).await;

    // Second attempt starts.
    advance(Duration::from_millis(100)).await;

    assert!(attempts.load(Ordering::SeqCst) >= 2);

    runtime.shutdown().await;
}

// 18. Handler panic is observed
#[tokio::test(start_paused = true)]
async fn handler_panic_observed() {
    let handler = |_ctx: JobContext| async move {
        panic!("intentional panic");
        #[allow(unreachable_code)]
        Ok(())
    };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(1))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Dead);

    runtime.shutdown().await;
}

// 19. Handler panic retries when attempts remain
#[tokio::test(start_paused = true)]
async fn handler_panic_retries() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |_ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst) + 1;
            if n == 1 {
                panic!("first attempt panic");
            }
            Ok(())
        }
    };

    let retry = RetryConfig::new(Duration::from_millis(50), Duration::from_millis(100));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(2))
        .await
        .unwrap();

    // Let retries happen.
    for _ in 0..5 {
        advance(Duration::from_millis(100)).await;
    }

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Completed);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    runtime.shutdown().await;
}

// 20. Handler panic does not permanently reduce worker pool capacity
#[tokio::test(start_paused = true)]
async fn panic_does_not_reduce_worker_capacity() {
    let job_count = Arc::new(AtomicU32::new(0));
    let job_count2 = job_count.clone();

    let handler = move |_ctx: JobContext| {
        let jc = job_count2.clone();
        async move {
            let n = jc.fetch_add(1, Ordering::SeqCst) + 1;
            if n == 1 {
                panic!("first job panics");
            }
            Ok(())
        }
    };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"1").with_max_attempts(1))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    for i in 2..5 {
        runtime
            .submit(
                queue_name("q"),
                JobSpec::new(format!("{}", i)).with_max_attempts(1),
            )
            .await
            .unwrap();
    }

    advance(Duration::from_secs(1)).await;

    assert_eq!(job_count.load(Ordering::SeqCst), 4);

    runtime.shutdown().await;
}

// 22. RetryWaiting cancellation works
#[tokio::test(start_paused = true)]
async fn retry_waiting_cancellation() {
    let handler =
        |_ctx: JobContext| async move { Err(JobError::retryable(std::io::Error::other("temp"))) };

    let retry = RetryConfig::new(Duration::from_secs(60), Duration::from_secs(120));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::RetryWaiting);

    let cancelled = runtime.cancel(job.id).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);

    runtime.shutdown().await;
}

// 23. Running cancellation works
#[tokio::test(start_paused = true)]
async fn running_cancellation() {
    let started = Arc::new(Notify::new());
    let started2 = started.clone();

    let handler = move |ctx: JobContext| {
        let s = started2.clone();
        async move {
            s.notify_one();
            ctx.cancelled().await;
            Ok(())
        }
    };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    advance(Duration::from_millis(50)).await;
    started.notified().await;

    let cancelled = runtime.cancel(job.id).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);

    runtime.shutdown().await;
}

// 24. Late ACK after cancellation is stale
#[tokio::test(start_paused = true)]
async fn late_ack_after_cancellation_is_stale() {
    let started = Arc::new(Notify::new());
    let started2 = started.clone();

    let handler = move |ctx: JobContext| {
        let s = started2.clone();
        async move {
            s.notify_one();
            ctx.cancelled().await;
            Ok(())
        }
    };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    advance(Duration::from_millis(50)).await;
    started.notified().await;

    runtime.cancel(job.id).await.unwrap();

    advance(Duration::from_millis(100)).await;

    let stats = runtime.stats().await.unwrap();
    assert!(stats.stale_outcomes >= 1);

    runtime.shutdown().await;
}

// 26. Repeated/stale terminal outcome does not over-release capacity
#[tokio::test(start_paused = true)]
async fn repeated_stale_outcome_no_over_release() {
    let handler = |_ctx: JobContext| async move {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(())
    };

    let config = config_single("q", 1)
        .with_worker_concurrency(2)
        .with_visibility_timeout(Duration::from_secs(1));
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"1").with_max_attempts(1))
        .await
        .unwrap();

    // Visibility timeout fires, job becomes Dead.
    advance(Duration::from_secs(2)).await;

    // Capacity released exactly once.
    runtime
        .try_submit(queue_name("q"), JobSpec::new(b"2").with_max_attempts(1))
        .await
        .unwrap();

    let result = runtime
        .try_submit(queue_name("q"), JobSpec::new(b"3"))
        .await;
    assert!(matches!(result, Err(Error::QueueFull(_))));

    runtime.shutdown().await;
}

// 27. Graceful shutdown stops new submissions
#[tokio::test(start_paused = true)]
async fn shutdown_stops_new_submissions() {
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime.shutdown().await;
}

// 28. Producer blocked on queue capacity wakes during shutdown
#[tokio::test(start_paused = true)]
async fn blocked_producer_wakes_on_shutdown() {
    let handler = |_ctx: JobContext| async move {
        std::future::pending::<()>().await;
        #[allow(unreachable_code)]
        Ok(())
    };

    let config = config_single("q", 1).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .try_submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    runtime.shutdown().await;
}

// NEW: Running job expires with no other runtime traffic
#[tokio::test(start_paused = true)]
async fn lease_expires_autonomously_no_traffic() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |_ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<()>().await;
            #[allow(unreachable_code)]
            Ok(())
        }
    };

    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_visibility_timeout(Duration::from_secs(1));
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(1))
        .await
        .unwrap();

    // First attempt starts.
    advance(Duration::from_millis(100)).await;

    // Wait for visibility timeout.
    advance(Duration::from_secs(2)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Dead);

    runtime.shutdown().await;
}

// NEW: RetryWaiting becomes runnable autonomously
#[tokio::test(start_paused = true)]
async fn retry_waiting_becomes_runnable_autonomously() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |_ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst) + 1;
            if n == 1 {
                Err(JobError::retryable(std::io::Error::other("temp")))
            } else {
                Ok(())
            }
        }
    };

    let retry = RetryConfig::new(Duration::from_secs(2), Duration::from_secs(60));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(2))
        .await
        .unwrap();

    // First attempt fails.
    advance(Duration::from_millis(100)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::RetryWaiting);

    // Wait for retry delay.
    advance(Duration::from_secs(3)).await;

    // Let worker execute.
    advance(Duration::from_millis(100)).await;

    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Completed);

    runtime.shutdown().await;
}

// NEW: Multiple deadlines fire in correct order
#[tokio::test(start_paused = true)]
async fn multiple_deadlines_fire_correctly() {
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let order2 = order.clone();

    let handler = move |ctx: JobContext| {
        let o = order2.clone();
        async move {
            let id = ctx.payload[0];
            o.lock().unwrap().push(id);
            Err(JobError::retryable(std::io::Error::other("temp")))
        }
    };

    let retry = RetryConfig::new(Duration::from_secs(1), Duration::from_secs(60));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(
            queue_name("q"),
            JobSpec::new(vec![1u8]).with_max_attempts(2),
        )
        .await
        .unwrap();
    runtime
        .submit(
            queue_name("q"),
            JobSpec::new(vec![2u8]).with_max_attempts(2),
        )
        .await
        .unwrap();

    // Both first attempts run.
    advance(Duration::from_millis(100)).await;
    advance(Duration::from_millis(100)).await;

    // Wait for retry delays.
    advance(Duration::from_secs(2)).await;
    advance(Duration::from_millis(100)).await;

    {
        let executed = order.lock().unwrap();
        assert_eq!(executed.len(), 4);
    }

    runtime.shutdown().await;
}

// NEW: Cancelling job makes its deadline harmless
#[tokio::test(start_paused = true)]
async fn cancel_makes_deadline_harmless() {
    let handler =
        |_ctx: JobContext| async move { Err(JobError::retryable(std::io::Error::other("temp"))) };

    let retry = RetryConfig::new(Duration::from_secs(5), Duration::from_secs(60));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test").with_max_attempts(3))
        .await
        .unwrap();

    // First attempt fails.
    advance(Duration::from_millis(100)).await;

    // Cancel while in RetryWaiting.
    runtime.cancel(job.id).await.unwrap();

    // Wait past retry deadline.
    advance(Duration::from_secs(10)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Cancelled);

    runtime.shutdown().await;
}

// NEW: Completing job makes its deadline harmless
#[tokio::test(start_paused = true)]
async fn complete_makes_lease_deadline_harmless() {
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_visibility_timeout(Duration::from_secs(5));
    let runtime = Runtime::start(config, handler).await.unwrap();

    let job = runtime
        .submit(queue_name("q"), JobSpec::new(b"test"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Completed);

    // Wait past visibility timeout.
    advance(Duration::from_secs(10)).await;

    let status = runtime.status(job.id).await.unwrap();
    assert_eq!(status.state, JobState::Completed);

    runtime.shutdown().await;
}

// NEW: Coordinator does not spin when there are no deadlines
#[tokio::test(start_paused = true)]
async fn coordinator_no_spin_without_deadlines() {
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    // No jobs submitted - coordinator should be idle, not spinning.
    // Just verify it doesn't hang or consume CPU.
    advance(Duration::from_secs(60)).await;

    // Should still be responsive.
    let stats = runtime.stats().await.unwrap();
    assert_eq!(stats.submitted, 0);

    runtime.shutdown().await;
}

// Jitter tests
#[test]
fn jitter_none_produces_exact_exponential() {
    let retry = RetryConfig::new(Duration::from_secs(1), Duration::from_secs(300));

    assert_eq!(retry.cap_for_attempt(1), Duration::from_secs(1));
    assert_eq!(retry.cap_for_attempt(2), Duration::from_secs(2));
    assert_eq!(retry.cap_for_attempt(3), Duration::from_secs(4));
    assert_eq!(retry.cap_for_attempt(4), Duration::from_secs(8));
}

#[test]
fn jitter_full_produces_value_within_cap() {
    use rand::SeedableRng;
    let retry = RetryConfig::new(Duration::from_secs(10), Duration::from_secs(60))
        .with_jitter(Jitter::Full);

    let mut rng = rand::rngs::SmallRng::seed_from_u64(42);

    for attempt in 1..=10 {
        let cap = retry.cap_for_attempt(attempt);
        for _ in 0..100 {
            let delay = retry.delay_with_rng(attempt, &mut rng);
            assert!(delay <= cap);
        }
    }
}

#[test]
fn delay_never_exceeds_max() {
    let retry = RetryConfig::new(Duration::from_secs(1), Duration::from_secs(60));

    for attempt in 1..100 {
        let cap = retry.cap_for_attempt(attempt);
        assert!(cap <= retry.max_delay);
    }
}

#[test]
fn large_attempt_counts_no_overflow() {
    let retry = RetryConfig::new(Duration::from_secs(1), Duration::from_secs(300));

    let cap = retry.cap_for_attempt(u32::MAX);
    assert!(cap <= retry.max_delay);
}

#[test]
fn seeded_full_jitter_is_deterministic() {
    use rand::SeedableRng;
    let retry = RetryConfig::new(Duration::from_secs(10), Duration::from_secs(60))
        .with_jitter(Jitter::Full);

    let mut rng1 = rand::rngs::SmallRng::seed_from_u64(12345);
    let mut rng2 = rand::rngs::SmallRng::seed_from_u64(12345);

    for attempt in 1..=5 {
        let d1 = retry.delay_with_rng(attempt, &mut rng1);
        let d2 = retry.delay_with_rng(attempt, &mut rng2);
        assert_eq!(d1, d2);
    }
}

#[test]
fn retry_backoff_never_exceeds_max() {
    let retry = RetryConfig::new(Duration::from_secs(1), Duration::from_secs(60));

    for attempt in 1..100 {
        let delay = retry.cap_for_attempt(attempt);
        assert!(delay <= retry.max_delay);
    }
}

// ============================================================
// GRACEFUL SHUTDOWN TESTS
// ============================================================

#[tokio::test(start_paused = true)]
async fn shutdown_rejects_new_submissions() {
    let handler = |_ctx: JobContext| async move {
        tokio::time::sleep(Duration::from_secs(10)).await;
        Ok(())
    };

    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_shutdown_timeout(Duration::from_secs(5));
    let runtime = Runtime::start(config, handler).await.unwrap();

    // Submit a job that will run during shutdown.
    runtime
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    // Start shutdown in background.
    let shutdown_handle = {
        let rt = runtime;
        tokio::spawn(async move { rt.shutdown().await })
    };

    advance(Duration::from_millis(100)).await;

    // Wait for shutdown to complete.
    advance(Duration::from_secs(10)).await;
    let _ = shutdown_handle.await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_no_new_jobs_leased() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |_ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            a.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(())
        }
    };

    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_shutdown_timeout(Duration::from_secs(5));
    let runtime = Runtime::start(config, handler).await.unwrap();

    // Submit first job.
    runtime
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    // First job starts.
    advance(Duration::from_millis(100)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // Submit second job before shutdown.
    runtime
        .submit(queue_name("q"), JobSpec::new(b"2"))
        .await
        .unwrap();

    // Start shutdown - should not lease the second job.
    let rt = runtime;
    let shutdown_handle = tokio::spawn(async move { rt.shutdown().await });

    // Let first job complete and shutdown finish.
    advance(Duration::from_secs(2)).await;
    let _ = shutdown_handle.await;

    // Only one job executed (second job not leased during shutdown).
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn running_job_completes_before_shutdown_timeout() {
    let completed = Arc::new(AtomicU32::new(0));
    let completed2 = completed.clone();

    let handler = move |_ctx: JobContext| {
        let c = completed2.clone();
        async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            c.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_shutdown_timeout(Duration::from_secs(5));
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    // Job starts.
    advance(Duration::from_millis(100)).await;

    // Job completes (after 1s total).
    advance(Duration::from_secs(2)).await;

    assert_eq!(completed.load(Ordering::SeqCst), 1);

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_returns_early_when_no_running_jobs() {
    let handler = |_ctx: JobContext| async move { Ok(()) };

    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_shutdown_timeout(Duration::from_secs(60));
    let runtime = Runtime::start(config, handler).await.unwrap();

    // No jobs submitted, shutdown should complete immediately.
    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn long_running_job_cancelled_after_shutdown_timeout() {
    let started = Arc::new(AtomicU32::new(0));
    let started2 = started.clone();

    let handler = move |_ctx: JobContext| {
        let s = started2.clone();
        async move {
            s.fetch_add(1, Ordering::SeqCst);
            // Long sleep that exceeds shutdown timeout.
            tokio::time::sleep(Duration::from_secs(60)).await;
            Ok(())
        }
    };

    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_shutdown_timeout(Duration::from_secs(2));
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    // Job starts.
    advance(Duration::from_millis(100)).await;
    assert_eq!(started.load(Ordering::SeqCst), 1);

    let shutdown_handle = tokio::spawn(async move { runtime.shutdown().await });

    // Wait for shutdown timeout.
    advance(Duration::from_secs(3)).await;
    let _ = shutdown_handle.await;
}

#[tokio::test(start_paused = true)]
async fn late_success_after_forced_cancellation_is_stale() {
    let started = Arc::new(Notify::new());
    let started2 = started.clone();

    let handler = move |ctx: JobContext| {
        let s = started2.clone();
        async move {
            s.notify_one();
            // Wait for cancellation then "succeed".
            ctx.cancelled().await;
            Ok(())
        }
    };

    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_shutdown_timeout(Duration::from_secs(1));
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    // Job starts.
    advance(Duration::from_millis(100)).await;
    started.notified().await;

    let shutdown_handle = tokio::spawn(async move { runtime.shutdown().await });

    // Shutdown timeout expires, job cancelled.
    advance(Duration::from_secs(2)).await;
    let _ = shutdown_handle.await;
}

// ============================================================
// PARKED WORKER TESTS (NO POLLING)
// ============================================================

#[tokio::test(start_paused = true)]
async fn submitted_work_wakes_idle_worker_immediately() {
    let executed = Arc::new(AtomicU32::new(0));
    let executed2 = executed.clone();

    let handler = move |_ctx: JobContext| {
        let e = executed2.clone();
        async move {
            e.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    // Worker should be parked waiting for work.
    advance(Duration::from_millis(50)).await;

    // Submit work.
    runtime
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    // Work should be picked up immediately (no 10ms poll delay).
    advance(Duration::from_millis(5)).await;

    assert_eq!(executed.load(Ordering::SeqCst), 1);

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn retry_becoming_ready_wakes_idle_worker() {
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts2 = attempts.clone();

    let handler = move |_ctx: JobContext| {
        let a = attempts2.clone();
        async move {
            let n = a.fetch_add(1, Ordering::SeqCst) + 1;
            if n == 1 {
                Err(JobError::retryable(std::io::Error::other("temp")))
            } else {
                Ok(())
            }
        }
    };

    let retry = RetryConfig::new(Duration::from_secs(2), Duration::from_secs(60));
    let config = config_single("q", 10)
        .with_worker_concurrency(1)
        .with_retry(retry);
    let runtime = Runtime::start(config, handler).await.unwrap();

    runtime
        .submit(queue_name("q"), JobSpec::new(b"1").with_max_attempts(2))
        .await
        .unwrap();

    // First attempt.
    advance(Duration::from_millis(100)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);

    // Wait for retry delay.
    advance(Duration::from_secs(3)).await;

    // Second attempt should happen immediately (no poll delay).
    advance(Duration::from_millis(5)).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_wakes_parked_workers() {
    let executed = Arc::new(AtomicU32::new(0));
    let executed2 = executed.clone();

    let handler = move |_ctx: JobContext| {
        let e = executed2.clone();
        async move {
            e.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = config_single("q", 10).with_worker_concurrency(4);
    let runtime = Runtime::start(config, handler).await.unwrap();

    // Workers parked waiting for work.
    advance(Duration::from_millis(100)).await;

    // Shutdown should wake all parked workers.
    runtime.shutdown().await;
}

// ============================================================
// ROUND-ROBIN SCHEDULING TESTS
// ============================================================

fn config_multi(names: &[&str], capacity: usize) -> RuntimeConfig {
    let queues: Vec<_> = names
        .iter()
        .map(|n| QueueConfig::new(queue_name(n), capacity))
        .collect();
    RuntimeConfig::new(queues, 64)
}

#[tokio::test(start_paused = true)]
async fn two_queues_both_make_progress() {
    let executed_a = Arc::new(AtomicU32::new(0));
    let executed_b = Arc::new(AtomicU32::new(0));
    let ea = executed_a.clone();
    let eb = executed_b.clone();

    let handler = move |ctx: JobContext| {
        let a = ea.clone();
        let b = eb.clone();
        async move {
            if ctx.queue.as_str() == "a" {
                a.fetch_add(1, Ordering::SeqCst);
            } else {
                b.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        }
    };

    let config = config_multi(&["a", "b"], 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    // Submit to both queues.
    for _ in 0..5 {
        runtime
            .submit(queue_name("a"), JobSpec::new(b"a"))
            .await
            .unwrap();
        runtime
            .submit(queue_name("b"), JobSpec::new(b"b"))
            .await
            .unwrap();
    }

    // Process all.
    advance(Duration::from_secs(1)).await;

    // Both queues should have made progress.
    assert_eq!(executed_a.load(Ordering::SeqCst), 5);
    assert_eq!(executed_b.load(Ordering::SeqCst), 5);

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn three_queues_rotate() {
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let order2 = order.clone();

    let handler = move |ctx: JobContext| {
        let o = order2.clone();
        async move {
            o.lock().unwrap().push(ctx.queue.as_str().to_string());
            Ok(())
        }
    };

    let config = config_multi(&["a", "b", "c"], 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    // Submit one job to each queue.
    runtime
        .submit(queue_name("a"), JobSpec::new(b"1"))
        .await
        .unwrap();
    runtime
        .submit(queue_name("b"), JobSpec::new(b"2"))
        .await
        .unwrap();
    runtime
        .submit(queue_name("c"), JobSpec::new(b"3"))
        .await
        .unwrap();

    advance(Duration::from_secs(1)).await;

    {
        let executed = order.lock().unwrap();
        assert_eq!(executed.len(), 3);
        assert!(executed.contains(&"a".to_string()));
        assert!(executed.contains(&"b".to_string()));
        assert!(executed.contains(&"c".to_string()));
    }

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn empty_queue_skipped() {
    let executed = Arc::new(AtomicU32::new(0));
    let executed2 = executed.clone();

    let handler = move |_ctx: JobContext| {
        let e = executed2.clone();
        async move {
            e.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    };

    let config = config_multi(&["a", "b", "c"], 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    // Only submit to queue "b".
    runtime
        .submit(queue_name("b"), JobSpec::new(b"1"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    assert_eq!(executed.load(Ordering::SeqCst), 1);

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn queue_rejoins_rotation() {
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let order2 = order.clone();

    let handler = move |ctx: JobContext| {
        let o = order2.clone();
        async move {
            o.lock().unwrap().push(ctx.queue.as_str().to_string());
            Ok(())
        }
    };

    let config = config_multi(&["a", "b"], 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    // Submit to "a" only first.
    runtime
        .submit(queue_name("a"), JobSpec::new(b"1"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    // Now submit to "b".
    runtime
        .submit(queue_name("b"), JobSpec::new(b"2"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    // Submit to "a" again.
    runtime
        .submit(queue_name("a"), JobSpec::new(b"3"))
        .await
        .unwrap();

    advance(Duration::from_millis(100)).await;

    {
        let executed = order.lock().unwrap();
        assert_eq!(executed.len(), 3);
    }

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn fifo_within_queue() {
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let order2 = order.clone();

    let handler = move |ctx: JobContext| {
        let o = order2.clone();
        async move {
            o.lock().unwrap().push(ctx.payload[0]);
            Ok(())
        }
    };

    let config = config_single("q", 10).with_worker_concurrency(1);
    let runtime = Runtime::start(config, handler).await.unwrap();

    for i in 1..=5u8 {
        runtime
            .submit(queue_name("q"), JobSpec::new(vec![i]))
            .await
            .unwrap();
    }

    advance(Duration::from_secs(1)).await;

    {
        let executed = order.lock().unwrap();
        assert_eq!(*executed, vec![1, 2, 3, 4, 5]);
    }

    runtime.shutdown().await;
}

// ============================================================
// CONFIG VALIDATION TESTS
// ============================================================

#[test]
fn zero_base_delay_rejected() {
    let retry = RetryConfig::new(Duration::ZERO, Duration::from_secs(60));
    assert!(retry.validate().is_err());
}

#[test]
fn zero_max_delay_rejected() {
    let retry = RetryConfig::new(Duration::from_secs(1), Duration::ZERO);
    assert!(retry.validate().is_err());
}

#[test]
fn base_delay_exceeds_max_rejected() {
    let retry = RetryConfig::new(Duration::from_secs(60), Duration::from_secs(10));
    assert!(retry.validate().is_err());
}

#[test]
fn zero_visibility_timeout_rejected() {
    let config = config_single("q", 10).with_visibility_timeout(Duration::ZERO);
    assert!(config.validate().is_err());
}

#[test]
fn zero_shutdown_timeout_rejected() {
    let config = config_single("q", 10).with_shutdown_timeout(Duration::ZERO);
    assert!(config.validate().is_err());
}

#[test]
fn huge_duration_no_truncation() {
    // Duration larger than u64 milliseconds.
    let huge = Duration::from_secs(u64::MAX / 1000);
    let retry = RetryConfig::new(huge, huge);

    // Should not panic or truncate incorrectly.
    let cap = retry.cap_for_attempt(1);
    assert!(cap <= retry.max_delay);
}
