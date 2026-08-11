use rust_durable_queue::{Error, JobSpec, JobState, QueueConfig, QueueName, RuntimeConfig, start};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Barrier;

fn queue_name(s: &str) -> QueueName {
    QueueName::new(s).unwrap()
}

fn config_single(name: &str, capacity: usize) -> RuntimeConfig {
    RuntimeConfig::new(vec![QueueConfig::new(queue_name(name), capacity)], 64)
}

// 1. Valid queue configuration
#[tokio::test]
async fn valid_queue_configuration() {
    let config = config_single("test", 10);
    assert!(config.validate().is_ok());
}

// 2. Invalid empty queue name
#[test]
fn invalid_empty_queue_name() {
    assert!(QueueName::new("").is_none());
}

// 3. Duplicate queue configuration
#[test]
fn duplicate_queue_configuration() {
    let config = RuntimeConfig::new(
        vec![
            QueueConfig::new(queue_name("same"), 10),
            QueueConfig::new(queue_name("same"), 5),
        ],
        64,
    );
    let err = config.validate().unwrap_err();
    assert!(matches!(err, Error::InvalidConfiguration(_)));
}

// 4. Zero capacity rejected
#[test]
fn zero_capacity_rejected() {
    let config = config_single("test", 0);
    let err = config.validate().unwrap_err();
    assert!(matches!(err, Error::InvalidConfiguration(_)));
}

// 5. Submit creates queued job
#[tokio::test]
async fn submit_creates_queued_job() {
    let handle = start(config_single("q", 10)).await.unwrap();
    let record = handle
        .submit(queue_name("q"), JobSpec::new(b"payload"))
        .await
        .unwrap();
    assert_eq!(record.state, JobState::Queued);
}

// 6. Status returns submitted job
#[tokio::test]
async fn status_returns_submitted_job() {
    let handle = start(config_single("q", 10)).await.unwrap();
    let record = handle
        .submit(queue_name("q"), JobSpec::new(b"data"))
        .await
        .unwrap();
    let status = handle.status(record.id).await.unwrap();
    assert_eq!(status.id, record.id);
    assert_eq!(status.state, JobState::Queued);
}

// 7. Multiple named queues work independently
#[tokio::test]
async fn multiple_queues_independent() {
    let config = RuntimeConfig::new(
        vec![
            QueueConfig::new(queue_name("a"), 5),
            QueueConfig::new(queue_name("b"), 5),
        ],
        64,
    );
    let handle = start(config).await.unwrap();

    let job_a = handle
        .submit(queue_name("a"), JobSpec::new(b"a"))
        .await
        .unwrap();
    let job_b = handle
        .submit(queue_name("b"), JobSpec::new(b"b"))
        .await
        .unwrap();

    assert_eq!(job_a.queue.as_str(), "a");
    assert_eq!(job_b.queue.as_str(), "b");

    let stats = handle.stats().await.unwrap();
    assert_eq!(stats.per_queue.get("a").unwrap().queued, 1);
    assert_eq!(stats.per_queue.get("b").unwrap().queued, 1);
}

// 8. Unknown queue submission fails
#[tokio::test]
async fn unknown_queue_fails() {
    let handle = start(config_single("exists", 10)).await.unwrap();
    let result = handle
        .try_submit(queue_name("unknown"), JobSpec::new(b"x"))
        .await;
    assert!(matches!(result, Err(Error::QueueNotFound(_))));
}

// 9. try_submit returns QueueFull when full
#[tokio::test]
async fn try_submit_returns_queue_full() {
    let handle = start(config_single("q", 2)).await.unwrap();

    handle
        .try_submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();
    handle
        .try_submit(queue_name("q"), JobSpec::new(b"2"))
        .await
        .unwrap();

    let result = handle.try_submit(queue_name("q"), JobSpec::new(b"3")).await;
    assert!(matches!(result, Err(Error::QueueFull(_))));
}

// 10. submit blocks when full and continues after capacity is released
#[tokio::test]
async fn submit_blocks_until_capacity_released() {
    let handle = start(config_single("q", 1)).await.unwrap();

    let job1 = handle
        .submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    let handle2 = handle.clone();
    let submitted = Arc::new(AtomicUsize::new(0));
    let submitted2 = submitted.clone();

    let submit_task = tokio::spawn(async move {
        handle2
            .submit(queue_name("q"), JobSpec::new(b"2"))
            .await
            .unwrap();
        submitted2.store(1, Ordering::SeqCst);
    });

    // Give submitter time to block.
    tokio::task::yield_now().await;
    assert_eq!(submitted.load(Ordering::SeqCst), 0);

    // Release capacity via cancel.
    handle.cancel(job1.id).await.unwrap();

    submit_task.await.unwrap();
    assert_eq!(submitted.load(Ordering::SeqCst), 1);
}

// 11. Queued job cancellation works
#[tokio::test]
async fn queued_job_cancellation() {
    let handle = start(config_single("q", 10)).await.unwrap();
    let job = handle
        .submit(queue_name("q"), JobSpec::new(b"x"))
        .await
        .unwrap();

    let cancelled = handle.cancel(job.id).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
}

// 12. Cancellation releases capacity
#[tokio::test]
async fn cancellation_releases_capacity() {
    let handle = start(config_single("q", 1)).await.unwrap();

    let job = handle
        .try_submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    // Queue is full.
    assert!(
        handle
            .try_submit(queue_name("q"), JobSpec::new(b"2"))
            .await
            .is_err()
    );

    handle.cancel(job.id).await.unwrap();

    // Now we can submit again.
    handle
        .try_submit(queue_name("q"), JobSpec::new(b"3"))
        .await
        .unwrap();
}

// 13. Repeated/invalid cancellation is handled correctly
#[tokio::test]
async fn repeated_cancellation_fails() {
    let handle = start(config_single("q", 10)).await.unwrap();
    let job = handle
        .submit(queue_name("q"), JobSpec::new(b"x"))
        .await
        .unwrap();

    handle.cancel(job.id).await.unwrap();

    let result = handle.cancel(job.id).await;
    assert!(matches!(result, Err(Error::InvalidTransition { .. })));
}

// 14. Capacity never exceeds configured limit
#[tokio::test]
async fn capacity_never_exceeded() {
    let handle = start(config_single("q", 3)).await.unwrap();

    for i in 0..3 {
        handle
            .try_submit(queue_name("q"), JobSpec::new(format!("{}", i)))
            .await
            .unwrap();
    }

    // All subsequent attempts should fail.
    for _ in 0..5 {
        let result = handle
            .try_submit(queue_name("q"), JobSpec::new(b"overflow"))
            .await;
        assert!(matches!(result, Err(Error::QueueFull(_))));
    }

    let stats = handle.stats().await.unwrap();
    assert_eq!(stats.per_queue.get("q").unwrap().queued, 3);
}

// 15. Multiple concurrent producers do not violate capacity
#[tokio::test]
async fn concurrent_producers_respect_capacity() {
    let capacity = 10usize;
    let producers = 20usize;
    let handle = start(config_single("q", capacity)).await.unwrap();
    let success_count = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(producers));

    let mut tasks = Vec::new();
    for _ in 0..producers {
        let h = handle.clone();
        let sc = success_count.clone();
        let b = barrier.clone();
        tasks.push(tokio::spawn(async move {
            b.wait().await;
            if h.try_submit(queue_name("q"), JobSpec::new(b"x"))
                .await
                .is_ok()
            {
                sc.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    assert_eq!(success_count.load(Ordering::SeqCst), capacity);
}

// 16. Legal state transitions work
#[tokio::test]
async fn legal_transition_queued_to_cancelled() {
    let handle = start(config_single("q", 10)).await.unwrap();
    let job = handle
        .submit(queue_name("q"), JobSpec::new(b"x"))
        .await
        .unwrap();
    assert_eq!(job.state, JobState::Queued);

    let cancelled = handle.cancel(job.id).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
}

// 17. Illegal state transitions are rejected
#[tokio::test]
async fn illegal_transition_cancelled_to_cancelled() {
    let handle = start(config_single("q", 10)).await.unwrap();
    let job = handle
        .submit(queue_name("q"), JobSpec::new(b"x"))
        .await
        .unwrap();

    handle.cancel(job.id).await.unwrap();
    let result = handle.cancel(job.id).await;

    assert!(matches!(result, Err(Error::InvalidTransition { .. })));
}

// Additional: job not found
#[tokio::test]
async fn status_job_not_found() {
    let handle = start(config_single("q", 10)).await.unwrap();
    let fake_id = rust_durable_queue::JobId::new();
    let result = handle.status(fake_id).await;
    assert!(matches!(result, Err(Error::JobNotFound(_))));
}

// Additional: stats reflect submissions
#[tokio::test]
async fn stats_reflect_submissions() {
    let handle = start(config_single("q", 10)).await.unwrap();

    for _ in 0..5 {
        handle
            .submit(queue_name("q"), JobSpec::new(b"x"))
            .await
            .unwrap();
    }

    let stats = handle.stats().await.unwrap();
    assert_eq!(stats.submitted, 5);
    assert_eq!(stats.queued, 5);
}

// Additional: stats reflect cancellations
#[tokio::test]
async fn stats_reflect_cancellations() {
    let handle = start(config_single("q", 10)).await.unwrap();

    let job = handle
        .submit(queue_name("q"), JobSpec::new(b"x"))
        .await
        .unwrap();
    handle.cancel(job.id).await.unwrap();

    let stats = handle.stats().await.unwrap();
    assert_eq!(stats.submitted, 1);
    assert_eq!(stats.cancelled, 1);
    assert_eq!(stats.queued, 0);
}

// Additional: zero channel capacity rejected
#[test]
fn zero_channel_capacity_rejected() {
    let config = RuntimeConfig::new(vec![QueueConfig::new(queue_name("q"), 10)], 0);
    let err = config.validate().unwrap_err();
    assert!(matches!(err, Error::InvalidConfiguration(_)));
}

// Cancelling a queued job releases exactly one permit
#[tokio::test]
async fn cancel_releases_exactly_one_permit() {
    let handle = start(config_single("q", 2)).await.unwrap();

    let job1 = handle
        .try_submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();
    let _job2 = handle
        .try_submit(queue_name("q"), JobSpec::new(b"2"))
        .await
        .unwrap();

    // Queue full.
    assert!(
        handle
            .try_submit(queue_name("q"), JobSpec::new(b"3"))
            .await
            .is_err()
    );

    // Cancel one job.
    handle.cancel(job1.id).await.unwrap();

    // Exactly one slot freed.
    handle
        .try_submit(queue_name("q"), JobSpec::new(b"4"))
        .await
        .unwrap();

    // Still full again.
    assert!(
        handle
            .try_submit(queue_name("q"), JobSpec::new(b"5"))
            .await
            .is_err()
    );
}

// Repeated cancellation does not release extra permits
#[tokio::test]
async fn repeated_cancel_does_not_release_extra_permits() {
    let handle = start(config_single("q", 1)).await.unwrap();

    let job = handle
        .try_submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    // Cancel once - releases permit.
    handle.cancel(job.id).await.unwrap();

    // Attempt repeated cancel - should fail but not release another permit.
    let _ = handle.cancel(job.id).await;
    let _ = handle.cancel(job.id).await;

    // Only one slot should be available (from the one valid cancel).
    handle
        .try_submit(queue_name("q"), JobSpec::new(b"2"))
        .await
        .unwrap();

    // Queue full again - proves no extra permits leaked.
    assert!(
        handle
            .try_submit(queue_name("q"), JobSpec::new(b"3"))
            .await
            .is_err()
    );
}

// Cancelling a submit future while waiting does not leak a permit
#[tokio::test]
async fn cancel_waiting_submit_does_not_leak_permit() {
    let handle = start(config_single("q", 1)).await.unwrap();

    // Fill the queue.
    handle
        .try_submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    let handle2 = handle.clone();
    let submit_task =
        tokio::spawn(async move { handle2.submit(queue_name("q"), JobSpec::new(b"2")).await });

    // Let it start waiting.
    tokio::task::yield_now().await;

    // Abort the waiting submit.
    submit_task.abort();
    let _ = submit_task.await;

    // Verify no permit leaked: capacity is still 1, and we have 1 job.
    // If we cancel that job, we should be able to submit exactly one more.
    let stats = handle.stats().await.unwrap();
    assert_eq!(stats.queued, 1);

    // Queue is still full (the aborted submit didn't consume a slot).
    assert!(
        handle
            .try_submit(queue_name("q"), JobSpec::new(b"leaked?"))
            .await
            .is_err()
    );
}

// Many blocked submitters use semaphore, not custom waiter structure
#[tokio::test]
async fn many_blocked_submitters_wake_correctly() {
    let capacity = 2usize;
    let waiters = 5usize;
    let handle = start(config_single("q", capacity)).await.unwrap();
    let completed = Arc::new(AtomicUsize::new(0));
    let job_ids = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    // Fill capacity.
    for i in 0..capacity {
        let job = handle
            .try_submit(queue_name("q"), JobSpec::new(format!("init{}", i)))
            .await
            .unwrap();
        job_ids.lock().await.push(job.id);
    }

    // Spawn many waiters that record their job IDs.
    let mut tasks = Vec::new();
    for i in 0..waiters {
        let h = handle.clone();
        let c = completed.clone();
        let ids = job_ids.clone();
        tasks.push(tokio::spawn(async move {
            let job = h
                .submit(queue_name("q"), JobSpec::new(format!("wait{}", i)))
                .await
                .unwrap();
            ids.lock().await.push(job.id);
            c.fetch_add(1, Ordering::SeqCst);
        }));
    }

    // Let waiters block.
    tokio::task::yield_now().await;
    assert_eq!(completed.load(Ordering::SeqCst), 0);

    // Release capacity by cancelling jobs, allowing waiters to proceed.
    // Each cancel releases one slot, allowing one waiter to complete.
    for _ in 0..(capacity + waiters) {
        tokio::task::yield_now().await;
        let ids = job_ids.lock().await;
        if let Some(&id) = ids.first() {
            drop(ids);
            handle.cancel(id).await.ok();
            job_ids.lock().await.retain(|&x| x != id);
        }
    }

    // Wait for all waiters to complete.
    for t in tasks {
        t.await.unwrap();
    }

    assert_eq!(completed.load(Ordering::SeqCst), waiters);
}

// Shutdown wakes producers waiting for capacity
#[tokio::test]
async fn shutdown_wakes_blocked_submitters() {
    use std::time::Duration;

    let config = config_single("q", 1);
    let handle = start(config).await.unwrap();

    // Fill capacity.
    handle
        .try_submit(queue_name("q"), JobSpec::new(b"1"))
        .await
        .unwrap();

    let handle2 = handle.clone();
    let submit_task =
        tokio::spawn(async move { handle2.submit(queue_name("q"), JobSpec::new(b"2")).await });

    // Let it start waiting.
    tokio::task::yield_now().await;

    // Explicitly shutdown to wake blocked submitters.
    handle.shutdown();

    // The blocked submit should complete with an error (not hang).
    let result = tokio::time::timeout(Duration::from_millis(100), submit_task).await;
    assert!(result.is_ok(), "submit should not hang after shutdown");

    let inner = result.unwrap().unwrap();
    assert!(
        matches!(inner, Err(Error::ShuttingDown)),
        "expected ShuttingDown error"
    );
}
