//! Basic example demonstrating library usage.
//!
//! Run with: cargo run --example basic

use rust_durable_queue::{
    JobContext, JobError, JobSpec, QueueConfig, QueueName, Result, Runtime, RuntimeConfig,
    StorageConfig,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Configure queues with bounded capacity.
    let emails = QueueName::new("emails").expect("valid queue name");
    let reports = QueueName::new("reports").expect("valid queue name");

    let queues = vec![QueueConfig::new(emails, 100), QueueConfig::new(reports, 50)];

    // Build runtime configuration.
    let config = RuntimeConfig::new(queues, 64)
        .with_storage(StorageConfig::Memory)
        .with_worker_concurrency(4)
        .with_retry(rust_durable_queue::RetryConfig::new(
            Duration::from_secs(1),
            Duration::from_secs(30),
        ))
        .with_visibility_timeout(Duration::from_secs(60))
        .with_shutdown_timeout(Duration::from_secs(10));

    // Define a handler that processes jobs.
    let handler = |ctx: JobContext| async move {
        let payload = String::from_utf8_lossy(&ctx.payload);
        println!(
            "Processing job {} (attempt {}/{}): {}",
            ctx.id, ctx.attempt, ctx.max_attempts, payload
        );

        // Simulate work.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check for cancellation.
        if ctx.is_cancelled() {
            println!("Job {} was cancelled", ctx.id);
            return Ok(());
        }

        // Simulate occasional failures for retry demonstration.
        if payload.contains("fail-once") && ctx.attempt == 1 {
            return Err(JobError::retryable(std::io::Error::other(
                "transient failure",
            )));
        }

        if payload.contains("fatal") {
            return Err(JobError::fatal(std::io::Error::other("permanent failure")));
        }

        Ok(())
    };

    // Start the runtime.
    let rt = Runtime::start(config, handler).await?;

    // Submit jobs.
    let email_queue = QueueName::new("emails").expect("valid queue name");
    let report_queue = QueueName::new("reports").expect("valid queue name");

    let job1 = rt
        .submit(email_queue.clone(), JobSpec::new(b"send welcome email"))
        .await?;
    println!("Submitted job: {}", job1.id);

    let job2 = rt
        .submit(email_queue.clone(), JobSpec::new(b"fail-once: retry test"))
        .await?;
    println!("Submitted job: {}", job2.id);

    let job3 = rt
        .submit(
            report_queue.clone(),
            JobSpec::new(b"generate monthly report"),
        )
        .await?;
    println!("Submitted job: {}", job3.id);

    // Wait for jobs to complete.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check job status.
    let status1 = rt.status(job1.id).await?;
    let status2 = rt.status(job2.id).await?;
    let status3 = rt.status(job3.id).await?;

    println!("\nJob statuses:");
    println!("  Job 1: {:?}", status1.state);
    println!(
        "  Job 2: {:?} (attempts: {})",
        status2.state, status2.attempts
    );
    println!("  Job 3: {:?}", status3.state);

    // Get statistics.
    let stats = rt.stats().await?;
    println!("\nStatistics:");
    println!("  Submitted: {}", stats.submitted);
    println!("  Completed: {}", stats.completed);
    println!("  Retried: {}", stats.retried);

    // Graceful shutdown.
    rt.shutdown().await;
    println!("\nShutdown complete.");

    Ok(())
}
