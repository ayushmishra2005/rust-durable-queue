use crate::coordinator::{Command, WorkerOutcome};
use crate::handler::{Handler, JobContext};
use crate::types::JobId;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Runs the worker pool.
pub async fn run_workers<H: Handler>(
    handler: Arc<H>,
    cmd_tx: mpsc::Sender<Command>,
    concurrency: usize,
    shutdown_token: CancellationToken,
) {
    let mut handles = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let h = handler.clone();
        let tx = cmd_tx.clone();
        let token = shutdown_token.clone();

        handles.push(tokio::spawn(worker_loop(h, tx, token)));
    }

    // Wait for all workers to exit.
    for handle in handles {
        let _ = handle.await;
    }
}

async fn worker_loop<H: Handler>(
    handler: Arc<H>,
    cmd_tx: mpsc::Sender<Command>,
    shutdown_token: CancellationToken,
) {
    loop {
        // Check for shutdown.
        if shutdown_token.is_cancelled() {
            break;
        }

        // Request work.
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        if cmd_tx
            .send(Command::FetchWork { reply: reply_tx })
            .await
            .is_err()
        {
            // Coordinator gone.
            break;
        }

        let leased = match reply_rx.await {
            Ok(Some(job)) => job,
            Ok(None) => {
                // No work available; wait briefly or until shutdown.
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => continue,
                    _ = shutdown_token.cancelled() => break,
                }
            }
            Err(_) => break,
        };

        // Save IDs before consuming context.
        let job_id = leased.context.id;
        let lease_id = leased.lease_id;

        // Execute with shutdown awareness.
        let shutdown = shutdown_token.clone();
        let outcome = tokio::select! {
            result = execute_job(&handler, leased.context, job_id) => result,
            _ = shutdown.cancelled() => {
                // Shutdown requested during execution.
                WorkerOutcome::Retryable
            }
        };

        // Report outcome if coordinator is still running.
        let _ = cmd_tx
            .send(Command::WorkerOutcome {
                id: job_id,
                lease_id,
                outcome,
            })
            .await;
    }
}

async fn execute_job<H: Handler>(
    handler: &Arc<H>,
    ctx: JobContext,
    job_id: JobId,
) -> WorkerOutcome {
    // Spawn handler in a separate task to catch panics.
    let h = handler.clone();
    let task = tokio::spawn(async move { h.handle(ctx).await });

    match task.await {
        Ok(Ok(())) => WorkerOutcome::Success,
        Ok(Err(e)) => {
            if e.is_retryable() {
                WorkerOutcome::Retryable
            } else {
                WorkerOutcome::Fatal
            }
        }
        Err(join_err) => {
            if join_err.is_panic() {
                eprintln!("handler panicked for job {}", job_id);
            }
            WorkerOutcome::Panic
        }
    }
}
