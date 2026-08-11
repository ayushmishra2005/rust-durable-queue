use crate::coordinator::{Command, LeasedJob, WorkerOutcome};
use crate::handler::{Handler, JobContext};
use crate::types::JobId;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Runs the worker pool.
pub async fn run_workers<H: Handler>(
    handler: Arc<H>,
    cmd_tx: mpsc::Sender<Command>,
    concurrency: usize,
    shutdown_token: CancellationToken,
) {
    let mut handles: Vec<JoinHandle<()>> = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let h = handler.clone();
        let tx = cmd_tx.clone();
        let token = shutdown_token.clone();

        handles.push(tokio::spawn(worker_loop(h, tx, token)));
    }

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
        if shutdown_token.is_cancelled() {
            break;
        }

        // Request work (will be parked if none available).
        let leased = match fetch_work(&cmd_tx, &shutdown_token).await {
            Some(job) => job,
            None => break, // Shutdown or channel closed.
        };

        let job_id = leased.context.id;
        let lease_id = leased.lease_id;

        // Execute handler with cancellation awareness.
        let outcome =
            execute_with_cancellation(&handler, leased.context, job_id, &shutdown_token).await;

        // Report outcome.
        let _ = cmd_tx
            .send(Command::WorkerOutcome {
                id: job_id,
                lease_id,
                outcome,
            })
            .await;
    }
}

async fn fetch_work(
    cmd_tx: &mpsc::Sender<Command>,
    shutdown_token: &CancellationToken,
) -> Option<LeasedJob> {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

    if cmd_tx
        .send(Command::FetchWork { reply: reply_tx })
        .await
        .is_err()
    {
        return None;
    }

    // Wait for work or shutdown. The coordinator will send None on shutdown.
    tokio::select! {
        biased;
        result = reply_rx => result.ok().flatten(),
        _ = shutdown_token.cancelled() => None,
    }
}

async fn execute_with_cancellation<H: Handler>(
    handler: &Arc<H>,
    ctx: JobContext,
    job_id: JobId,
    shutdown_token: &CancellationToken,
) -> WorkerOutcome {
    let h = handler.clone();
    let task = tokio::spawn(async move { h.handle(ctx).await });

    tokio::pin!(task);

    tokio::select! {
        biased;
        result = &mut task => {
            match result {
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
        _ = shutdown_token.cancelled() => {
            task.abort();
            WorkerOutcome::Retryable
        }
    }
}
