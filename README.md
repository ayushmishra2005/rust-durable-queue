# Rust Durable Queue

A single-process async job queue written in Rust. Currently under development.

## Current Features

- Bounded named queues with configurable capacity
- Concurrent workers with configurable concurrency
- Async handler API with job context
- Leases with fencing for stale outcome rejection
- Visibility timeout for hanging jobs
- Retries with exponential backoff and jitter
- Dead-letter state for exhausted retries and fatal failures
- Cancellation of queued, waiting, and running jobs
- Graceful shutdown with configurable timeout
- Round-robin scheduling across named queues
- Parked workers (no polling)
- Coordinator-owned mutable state (no shared locks)
- **WAL durability layer** with persist-before-apply ordering

## Job Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Queued: submit
    Queued --> Running: worker fetches
    Running --> Completed: success
    Running --> RetryWaiting: retryable failure
    Running --> Dead: fatal / exhausted
    RetryWaiting --> Queued: retry delay
    RetryWaiting --> Dead: exhausted
    Queued --> Cancelled: cancel
    Running --> Cancelled: cancel
    RetryWaiting --> Cancelled: cancel
```

## Architecture

```mermaid
graph TD
    P1[Producer] -->|submit| CH[Bounded Channel]
    P2[Producer] -->|try_submit| CH
    CH --> C[Coordinator]
    C --> S[Job Store]
    C --> Q[Queue State]
    C --> R[Ready Queue]
    C --> L[Leases]
    C --> RT[Retry Scheduler]
    C --> PW[Parked Workers]
    W1[Worker] -->|FetchWork| C
    W2[Worker] -->|FetchWork| C
    C -->|LeasedJob| W1
    C -->|LeasedJob| W2
    W1 -->|Outcome| C
    W2 -->|Outcome| C
```

The coordinator task exclusively owns mutable queue state. Workers are parked
when idle (no polling) and woken immediately when work is available. Jobs are
scheduled round-robin across named queues (FIFO within each queue).

## Retries

Jobs that fail with a retryable error are scheduled for retry automatically:

- Exponential backoff: `cap = min(max_delay, base_delay * 2^(attempt-1))`
- Jitter modes:
  - `Jitter::None`: delay equals the cap
  - `Jitter::Full`: delay sampled uniformly in `[0, cap]`
- `max_attempts` controls total allowed executions

Timers fire autonomously; no external activity is required.

Example: `max_attempts = 3` allows attempts 1, 2, 3 then Dead.

## Leases

Each running job has a unique lease identity (monotonic epoch). When a worker
reports an outcome, the coordinator validates the lease. Stale outcomes from
old leases (e.g., after timeout/retry) are rejected.

## Visibility Timeout

If a running job exceeds its visibility timeout:

1. The lease is invalidated (timer fires autonomously)
2. The job is scheduled for retry (if attempts remain)
3. Late results from the original worker are rejected as stale

The coordinator wakes itself at the next deadline; no polling or external commands required.

## Graceful Shutdown

Calling `runtime.shutdown()`:

1. Stops accepting new submissions
2. Closes semaphores (blocked submitters wake with ShuttingDown)
3. Stops leasing new jobs
4. Waits up to `shutdown_timeout` for running jobs to complete
5. If all running jobs finish, shutdown completes early
6. After timeout, remaining leases are cancelled
7. Late outcomes from cancelled jobs are rejected as stale

Cancellation is cooperative: handlers should check `ctx.is_cancelled()` for
timely exit. Async tasks can be aborted, but external side effects may already
have occurred.

## Durability

WAL mode provides durable transition writes:

```rust
let config = RuntimeConfig::new(queues, 64)
    .with_storage(StorageConfig::Wal { path: "data".into() });
```

In WAL mode, all state transitions follow persist-before-apply ordering:

1. Build WAL record
2. Append to WAL file
3. `sync_data()` to disk
4. Apply to in-memory state
5. Expose result to caller

This ensures:
- Workers never execute before their lease is durable
- Capacity is never released before terminal state is durable
- Memory state is never mutated before successful persistence

### Crash Recovery

On startup with an existing WAL, the runtime:

1. Validates the WAL header
2. Scans and validates all records (CRC, sequence, payload)
3. Repairs truncated final frames from crashes
4. Replays records to rebuild in-memory state
5. Reconciles crash-lost Running jobs (re-schedules or marks Dead)
6. Rebuilds permits, retry timers, and ready queues
7. Continues WAL sequence numbers correctly
8. Starts workers only after recovery completes

See [docs/wal-format.md](docs/wal-format.md) for WAL format details.

## Delivery Semantics

The queue provides **at-least-once** execution.

A job may run more than once if:
1. The handler performs an external side effect
2. The process crashes before the completion record is durably written

On restart, the job will execute again because no durable completion exists.

**Handlers that perform external side effects should be idempotent.**

This is NOT exactly-once delivery. True exactly-once requires distributed
transactions between the queue and external systems.

## Development Status

**Current step:** WAL replay, crash recovery, and startup state reconstruction.

## Build and Test

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
```

## License

Apache-2.0
