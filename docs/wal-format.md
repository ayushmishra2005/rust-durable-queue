# WAL Format

This document describes the Write-Ahead Log (WAL) format used for durability.

## File Layout

```
data/
    LOCK        # Exclusive writer lock file
    wal.log     # Append-only WAL file
```

## File Header

The WAL file begins with a fixed 10-byte header:

| Offset | Size | Field   | Description                    |
|--------|------|---------|--------------------------------|
| 0      | 8    | magic   | `RDQUEUE\0` (8 bytes)          |
| 8      | 2    | version | Format version (u16 LE), currently `1` |

Invalid magic bytes cause `InvalidWalHeader` error.
Unsupported versions cause `UnsupportedWalVersion` error.

## Record Frame

Each record follows the header as a framed entry:

| Offset | Size      | Field          | Description                              |
|--------|-----------|----------------|------------------------------------------|
| 0      | 4         | payload_len    | Payload size in bytes (u32 LE)           |
| 4      | 1         | record_version | Record format version (u8), currently `1`|
| 5      | 1         | record_type    | Record type discriminant (u8)            |
| 6      | 8         | sequence       | Monotonically increasing sequence (u64 LE)|
| 14     | N         | payload        | Postcard-serialized record data          |
| 14+N   | 4         | checksum       | CRC32 of bytes 0..(14+N) (u32 LE)        |

Total frame size: 18 + payload_len bytes (FRAME_OVERHEAD = 18).

### Maximum Record Size

`MAX_RECORD_SIZE = 1,048,576` bytes (1MB).

Frames with `payload_len > MAX_RECORD_SIZE` are rejected before allocation.
This prevents memory exhaustion from corrupted or malicious length fields.

### Record Types

| Type | Value | Description              |
|------|-------|--------------------------|
| JobSubmitted      | 1 | New job created         |
| JobLeased         | 2 | Job leased to worker    |
| JobRetryScheduled | 3 | Retry scheduled         |
| JobCompleted      | 4 | Job completed           |
| JobDead           | 5 | Job exhausted retries   |
| JobCancelled      | 6 | Job cancelled           |

### Sequence Numbers

- Start at 1 for a new WAL
- Increment by 1 for each record
- Monotonically increasing
- Used for ordering and future integrity checks

### Checksum

- Algorithm: CRC32 (via `crc32fast`)
- Coverage: All bytes from offset 0 through end of payload (before checksum)
- Purpose: Accidental corruption detection, not cryptographic integrity

## Durability Ordering

All durable state transitions follow this order:

```
VALIDATE -> BUILD RECORD -> APPEND WAL -> SYNC -> APPLY TO MEMORY -> EXPOSE RESULT
```

This ensures:
- Memory is never mutated before successful persistence
- Workers never see uncommitted state
- Capacity is never released before terminal state is durable

### Submit

1. Validate queue exists and payload size
2. Generate JobId, capture wall-clock created_at
3. Build JobSubmitted record
4. Append + sync_data()
5. Apply to in-memory store
6. Add to ready queue, update stats, hold permit

### Lease

1. Select job from ready queue
2. Calculate attempt, generate lease_id, capture leased_at
3. Build JobLeased record
4. Append + sync_data()
5. Apply to memory (mark Running, set attempts)
6. Setup lease tracking (deadline, cancellation token)
7. Return to worker

Handler execution begins only after durable lease.

### Completion/Dead/Cancelled

1. Validate lease is current
2. Capture wall-clock timestamp
3. Build terminal record (JobCompleted/JobDead/JobCancelled)
4. Append + sync_data()
5. Apply to memory
6. Release capacity permit, update stats

### Retry

1. Validate lease, calculate delay with jitter
2. Compute wall-clock available_at (persisted)
3. Build JobRetryScheduled record
4. Append + sync_data()
5. Apply to memory (mark RetryWaiting)
6. Derive monotonic Instant from persisted time
7. Schedule retry timer

The jittered available_at is persisted to avoid re-sampling on recovery.

## Time Handling

- **UnixMillis(i64)**: Wall-clock time persisted in WAL records
- **tokio::time::Instant**: Runtime monotonic time for scheduling

Wall-clock time is used for:
- created_at (job submission)
- leased_at (lease acquisition)
- available_at (retry scheduling)
- completed_at, dead_at, cancelled_at (terminal states)

Monotonic time is used for:
- In-memory lease deadlines
- Retry timers
- Shutdown timeouts

This separation ensures:
- WAL records contain portable timestamps
- Runtime scheduling uses appropriate monotonic clocks
- No Instant or Tokio types are serialized

## WAL Writer

- Single writer thread owns `std::fs::File`
- Coordinator sends records via bounded channel (64 slots)
- Each append: `write_all(frame)` then `sync_data()` before ack
- No group commit (correctness over throughput)

## Exclusive Lock

The `LOCK` file uses `fs4::try_lock_exclusive()` to prevent multiple
runtime instances from writing to the same WAL directory.

`StoreLocked` error returned if lock acquisition fails.

## Recovery

On startup with an existing WAL, the runtime performs sequential recovery.

### Recovery Steps

1. **Acquire exclusive lock** - Prevents multiple instances
2. **Validate header** - Bad magic or unsupported version is a fatal error
3. **Scan records** - Read all frames, validate CRC, decode payloads
4. **Detect truncated tail** - EOF mid-frame is treated as crash truncation
5. **Repair if needed** - Truncate file to last valid frame boundary, sync
6. **Replay records** - Apply each record to rebuild in-memory state
7. **Reconcile Running jobs** - Crash-lost leases are durably re-scheduled
8. **Rebuild runtime state** - Permits, retry timers, ready queues, stats
9. **Continue sequence** - Writer starts at `last_sequence + 1`
10. **Start workers** - Only after recovery is complete

### Corruption vs Truncation

| Condition | Recovery Behavior |
|-----------|-------------------|
| Partial final frame (EOF mid-read) | Truncate to last good offset, continue |
| Complete frame with bad CRC | Fatal startup error |
| Bad postcard payload in complete frame | Fatal startup error |
| Sequence gap (e.g., 1,2,4) | Fatal startup error |
| Sequence duplicate (e.g., 1,2,2) | Fatal startup error |
| Bad header magic | Fatal startup error |
| Unsupported WAL version | Fatal startup error |

Truncated tail recovery is safe because the incomplete frame was never
acknowledged to any caller. Corrupted complete frames indicate data loss.

### Crash-Lost Running Jobs

If the WAL ends with a job in `Running` state, the previous process died
while a lease was active. That lease is invalidated.

- **If attempts remain**: Append durable `JobRetryScheduled` with `available_at = now`
- **If attempts exhausted**: Append durable `JobDead`

These reconciliation records are synced before exposing recovered state.
This ensures another crash during recovery produces consistent behavior.

**Attempts are NOT incremented during recovery.** Attempts increment only
when a new execution actually starts.

### Sequence Continuation

After recovery, the WAL writer continues from `last_sequence + 1`.
Sequence numbers never restart at 1 for an existing WAL.
Sequence overflow (u64::MAX reached) is a fatal error.

### Lease Epoch Fencing

Recovery determines `max_seen_epoch` from all `JobLeased` records.
The runtime sets `next_lease_epoch = max_seen_epoch + 1`.
This ensures no recovered Running lease remains valid.

### Clock Behavior

Retry delays use persisted `available_at` wall-clock times.
On recovery: `remaining = max(available_at - now, 0)`.
If the system clock moved backward, retries may fire immediately.
No clock-skew compensation is performed.
