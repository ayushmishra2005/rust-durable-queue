use std::io;
use thiserror::Error;

/// WAL-specific errors.
#[derive(Debug, Error)]
pub enum WalError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("WAL directory is locked by another process")]
    StoreLocked,

    #[error("invalid WAL header")]
    InvalidWalHeader,

    #[error("unsupported WAL version: {0}")]
    UnsupportedWalVersion(u16),

    #[error("unsupported record version: {0}")]
    UnsupportedRecordVersion(u8),

    #[error("record too large: {0} bytes")]
    RecordTooLarge(usize),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("checksum mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    ChecksumMismatch { expected: u32, computed: u32 },

    #[error("sequence violation: expected {expected}, got {got}")]
    SequenceViolation { expected: u64, got: u64 },

    #[error("truncated frame")]
    TruncatedFrame,

    #[error("sync failed")]
    SyncFailed,

    #[error("WAL writer channel closed")]
    WriterChannelClosed,

    #[error("recovered job belongs to unknown queue: {0}")]
    RecoveryQueueNotFound(String),

    #[error(
        "recovered job count ({recovered}) exceeds queue capacity ({capacity}) for queue: {queue}"
    )]
    RecoveryCapacityExceeded {
        queue: String,
        recovered: usize,
        capacity: usize,
    },

    #[error("WAL sequence numbers exhausted")]
    SequenceExhausted,

    #[error("lease epoch numbers exhausted")]
    LeaseEpochExhausted,

    #[error("recovery state error: {0}")]
    RecoveryStateError(String),
}

pub type WalResult<T> = std::result::Result<T, WalError>;
