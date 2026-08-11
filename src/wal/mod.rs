mod codec;
mod error;
mod record;
pub mod scanner;
mod writer;

pub use codec::{
    FRAME_OVERHEAD, HEADER_SIZE, MAX_RECORD_SIZE, RECORD_VERSION, WAL_MAGIC, WAL_VERSION, WalFrame,
    WalHeader,
};
pub use error::{WalError, WalResult};
pub use record::WalRecord;
pub use scanner::{ScanResult, repair_truncated_tail, scan_wal};
pub use writer::{TestableWalWriter, WalWriter};
