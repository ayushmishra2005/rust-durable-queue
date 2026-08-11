use crate::wal::codec::{HEADER_SIZE, WalFrame, WalHeader};
use crate::wal::error::{WalError, WalResult};
use crate::wal::record::WalRecord;
use fs4::fs_std::FileExt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use tokio::sync::{mpsc, oneshot};

const WAL_FILE: &str = "wal.log";
const LOCK_FILE: &str = "LOCK";
const WRITER_CHANNEL_SIZE: usize = 64;

/// Request sent to the WAL writer thread.
struct WriteRequest {
    record: WalRecord,
    reply: oneshot::Sender<WalResult<u64>>,
}

/// Handle for communicating with the WAL writer thread.
pub struct WalWriter {
    tx: mpsc::Sender<WriteRequest>,
    _handle: JoinHandle<()>,
}

impl WalWriter {
    /// Open or create WAL at the given directory path.
    pub fn open(path: impl AsRef<Path>) -> WalResult<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;

        let lock_path = path.join(LOCK_FILE);
        let wal_path = path.join(WAL_FILE);

        // Acquire exclusive lock.
        let lock_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| WalError::StoreLocked)?;

        // Open or create WAL file.
        let (file, next_seq) = Self::open_wal_file(&wal_path)?;

        let (tx, rx) = mpsc::channel(WRITER_CHANNEL_SIZE);
        let handle = thread::spawn(move || {
            WriterThread::new(file, lock_file, next_seq).run(rx);
        });

        Ok(Self {
            tx,
            _handle: handle,
        })
    }

    /// Create writer from pre-recovered state (used after recovery).
    pub fn from_recovered(wal_file: File, lock_file: File, next_seq: u64) -> WalResult<Self> {
        let (tx, rx) = mpsc::channel(WRITER_CHANNEL_SIZE);
        let handle = thread::spawn(move || {
            WriterThread::new(wal_file, lock_file, next_seq).run(rx);
        });

        Ok(Self {
            tx,
            _handle: handle,
        })
    }

    fn open_wal_file(path: &Path) -> WalResult<(File, u64)> {
        let exists = path.exists();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        if exists {
            // Read and validate header.
            let mut header_buf = [0u8; HEADER_SIZE];
            let n = file.read(&mut header_buf)?;
            if n == 0 {
                // Empty file, write header.
                let header = WalHeader::new();
                file.write_all(&header.encode())?;
                file.sync_data()?;
                return Ok((file, 1));
            }
            if n < HEADER_SIZE {
                return Err(WalError::InvalidWalHeader);
            }
            WalHeader::decode(&header_buf)?;

            // Scan to find next sequence number (for future replay).
            let next_seq = Self::scan_for_next_sequence(&mut file)?;
            Ok((file, next_seq))
        } else {
            // New file, write header.
            let header = WalHeader::new();
            file.write_all(&header.encode())?;
            file.sync_data()?;
            Ok((file, 1))
        }
    }

    fn scan_for_next_sequence(file: &mut File) -> WalResult<u64> {
        use std::io::{Seek, SeekFrom};

        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;

        if buf.is_empty() {
            return Ok(1);
        }

        let mut offset = 0;
        let mut max_seq = 0u64;

        while offset < buf.len() {
            match WalFrame::decode(&buf[offset..]) {
                Ok((frame, consumed)) => {
                    max_seq = max_seq.max(frame.sequence);
                    offset += consumed;
                }
                Err(WalError::TruncatedFrame) => {
                    break;
                }
                Err(e) => return Err(e),
            }
        }

        // Seek to end for appending.
        file.seek(SeekFrom::End(0))?;
        Ok(max_seq + 1)
    }

    /// Append a record and wait for durable acknowledgment.
    pub async fn append(&self, record: WalRecord) -> WalResult<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(WriteRequest { record, reply: tx })
            .await
            .map_err(|_| WalError::WriterChannelClosed)?;
        rx.await.map_err(|_| WalError::WriterChannelClosed)?
    }
}

struct WriterThread {
    file: File,
    #[allow(dead_code)]
    lock_file: File, // Kept open to hold lock.
    next_seq: u64,
    fail_next_append: Arc<AtomicBool>,
    fail_next_sync: Arc<AtomicBool>,
}

impl WriterThread {
    fn new(file: File, lock_file: File, next_seq: u64) -> Self {
        Self {
            file,
            lock_file,
            next_seq,
            fail_next_append: Arc::new(AtomicBool::new(false)),
            fail_next_sync: Arc::new(AtomicBool::new(false)),
        }
    }

    fn run(mut self, mut rx: mpsc::Receiver<WriteRequest>) {
        while let Some(req) = rx.blocking_recv() {
            let result = self.process_write(req.record);
            let _ = req.reply.send(result);
        }
    }

    fn process_write(&mut self, record: WalRecord) -> WalResult<u64> {
        // Fault injection.
        if self.fail_next_append.swap(false, Ordering::SeqCst) {
            return Err(WalError::Io(std::io::Error::other(
                "injected append failure",
            )));
        }

        let seq = self.next_seq;
        let frame = WalFrame::new(seq, record);
        let encoded = frame.encode()?;

        self.file.write_all(&encoded)?;

        // Fault injection.
        if self.fail_next_sync.swap(false, Ordering::SeqCst) {
            return Err(WalError::SyncFailed);
        }

        self.file.sync_data()?;
        self.next_seq += 1;

        Ok(seq)
    }
}

/// WAL writer with fault injection for testing.
pub struct TestableWalWriter {
    inner: WalWriter,
    fail_next_append: Arc<AtomicBool>,
    fail_next_sync: Arc<AtomicBool>,
}

impl TestableWalWriter {
    pub fn open(path: impl AsRef<Path>) -> WalResult<Self> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;

        let lock_path = path.join(LOCK_FILE);
        let wal_path = path.join(WAL_FILE);

        let lock_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| WalError::StoreLocked)?;

        let (file, next_seq) = WalWriter::open_wal_file(&wal_path)?;

        let fail_next_append = Arc::new(AtomicBool::new(false));
        let fail_next_sync = Arc::new(AtomicBool::new(false));

        let (tx, rx) = mpsc::channel(WRITER_CHANNEL_SIZE);
        let fail_append = fail_next_append.clone();
        let fail_sync = fail_next_sync.clone();
        let handle = thread::spawn(move || {
            let mut thread = WriterThread::new(file, lock_file, next_seq);
            thread.fail_next_append = fail_append;
            thread.fail_next_sync = fail_sync;
            thread.run(rx);
        });

        Ok(Self {
            inner: WalWriter {
                tx,
                _handle: handle,
            },
            fail_next_append,
            fail_next_sync,
        })
    }

    pub async fn append(&self, record: WalRecord) -> WalResult<u64> {
        self.inner.append(record).await
    }

    pub fn fail_next_append(&self) {
        self.fail_next_append.store(true, Ordering::SeqCst);
    }

    pub fn fail_next_sync(&self) {
        self.fail_next_sync.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{JobId, JobSpec, QueueName, UnixMillis};
    use tempfile::TempDir;

    #[tokio::test]
    async fn writer_creates_wal_file() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path()).unwrap();

        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("test").unwrap(),
            spec: JobSpec::new(b"payload"),
            created_at: UnixMillis::now(),
        };

        let seq = writer.append(record).await.unwrap();
        assert_eq!(seq, 1);

        assert!(dir.path().join(WAL_FILE).exists());
        assert!(dir.path().join(LOCK_FILE).exists());
    }

    #[tokio::test]
    async fn sequence_increments() {
        let dir = TempDir::new().unwrap();
        let writer = WalWriter::open(dir.path()).unwrap();

        for i in 1..=5 {
            let record = WalRecord::JobSubmitted {
                id: JobId::new(),
                queue: QueueName::new("test").unwrap(),
                spec: JobSpec::new(b"payload"),
                created_at: UnixMillis::now(),
            };
            let seq = writer.append(record).await.unwrap();
            assert_eq!(seq, i);
        }
    }

    #[test]
    fn exclusive_lock_prevents_second_writer() {
        let dir = TempDir::new().unwrap();
        let _writer1 = WalWriter::open(dir.path()).unwrap();
        let result = WalWriter::open(dir.path());
        assert!(matches!(result, Err(WalError::StoreLocked)));
    }

    #[tokio::test]
    async fn fault_injection_append() {
        let dir = TempDir::new().unwrap();
        let writer = TestableWalWriter::open(dir.path()).unwrap();

        writer.fail_next_append();

        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("test").unwrap(),
            spec: JobSpec::new(b"payload"),
            created_at: UnixMillis::now(),
        };

        let result = writer.append(record).await;
        assert!(matches!(result, Err(WalError::Io(_))));
    }

    #[tokio::test]
    async fn fault_injection_sync() {
        let dir = TempDir::new().unwrap();
        let writer = TestableWalWriter::open(dir.path()).unwrap();

        writer.fail_next_sync();

        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("test").unwrap(),
            spec: JobSpec::new(b"payload"),
            created_at: UnixMillis::now(),
        };

        let result = writer.append(record).await;
        assert!(matches!(result, Err(WalError::SyncFailed)));
    }

    #[tokio::test]
    async fn reopen_continues_sequence() {
        let dir = TempDir::new().unwrap();

        {
            let writer = WalWriter::open(dir.path()).unwrap();
            for _ in 0..3 {
                let record = WalRecord::JobSubmitted {
                    id: JobId::new(),
                    queue: QueueName::new("test").unwrap(),
                    spec: JobSpec::new(b"data"),
                    created_at: UnixMillis::now(),
                };
                writer.append(record).await.unwrap();
            }
        }

        {
            let writer = WalWriter::open(dir.path()).unwrap();
            let record = WalRecord::JobSubmitted {
                id: JobId::new(),
                queue: QueueName::new("test").unwrap(),
                spec: JobSpec::new(b"data"),
                created_at: UnixMillis::now(),
            };
            let seq = writer.append(record).await.unwrap();
            assert_eq!(seq, 4);
        }
    }
}
