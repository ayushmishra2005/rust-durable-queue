use crate::wal::error::{WalError, WalResult};
use crate::wal::record::WalRecord;

/// WAL file magic bytes: "RDQUEUE\0"
pub const WAL_MAGIC: [u8; 8] = *b"RDQUEUE\0";

/// Current WAL format version.
pub const WAL_VERSION: u16 = 1;

/// WAL header size in bytes.
pub const HEADER_SIZE: usize = 10; // 8 magic + 2 version

/// Frame metadata size: len(4) + record_version(1) + record_type(1) + sequence(8) + crc(4)
pub const FRAME_OVERHEAD: usize = 18;

/// Maximum record payload size (1MB).
pub const MAX_RECORD_SIZE: u32 = 1024 * 1024;

/// Record format version.
pub const RECORD_VERSION: u8 = 1;

/// WAL file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHeader {
    pub magic: [u8; 8],
    pub version: u16,
}

impl WalHeader {
    pub fn new() -> Self {
        Self {
            magic: WAL_MAGIC,
            version: WAL_VERSION,
        }
    }

    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.magic);
        buf[8..10].copy_from_slice(&self.version.to_le_bytes());
        buf
    }

    pub fn decode(buf: &[u8; HEADER_SIZE]) -> WalResult<Self> {
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if magic != WAL_MAGIC {
            return Err(WalError::InvalidWalHeader);
        }

        let version = u16::from_le_bytes([buf[8], buf[9]]);
        if version != WAL_VERSION {
            return Err(WalError::UnsupportedWalVersion(version));
        }

        Ok(Self { magic, version })
    }
}

impl Default for WalHeader {
    fn default() -> Self {
        Self::new()
    }
}

/// Frame layout:
/// - payload_len: u32 LE (4 bytes)
/// - record_version: u8 (1 byte)
/// - record_type: u8 (1 byte)
/// - sequence: u64 LE (8 bytes)
/// - payload: postcard bytes (payload_len bytes)
/// - checksum: u32 LE CRC32 of all preceding bytes (4 bytes)
#[derive(Debug, Clone)]
pub struct WalFrame {
    pub sequence: u64,
    pub record: WalRecord,
}

impl WalFrame {
    pub fn new(sequence: u64, record: WalRecord) -> Self {
        Self { sequence, record }
    }

    pub fn encode(&self) -> WalResult<Vec<u8>> {
        let payload = postcard::to_allocvec(&self.record)
            .map_err(|e| WalError::Serialization(e.to_string()))?;

        if payload.len() > MAX_RECORD_SIZE as usize {
            return Err(WalError::RecordTooLarge(payload.len()));
        }

        let payload_len = payload.len() as u32;
        let total_size = FRAME_OVERHEAD + payload.len();
        let mut buf = Vec::with_capacity(total_size);

        // payload_len
        buf.extend_from_slice(&payload_len.to_le_bytes());
        // record_version
        buf.push(RECORD_VERSION);
        // record_type
        buf.push(self.record.record_type());
        // sequence
        buf.extend_from_slice(&self.sequence.to_le_bytes());
        // payload
        buf.extend_from_slice(&payload);
        // checksum (covers everything before it)
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        Ok(buf)
    }

    /// Decode frame from bytes. Returns (frame, bytes_consumed).
    pub fn decode(buf: &[u8]) -> WalResult<(Self, usize)> {
        if buf.len() < FRAME_OVERHEAD {
            return Err(WalError::TruncatedFrame);
        }

        // Read payload_len first to validate before allocation.
        let payload_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if payload_len > MAX_RECORD_SIZE {
            return Err(WalError::RecordTooLarge(payload_len as usize));
        }

        let total_size = FRAME_OVERHEAD + payload_len as usize;
        if buf.len() < total_size {
            return Err(WalError::TruncatedFrame);
        }

        let record_version = buf[4];
        if record_version != RECORD_VERSION {
            return Err(WalError::UnsupportedRecordVersion(record_version));
        }

        let _record_type = buf[5];
        let sequence = u64::from_le_bytes([
            buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13],
        ]);

        let payload_start = 14;
        let payload_end = payload_start + payload_len as usize;
        let payload = &buf[payload_start..payload_end];

        // Verify checksum.
        let stored_crc = u32::from_le_bytes([
            buf[payload_end],
            buf[payload_end + 1],
            buf[payload_end + 2],
            buf[payload_end + 3],
        ]);
        let computed_crc = crc32fast::hash(&buf[..payload_end]);
        if stored_crc != computed_crc {
            return Err(WalError::ChecksumMismatch {
                expected: stored_crc,
                computed: computed_crc,
            });
        }

        let record: WalRecord =
            postcard::from_bytes(payload).map_err(|e| WalError::Serialization(e.to_string()))?;

        Ok((Self { sequence, record }, total_size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{JobId, JobSpec, QueueName, UnixMillis};

    #[test]
    fn header_roundtrip() {
        let header = WalHeader::new();
        let encoded = header.encode();
        let decoded = WalHeader::decode(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn bad_magic_rejected() {
        let mut buf = WalHeader::new().encode();
        buf[0] = b'X';
        assert!(matches!(
            WalHeader::decode(&buf),
            Err(WalError::InvalidWalHeader)
        ));
    }

    #[test]
    fn unsupported_version_rejected() {
        let mut buf = WalHeader::new().encode();
        buf[8] = 99;
        buf[9] = 0;
        assert!(matches!(
            WalHeader::decode(&buf),
            Err(WalError::UnsupportedWalVersion(99))
        ));
    }

    #[test]
    fn frame_roundtrip() {
        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("test").unwrap(),
            spec: JobSpec::new(b"payload"),
            created_at: UnixMillis::now(),
        };
        let frame = WalFrame::new(1, record.clone());
        let encoded = frame.encode().unwrap();
        let (decoded, consumed) = WalFrame::decode(&encoded).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.sequence, 1);
        assert_eq!(decoded.record, record);
    }

    #[test]
    fn crc_validation() {
        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("test").unwrap(),
            spec: JobSpec::new(b"payload"),
            created_at: UnixMillis::now(),
        };
        let frame = WalFrame::new(1, record);
        let mut encoded = frame.encode().unwrap();
        // Corrupt payload byte.
        encoded[15] ^= 0xFF;
        assert!(matches!(
            WalFrame::decode(&encoded),
            Err(WalError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn truncated_frame_rejected() {
        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("test").unwrap(),
            spec: JobSpec::new(b"payload"),
            created_at: UnixMillis::now(),
        };
        let frame = WalFrame::new(1, record);
        let encoded = frame.encode().unwrap();
        // Truncate.
        let truncated = &encoded[..encoded.len() - 5];
        assert!(matches!(
            WalFrame::decode(truncated),
            Err(WalError::TruncatedFrame)
        ));
    }

    #[test]
    fn oversized_frame_rejected() {
        let mut buf = vec![0u8; FRAME_OVERHEAD + 100];
        // Set payload_len to exceed MAX_RECORD_SIZE.
        let huge_len = MAX_RECORD_SIZE + 1;
        buf[0..4].copy_from_slice(&huge_len.to_le_bytes());
        assert!(matches!(
            WalFrame::decode(&buf),
            Err(WalError::RecordTooLarge(_))
        ));
    }

    #[test]
    fn all_record_types_roundtrip() {
        let job_id = JobId::new();
        let queue = QueueName::new("q").unwrap();
        let lease_id = crate::types::LeaseId::new(42);
        let ts = UnixMillis::now();

        let records = vec![
            WalRecord::JobSubmitted {
                id: job_id,
                queue: queue.clone(),
                spec: JobSpec::new(b"test"),
                created_at: ts,
            },
            WalRecord::JobLeased {
                id: job_id,
                lease_id,
                attempt: 1,
                leased_at: ts,
            },
            WalRecord::JobRetryScheduled {
                id: job_id,
                lease_id,
                attempt: 1,
                available_at: ts,
            },
            WalRecord::JobCompleted {
                id: job_id,
                lease_id,
                completed_at: ts,
            },
            WalRecord::JobDead {
                id: job_id,
                lease_id,
                dead_at: ts,
            },
            WalRecord::JobCancelled {
                id: job_id,
                cancelled_at: ts,
            },
        ];

        for (seq, record) in records.into_iter().enumerate() {
            let frame = WalFrame::new(seq as u64 + 1, record.clone());
            let encoded = frame.encode().unwrap();
            let (decoded, _) = WalFrame::decode(&encoded).unwrap();
            assert_eq!(decoded.record, record);
        }
    }
}
