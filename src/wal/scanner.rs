//! WAL scanner for sequential frame reading and validation.

use crate::wal::codec::{FRAME_OVERHEAD, HEADER_SIZE, MAX_RECORD_SIZE, WalFrame, WalHeader};
use crate::wal::error::{WalError, WalResult};
use crate::wal::record::WalRecord;
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Result of scanning a WAL file.
#[derive(Debug)]
pub struct ScanResult {
    /// Decoded records in order.
    pub records: Vec<WalRecord>,
    /// Last valid sequence number seen (0 if empty).
    pub last_sequence: u64,
    /// Maximum lease epoch seen (0 if none).
    pub max_lease_epoch: u64,
    /// Byte offset of last valid frame end (after header).
    pub last_valid_offset: u64,
    /// Whether the final frame was truncated (crash tail).
    pub had_truncated_tail: bool,
}

/// Scan a WAL file and return decoded records with metadata.
/// Does NOT modify the file.
pub fn scan_wal(path: &Path) -> WalResult<ScanResult> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();

    if file_len == 0 {
        return Ok(ScanResult {
            records: Vec::new(),
            last_sequence: 0,
            max_lease_epoch: 0,
            last_valid_offset: 0,
            had_truncated_tail: false,
        });
    }

    // Read and validate header.
    if file_len < HEADER_SIZE as u64 {
        return Err(WalError::InvalidWalHeader);
    }

    let mut header_buf = [0u8; HEADER_SIZE];
    file.read_exact(&mut header_buf)?;
    WalHeader::decode(&header_buf)?;

    // Read remaining content.
    let mut content = Vec::new();
    file.read_to_end(&mut content)?;

    scan_frames(&content)
}

/// Scan frames from buffer (content after header).
fn scan_frames(buf: &[u8]) -> WalResult<ScanResult> {
    let mut records = Vec::new();
    let mut last_sequence = 0u64;
    let mut max_lease_epoch = 0u64;
    let mut offset = 0usize;
    let mut last_valid_offset = 0u64;
    let mut had_truncated_tail = false;
    let mut expected_seq = 1u64;

    while offset < buf.len() {
        let remaining = &buf[offset..];

        // Check if we have enough bytes for minimum frame metadata.
        if remaining.len() < 4 {
            // Not enough for payload_len - truncated tail.
            had_truncated_tail = true;
            break;
        }

        // Read payload_len to validate before allocation.
        let payload_len =
            u32::from_le_bytes([remaining[0], remaining[1], remaining[2], remaining[3]]);

        if payload_len > MAX_RECORD_SIZE {
            // Corrupted length in complete-looking frame is an error.
            return Err(WalError::RecordTooLarge(payload_len as usize));
        }

        let total_frame_size = FRAME_OVERHEAD + payload_len as usize;

        if remaining.len() < total_frame_size {
            // Truncated frame - crash tail.
            had_truncated_tail = true;
            break;
        }

        // Try to decode the complete frame.
        match WalFrame::decode(remaining) {
            Ok((frame, consumed)) => {
                // Validate sequence.
                if frame.sequence != expected_seq {
                    return Err(WalError::SequenceViolation {
                        expected: expected_seq,
                        got: frame.sequence,
                    });
                }

                // Track max lease epoch.
                if let WalRecord::JobLeased { lease_id, .. } = &frame.record {
                    max_lease_epoch = max_lease_epoch.max(lease_id.epoch());
                }

                last_sequence = frame.sequence;
                expected_seq = expected_seq
                    .checked_add(1)
                    .ok_or(WalError::SequenceViolation {
                        expected: u64::MAX,
                        got: 0,
                    })?;

                records.push(frame.record);
                offset += consumed;
                last_valid_offset = offset as u64;
            }
            Err(WalError::TruncatedFrame) => {
                // Truncated tail.
                had_truncated_tail = true;
                break;
            }
            Err(e) => {
                // Any other error (checksum, serialization, etc.) is corruption.
                return Err(e);
            }
        }
    }

    Ok(ScanResult {
        records,
        last_sequence,
        max_lease_epoch,
        last_valid_offset,
        had_truncated_tail,
    })
}

/// Repair a truncated WAL by truncating to last valid frame boundary.
/// Returns true if repair was performed.
pub fn repair_truncated_tail(path: &Path, last_valid_offset: u64) -> WalResult<bool> {
    let file_len = std::fs::metadata(path)?.len();
    let expected_len = HEADER_SIZE as u64 + last_valid_offset;

    if file_len <= expected_len {
        return Ok(false);
    }

    // Truncate file to last valid offset + header.
    let file = std::fs::OpenOptions::new().write(true).open(path)?;
    file.set_len(expected_len)?;
    file.sync_all()?;

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{JobId, JobSpec, LeaseId, QueueName, UnixMillis};
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_wal(dir: &TempDir, records: &[WalRecord]) -> std::path::PathBuf {
        let path = dir.path().join("wal.log");
        let mut file = File::create(&path).unwrap();

        // Write header.
        file.write_all(&WalHeader::new().encode()).unwrap();

        // Write frames.
        for (i, record) in records.iter().enumerate() {
            let frame = WalFrame::new(i as u64 + 1, record.clone());
            file.write_all(&frame.encode().unwrap()).unwrap();
        }

        file.sync_all().unwrap();
        path
    }

    #[test]
    fn scan_empty_wal() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wal.log");
        let mut file = File::create(&path).unwrap();
        file.write_all(&WalHeader::new().encode()).unwrap();
        file.sync_all().unwrap();

        let result = scan_wal(&path).unwrap();
        assert!(result.records.is_empty());
        assert_eq!(result.last_sequence, 0);
        assert!(!result.had_truncated_tail);
    }

    #[test]
    fn scan_single_record() {
        let dir = TempDir::new().unwrap();
        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("q").unwrap(),
            spec: JobSpec::new(b"test"),
            created_at: UnixMillis::now(),
        };
        let path = create_test_wal(&dir, std::slice::from_ref(&record));

        let result = scan_wal(&path).unwrap();
        assert_eq!(result.records.len(), 1);
        assert_eq!(result.records[0], record);
        assert_eq!(result.last_sequence, 1);
        assert!(!result.had_truncated_tail);
    }

    #[test]
    fn scan_multiple_records() {
        let dir = TempDir::new().unwrap();
        let job_id = JobId::new();
        let queue = QueueName::new("q").unwrap();
        let lease_id = LeaseId::new(42);

        let records = vec![
            WalRecord::JobSubmitted {
                id: job_id,
                queue: queue.clone(),
                spec: JobSpec::new(b"test"),
                created_at: UnixMillis::now(),
            },
            WalRecord::JobLeased {
                id: job_id,
                lease_id,
                attempt: 1,
                leased_at: UnixMillis::now(),
            },
            WalRecord::JobCompleted {
                id: job_id,
                lease_id,
                completed_at: UnixMillis::now(),
            },
        ];

        let path = create_test_wal(&dir, &records);
        let result = scan_wal(&path).unwrap();

        assert_eq!(result.records.len(), 3);
        assert_eq!(result.last_sequence, 3);
        assert_eq!(result.max_lease_epoch, 42);
        assert!(!result.had_truncated_tail);
    }

    #[test]
    fn scan_detects_truncated_tail() {
        let dir = TempDir::new().unwrap();
        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("q").unwrap(),
            spec: JobSpec::new(b"test"),
            created_at: UnixMillis::now(),
        };
        let path = create_test_wal(&dir, &[record]);

        // Append partial frame bytes - start of a valid frame header.
        // payload_len (small) + partial metadata.
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        // 50 bytes payload (0x32 little endian) + partial metadata
        file.write_all(&[50, 0, 0, 0, 1, 1]).unwrap();
        file.sync_all().unwrap();

        let result = scan_wal(&path).unwrap();
        assert_eq!(result.records.len(), 1);
        assert!(result.had_truncated_tail);
    }

    #[test]
    fn scan_rejects_bad_checksum() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wal.log");

        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("q").unwrap(),
            spec: JobSpec::new(b"test"),
            created_at: UnixMillis::now(),
        };

        let mut file = File::create(&path).unwrap();
        file.write_all(&WalHeader::new().encode()).unwrap();

        let frame = WalFrame::new(1, record);
        let mut encoded = frame.encode().unwrap();
        // Corrupt a byte in the middle (not truncated).
        encoded[15] ^= 0xFF;
        file.write_all(&encoded).unwrap();
        file.sync_all().unwrap();

        let result = scan_wal(&path);
        assert!(matches!(result, Err(WalError::ChecksumMismatch { .. })));
    }

    #[test]
    fn scan_rejects_sequence_gap() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("wal.log");

        let mut file = File::create(&path).unwrap();
        file.write_all(&WalHeader::new().encode()).unwrap();

        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("q").unwrap(),
            spec: JobSpec::new(b"test"),
            created_at: UnixMillis::now(),
        };

        // Write frame with sequence 1.
        let frame1 = WalFrame::new(1, record.clone());
        file.write_all(&frame1.encode().unwrap()).unwrap();

        // Write frame with sequence 3 (gap).
        let frame3 = WalFrame::new(3, record);
        file.write_all(&frame3.encode().unwrap()).unwrap();
        file.sync_all().unwrap();

        let result = scan_wal(&path);
        assert!(matches!(
            result,
            Err(WalError::SequenceViolation {
                expected: 2,
                got: 3
            })
        ));
    }

    #[test]
    fn repair_truncated_tail() {
        let dir = TempDir::new().unwrap();
        let record = WalRecord::JobSubmitted {
            id: JobId::new(),
            queue: QueueName::new("q").unwrap(),
            spec: JobSpec::new(b"test"),
            created_at: UnixMillis::now(),
        };
        let path = create_test_wal(&dir, &[record]);

        let original_len = std::fs::metadata(&path).unwrap().len();

        // Append partial frame (valid-looking but incomplete).
        // Small payload length that's valid, then partial frame data.
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        file.write_all(&[50, 0, 0, 0, 1, 1, 0, 0, 0, 0]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let scan_result = scan_wal(&path).unwrap();
        assert!(scan_result.had_truncated_tail);

        let repaired = super::repair_truncated_tail(&path, scan_result.last_valid_offset).unwrap();
        assert!(repaired);

        let new_len = std::fs::metadata(&path).unwrap().len();
        assert_eq!(new_len, original_len);

        // Verify clean scan after repair.
        let result = scan_wal(&path).unwrap();
        assert!(!result.had_truncated_tail);
        assert_eq!(result.records.len(), 1);
    }
}
