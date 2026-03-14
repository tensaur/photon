use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use photon_core::types::batch::WireBatch;
use photon_core::types::sequence::{SegmentIndex, SequenceNumber};

use crate::domain::ports::wal::WalError;

const MAGIC: &[u8; 4] = b"PHTN";
const HEADER_SIZE: usize = 36;
const CRC_SIZE: usize = 4;
pub(crate) const RECORD_OVERHEAD: usize = HEADER_SIZE + CRC_SIZE;

const OFF_MAGIC: usize = 0;
const OFF_PAYLOAD_LEN: usize = 4;
const OFF_SEQUENCE: usize = 8;
const OFF_BATCH_CRC: usize = 16;
const OFF_CREATED_AT: usize = 20;
const OFF_POINT_COUNT: usize = 28;
const OFF_UNCOMPRESSED: usize = 32;

pub trait SegmentPhase {}

pub struct Active;
impl SegmentPhase for Active {}

pub struct Sealed;
impl SegmentPhase for Sealed {}

pub struct Acked;
impl SegmentPhase for Acked {}

pub struct Segment<S: SegmentPhase> {
    path: PathBuf,
    index: SegmentIndex,
    file: File,
    last_sequence: Option<SequenceNumber>,
    write_offset: u64,
    capacity: u64,
    _phase: PhantomData<S>,
}

#[derive(Clone, Debug)]
pub struct WalRecord {
    pub sequence_number: SequenceNumber,
    pub compressed_payload: bytes::Bytes,
    pub batch_crc32: u32,
    pub created_at: SystemTime,
    pub point_count: usize,
    pub uncompressed_size: usize,
}

impl Segment<Active> {
    pub fn create(dir: &Path, index: SegmentIndex, capacity: u64) -> Result<Self, WalError> {
        let path = segment_path(dir, index);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;

        Ok(Self {
            path,
            index,
            file,
            last_sequence: None,
            write_offset: 0,
            capacity,
            _phase: PhantomData,
        })
    }

    /// Reopen an existing segment for crash recovery.
    pub fn open_for_recovery(
        dir: &Path,
        index: SegmentIndex,
        capacity: u64,
    ) -> Result<Self, WalError> {
        let path = segment_path(dir, index);
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        let mut seg = Self {
            path,
            index,
            file,
            last_sequence: None,
            write_offset: 0,
            capacity,
            _phase: PhantomData,
        };

        seg.scan_valid_extent()?;
        Ok(seg)
    }

    pub fn append(&mut self, batch: &WireBatch, rotation_threshold: f64) -> Result<bool, WalError> {
        let payload = &batch.compressed_payload;
        let total = RECORD_OVERHEAD as u64 + payload.len() as u64;

        if self.write_offset + total > self.capacity {
            return Err(WalError::DiskFull {
                budget: self.capacity,
                used: self.write_offset,
            });
        }

        let mut header = [0u8; HEADER_SIZE];
        header[OFF_MAGIC..OFF_PAYLOAD_LEN].copy_from_slice(MAGIC);
        header[OFF_PAYLOAD_LEN..OFF_SEQUENCE]
            .copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[OFF_SEQUENCE..OFF_BATCH_CRC]
            .copy_from_slice(&u64::from(batch.sequence_number).to_le_bytes());
        header[OFF_BATCH_CRC..OFF_CREATED_AT].copy_from_slice(&batch.crc32.to_le_bytes());
        header[OFF_CREATED_AT..OFF_POINT_COUNT]
            .copy_from_slice(&to_epoch_ms(batch.created_at).to_le_bytes());
        header[OFF_POINT_COUNT..OFF_UNCOMPRESSED]
            .copy_from_slice(&(batch.point_count as u32).to_le_bytes());
        header[OFF_UNCOMPRESSED..HEADER_SIZE]
            .copy_from_slice(&(batch.uncompressed_size as u32).to_le_bytes());

        // CRC covers header and payload
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header);
        hasher.update(payload);
        let record_crc = hasher.finalize();

        // Write header, payload, crc
        self.file.seek(SeekFrom::Start(self.write_offset))?;
        self.file.write_all(&header)?;
        self.file.write_all(payload)?;
        self.file.write_all(&record_crc.to_le_bytes())?;

        self.write_offset += total;
        self.last_sequence = Some(batch.sequence_number);

        Ok(self.write_offset as f64 / self.capacity as f64 > rotation_threshold)
    }

    pub fn flush(&self) -> Result<(), WalError> {
        self.file.sync_data().map_err(Into::into)
    }

    /// No explicit sync as the OS flushes page cache
    pub fn flush_async(&self) -> Result<(), WalError> {
        Ok(())
    }

    pub fn seal(self) -> Segment<Sealed> {
        self.transition()
    }

    /// Walk from offset 0, validate each record's CRC, set write_offset
    /// after the last valid one. Truncate any trailing partial record.
    fn scan_valid_extent(&mut self) -> Result<(), WalError> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&self.file);
        let mut offset = 0u64;

        loop {
            let mut header = [0u8; HEADER_SIZE];

            if reader.read_exact(&mut header).is_err()
                || &header[OFF_MAGIC..OFF_PAYLOAD_LEN] != MAGIC
            {
                break;
            }

            let payload_len =
                u32::from_le_bytes(header[OFF_PAYLOAD_LEN..OFF_SEQUENCE].try_into().unwrap())
                    as usize;
            let seq = SequenceNumber::from(u64::from_le_bytes(
                header[OFF_SEQUENCE..OFF_BATCH_CRC].try_into().unwrap(),
            ));
            let total = RECORD_OVERHEAD as u64 + payload_len as u64;

            let mut payload = vec![0u8; payload_len];
            if reader.read_exact(&mut payload).is_err() {
                break;
            }

            let mut crc_buf = [0u8; 4];
            if reader.read_exact(&mut crc_buf).is_err() {
                break;
            }

            let stored_crc = u32::from_le_bytes(crc_buf);

            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&header);
            hasher.update(&payload);

            if hasher.finalize() != stored_crc {
                tracing::warn!(segment = %self.index, offset, "CRC mismatch - truncating");
                break;
            }

            self.last_sequence = Some(seq);
            offset += total;
        }

        // Truncate file at the last valid record boundary
        self.file.set_len(offset)?;
        self.write_offset = offset;
        Ok(())
    }
}

impl Segment<Sealed> {
    pub fn mark_acked(self) -> Segment<Acked> {
        self.transition()
    }
}

impl Segment<Acked> {
    pub fn delete(self) -> io::Result<()> {
        let path = self.path.clone();
        drop(self.file);
        fs::remove_file(&path)
    }
}

impl<S: SegmentPhase> Segment<S> {
    pub fn index(&self) -> SegmentIndex {
        self.index
    }
    pub fn last_sequence(&self) -> Option<SequenceNumber> {
        self.last_sequence
    }
    pub fn bytes_used(&self) -> u64 {
        self.write_offset
    }

    /// Read all valid records from the segment.
    pub fn read_records(&self) -> Result<Vec<WalRecord>, WalError> {
        let mut reader = BufReader::new(&self.file);
        reader.seek(SeekFrom::Start(0))?;

        let mut records = Vec::new();
        let mut offset = 0u64;

        while offset < self.write_offset {
            let mut header = [0u8; HEADER_SIZE];
            if reader.read_exact(&mut header).is_err() {
                break;
            }
            if &header[OFF_MAGIC..OFF_PAYLOAD_LEN] != MAGIC {
                break;
            }

            let payload_len =
                u32::from_le_bytes(header[OFF_PAYLOAD_LEN..OFF_SEQUENCE].try_into().unwrap())
                    as usize;

            let mut payload = vec![0u8; payload_len];
            if reader.read_exact(&mut payload).is_err() {
                break;
            }

            // Skip the record CRC - we validated it during recovery.
            let mut crc_buf = [0u8; 4];
            if reader.read_exact(&mut crc_buf).is_err() {
                break;
            }

            records.push(WalRecord {
                sequence_number: SequenceNumber::from(u64::from_le_bytes(
                    header[OFF_SEQUENCE..OFF_BATCH_CRC].try_into().unwrap(),
                )),
                compressed_payload: payload.into(),
                batch_crc32: u32::from_le_bytes(
                    header[OFF_BATCH_CRC..OFF_CREATED_AT].try_into().unwrap(),
                ),
                created_at: from_epoch_ms(u64::from_le_bytes(
                    header[OFF_CREATED_AT..OFF_POINT_COUNT].try_into().unwrap(),
                )),
                point_count: u32::from_le_bytes(
                    header[OFF_POINT_COUNT..OFF_UNCOMPRESSED]
                        .try_into()
                        .unwrap(),
                ) as usize,
                uncompressed_size: u32::from_le_bytes(
                    header[OFF_UNCOMPRESSED..HEADER_SIZE].try_into().unwrap(),
                ) as usize,
            });

            offset += RECORD_OVERHEAD as u64 + payload_len as u64;
        }

        Ok(records)
    }

    /// Read the first record with sequence > `after`, returning early.
    pub fn read_first_record_after(
        &self,
        after: SequenceNumber,
    ) -> Result<Option<WalRecord>, WalError> {
        let mut reader = BufReader::new(&self.file);
        reader.seek(SeekFrom::Start(0))?;

        let mut offset = 0u64;

        while offset < self.write_offset {
            let mut header = [0u8; HEADER_SIZE];
            if reader.read_exact(&mut header).is_err() {
                break;
            }
            if &header[OFF_MAGIC..OFF_PAYLOAD_LEN] != MAGIC {
                break;
            }

            let payload_len =
                u32::from_le_bytes(header[OFF_PAYLOAD_LEN..OFF_SEQUENCE].try_into().unwrap())
                    as usize;
            let seq = SequenceNumber::from(u64::from_le_bytes(
                header[OFF_SEQUENCE..OFF_BATCH_CRC].try_into().unwrap(),
            ));

            if seq <= after {
                let skip = payload_len as u64 + CRC_SIZE as u64;
                reader.seek(SeekFrom::Current(skip as i64))?;
                offset += RECORD_OVERHEAD as u64 + payload_len as u64;
                continue;
            }

            let mut payload = vec![0u8; payload_len];
            if reader.read_exact(&mut payload).is_err() {
                break;
            }

            let mut crc_buf = [0u8; 4];
            let _ = reader.read_exact(&mut crc_buf);

            return Ok(Some(WalRecord {
                sequence_number: seq,
                compressed_payload: payload.into(),
                batch_crc32: u32::from_le_bytes(
                    header[OFF_BATCH_CRC..OFF_CREATED_AT].try_into().unwrap(),
                ),
                created_at: from_epoch_ms(u64::from_le_bytes(
                    header[OFF_CREATED_AT..OFF_POINT_COUNT].try_into().unwrap(),
                )),
                point_count: u32::from_le_bytes(
                    header[OFF_POINT_COUNT..OFF_UNCOMPRESSED]
                        .try_into()
                        .unwrap(),
                ) as usize,
                uncompressed_size: u32::from_le_bytes(
                    header[OFF_UNCOMPRESSED..HEADER_SIZE].try_into().unwrap(),
                ) as usize,
            }));
        }

        Ok(None)
    }

    fn transition<N: SegmentPhase>(self) -> Segment<N> {
        Segment {
            path: self.path,
            index: self.index,
            file: self.file,
            last_sequence: self.last_sequence,
            write_offset: self.write_offset,
            capacity: self.capacity,
            _phase: PhantomData,
        }
    }
}

fn to_epoch_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

fn from_epoch_ms(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

pub(crate) fn segment_path(dir: &Path, index: SegmentIndex) -> PathBuf {
    dir.join(format!("segment-{index}.wal"))
}

pub(crate) fn list_segments(dir: &Path) -> io::Result<Vec<(SegmentIndex, PathBuf)>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(idx) = name
            .strip_prefix("segment-")
            .and_then(|s| s.strip_suffix(".wal"))
            .and_then(|s| s.parse::<u64>().ok())
        {
            out.push((SegmentIndex::from(idx), entry.path()));
        }
    }
    out.sort_by_key(|(idx, _)| *idx);
    Ok(out)
}
