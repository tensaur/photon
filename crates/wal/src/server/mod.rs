use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::Notify;

use photon_core::types::batch::WireBatch;
use photon_core::types::config::WalSyncPolicy;
use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use crate::ports::{WalAppender, WalError};

const MAGIC: &[u8; 4] = b"PHTN";
const HEADER_SIZE: usize = 52;
const CRC_SIZE: usize = 4;
const RECORD_OVERHEAD: usize = HEADER_SIZE + CRC_SIZE;

const OFF_MAGIC: usize = 0;
const OFF_PAYLOAD_LEN: usize = 4;
const OFF_RUN_ID: usize = 8;
const OFF_SEQUENCE: usize = 24;
const OFF_BATCH_CRC: usize = 32;
const OFF_CREATED_AT: usize = 36;
const OFF_POINT_COUNT: usize = 44;
const OFF_UNCOMPRESSED: usize = 48;

const WAL_FILENAME: &str = "server.wal";
const META_FILENAME: &str = "server-wal.meta";

#[derive(Clone, Debug)]
pub struct ServerWalConfig {
    pub sync_policy: WalSyncPolicy,
}

impl Default for ServerWalConfig {
    fn default() -> Self {
        Self {
            sync_policy: WalSyncPolicy::OsManaged,
        }
    }
}

pub fn open(
    dir: impl Into<PathBuf>,
    config: ServerWalConfig,
) -> Result<(ServerWalAppender, ServerWalReader, Arc<Notify>), WalError> {
    let dir = dir.into();
    fs::create_dir_all(&dir)?;

    let wal_path = dir.join(WAL_FILENAME);
    let meta = load_meta(&dir);
    let watermarks = meta.to_watermarks();

    // Open or create the WAL file
    let write_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&wal_path)?;

    // Recover: scan for valid extent
    let write_offset = scan_valid_extent(&write_file)?;

    // Separate file handle for reading — independent seek position
    let read_file = OpenOptions::new().read(true).write(true).open(&wal_path)?;

    let notify = Arc::new(Notify::new());

    let appender = ServerWalAppender {
        file: write_file,
        write_offset,
        config,
        batches_since_sync: 0,
        notify: Arc::clone(&notify),
    };

    let reader = ServerWalReader {
        dir,
        file: read_file,
        write_offset,
        committed_offset: meta.committed_offset,
        read_offset: meta.committed_offset,
        watermarks,
    };

    Ok((appender, reader, notify))
}

pub struct ServerWalAppender {
    file: File,
    write_offset: u64,
    config: ServerWalConfig,
    batches_since_sync: u32,
    notify: Arc<Notify>,
}

impl ServerWalAppender {
    fn maybe_sync(&mut self) -> Result<(), WalError> {
        match &self.config.sync_policy {
            WalSyncPolicy::EveryBatch => {
                self.file.sync_data()?;
            }
            WalSyncPolicy::Periodic { batches, .. } => {
                if self.batches_since_sync >= *batches {
                    self.file.sync_data()?;
                    self.batches_since_sync = 0;
                }
            }
            WalSyncPolicy::OsManaged => {}
        }
        Ok(())
    }
}

impl WalAppender for ServerWalAppender {
    fn append(&mut self, batch: &WireBatch) -> Result<(), WalError> {
        let payload = &batch.compressed_payload;
        let run_uuid: uuid::Uuid = batch.run_id.into();

        let mut header = [0u8; HEADER_SIZE];
        header[OFF_MAGIC..OFF_PAYLOAD_LEN].copy_from_slice(MAGIC);
        header[OFF_PAYLOAD_LEN..OFF_RUN_ID]
            .copy_from_slice(&(payload.len() as u32).to_le_bytes());
        header[OFF_RUN_ID..OFF_SEQUENCE].copy_from_slice(run_uuid.as_bytes());
        header[OFF_SEQUENCE..OFF_BATCH_CRC]
            .copy_from_slice(&u64::from(batch.sequence_number).to_le_bytes());
        header[OFF_BATCH_CRC..OFF_CREATED_AT].copy_from_slice(&batch.crc32.to_le_bytes());
        header[OFF_CREATED_AT..OFF_POINT_COUNT]
            .copy_from_slice(&to_epoch_ms(batch.created_at).to_le_bytes());
        header[OFF_POINT_COUNT..OFF_UNCOMPRESSED]
            .copy_from_slice(&(batch.point_count as u32).to_le_bytes());
        header[OFF_UNCOMPRESSED..HEADER_SIZE]
            .copy_from_slice(&(batch.uncompressed_size as u32).to_le_bytes());

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header);
        hasher.update(payload);
        let record_crc = hasher.finalize();

        self.file.seek(SeekFrom::Start(self.write_offset))?;
        self.file.write_all(&header)?;
        self.file.write_all(payload)?;
        self.file.write_all(&record_crc.to_le_bytes())?;

        self.write_offset += RECORD_OVERHEAD as u64 + payload.len() as u64;
        self.batches_since_sync += 1;
        self.maybe_sync()?;

        self.notify.notify_one();
        Ok(())
    }
}

pub struct ServerWalReader {
    dir: PathBuf,
    file: File,
    write_offset: u64,
    committed_offset: u64,
    read_offset: u64,
    watermarks: HashMap<RunId, SequenceNumber>,
}

impl ServerWalReader {
    /// Read up to `limit` entries from the current read position.
    /// Returns empty vec if caught up.
    pub fn read_available(&mut self, limit: usize) -> Result<Vec<WireBatch>, WalError> {
        // Re-check how much has been written (appender may have advanced)
        self.write_offset = file_len(&self.file)?;

        if self.read_offset >= self.write_offset {
            return Ok(Vec::new());
        }

        let mut reader = BufReader::new(&self.file);
        reader.seek(SeekFrom::Start(self.read_offset))?;

        let mut out = Vec::new();
        let mut offset = self.read_offset;

        while offset < self.write_offset && out.len() < limit {
            let mut header = [0u8; HEADER_SIZE];
            if reader.read_exact(&mut header).is_err() {
                break;
            }
            if &header[OFF_MAGIC..OFF_PAYLOAD_LEN] != MAGIC {
                break;
            }

            let payload_len =
                u32::from_le_bytes(header[OFF_PAYLOAD_LEN..OFF_RUN_ID].try_into().unwrap())
                    as usize;

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
                break;
            }

            let run_id_bytes: [u8; 16] = header[OFF_RUN_ID..OFF_SEQUENCE].try_into().unwrap();

            out.push(WireBatch {
                run_id: RunId::from(uuid::Uuid::from_bytes(run_id_bytes)),
                sequence_number: SequenceNumber::from(u64::from_le_bytes(
                    header[OFF_SEQUENCE..OFF_BATCH_CRC].try_into().unwrap(),
                )),
                compressed_payload: payload.into(),
                crc32: u32::from_le_bytes(
                    header[OFF_BATCH_CRC..OFF_CREATED_AT].try_into().unwrap(),
                ),
                created_at: from_epoch_ms(u64::from_le_bytes(
                    header[OFF_CREATED_AT..OFF_POINT_COUNT].try_into().unwrap(),
                )),
                point_count: u32::from_le_bytes(
                    header[OFF_POINT_COUNT..OFF_UNCOMPRESSED].try_into().unwrap(),
                ) as usize,
                uncompressed_size: u32::from_le_bytes(
                    header[OFF_UNCOMPRESSED..HEADER_SIZE].try_into().unwrap(),
                ) as usize,
            });

            offset += RECORD_OVERHEAD as u64 + payload_len as u64;
        }

        self.read_offset = offset;
        Ok(out)
    }

    /// Watermarks loaded from meta on startup. Use to seed the dedup cache.
    pub fn watermarks(&self) -> &HashMap<RunId, SequenceNumber> {
        &self.watermarks
    }

    /// Persist committed offset and watermarks, then compact the WAL file.
    pub fn commit(
        &mut self,
        new_watermarks: &HashMap<RunId, SequenceNumber>,
    ) -> Result<(), WalError> {
        self.committed_offset = self.read_offset;

        // Merge watermarks (take max per run)
        for (run_id, seq) in new_watermarks {
            self.watermarks
                .entry(*run_id)
                .and_modify(|existing| {
                    if *seq > *existing {
                        *existing = *seq;
                    }
                })
                .or_insert(*seq);
        }

        persist_meta(&self.dir, self.committed_offset, &self.watermarks)?;

        // Compact: if everything has been consumed, truncate the file
        let file_size = file_len(&self.file)?;
        if self.committed_offset >= file_size {
            self.file.set_len(0)?;
            self.write_offset = 0;
            self.committed_offset = 0;
            self.read_offset = 0;
            persist_meta(&self.dir, 0, &self.watermarks)?;
        }

        Ok(())
    }
}

fn persist_meta(
    dir: &Path,
    committed_offset: u64,
    watermarks: &HashMap<RunId, SequenceNumber>,
) -> Result<(), WalError> {
    let meta = ServerMeta {
        committed_offset,
        watermarks: watermarks
            .iter()
            .map(|(k, v)| {
                let uuid: uuid::Uuid = (*k).into();
                (uuid.to_string(), u64::from(*v))
            })
            .collect(),
    };

    let json = serde_json::to_string(&meta).map_err(|e| WalError::Unknown(e.into()))?;

    let tmp = dir.join(".server-wal.meta.tmp");
    fs::write(&tmp, json.as_bytes())?;
    fs::rename(&tmp, dir.join(META_FILENAME))?;
    Ok(())
}

fn file_len(file: &File) -> Result<u64, WalError> {
    Ok(file.metadata()?.len())
}

/// Scan from offset 0, validate CRC on each record, return the offset after
/// the last valid record. Truncates any partial trailing record.
fn scan_valid_extent(file: &File) -> Result<u64, WalError> {
    let file_size = file.metadata()?.len();
    if file_size == 0 {
        return Ok(0);
    }

    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(0))?;
    let mut offset = 0u64;

    loop {
        let mut header = [0u8; HEADER_SIZE];
        if reader.read_exact(&mut header).is_err() || &header[OFF_MAGIC..OFF_PAYLOAD_LEN] != MAGIC
        {
            break;
        }

        let payload_len =
            u32::from_le_bytes(header[OFF_PAYLOAD_LEN..OFF_RUN_ID].try_into().unwrap()) as usize;

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
            tracing::warn!(offset, "server WAL CRC mismatch — truncating");
            break;
        }

        offset += RECORD_OVERHEAD as u64 + payload_len as u64;
    }

    // Truncate any partial record at the end
    if offset < file_size {
        tracing::info!(
            valid = offset,
            file_size,
            "server WAL: truncating partial record"
        );
        file.set_len(offset)?;
    }

    Ok(offset)
}

fn to_epoch_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

fn from_epoch_ms(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ServerMeta {
    committed_offset: u64,
    #[serde(default)]
    watermarks: HashMap<String, u64>,
}

impl ServerMeta {
    fn to_watermarks(&self) -> HashMap<RunId, SequenceNumber> {
        self.watermarks
            .iter()
            .filter_map(|(k, v)| {
                uuid::Uuid::parse_str(k)
                    .ok()
                    .map(|uuid| (RunId::from(uuid), SequenceNumber::from(*v)))
            })
            .collect()
    }
}

fn load_meta(dir: &Path) -> ServerMeta {
    let Ok(content) = fs::read_to_string(dir.join(META_FILENAME)) else {
        return ServerMeta {
            committed_offset: 0,
            watermarks: HashMap::new(),
        };
    };

    match serde_json::from_str::<ServerMeta>(&content) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!("corrupt server-wal.meta, starting from zero: {e}");
            ServerMeta {
                committed_offset: 0,
                watermarks: HashMap::new(),
            }
        }
    }
}
