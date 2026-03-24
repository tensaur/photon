use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Notify;

use photon_core::types::batch::WireBatch;
use photon_core::types::config::{WalConfig, WalSyncPolicy};
use photon_core::types::sequence::SegmentIndex;

use crate::ports::{WalAppender, WalError};
use crate::segment::{Active, Sealed, Segment};

#[derive(Clone, Debug)]
pub struct ServerWalConfig {
    pub wal: WalConfig,
    pub rotation_threshold: f64,
}

impl Default for ServerWalConfig {
    fn default() -> Self {
        Self {
            wal: WalConfig::default(),
            rotation_threshold: 0.95,
        }
    }
}

const META_FILENAME: &str = "server-wal.meta";

pub fn open(
    dir: impl Into<PathBuf>,
    config: ServerWalConfig,
) -> Result<(ServerWalAppender, ServerWalReader, Arc<Notify>), WalError> {
    let dir = dir.into();
    fs::create_dir_all(&dir)?;

    let meta = load_meta(&dir);
    let existing = crate::segment::list_segments(&dir)?;
    let cap = config.wal.segment_size;

    let (active, sealed) = if existing.is_empty() {
        (Segment::create(&dir, SegmentIndex::ZERO, cap)?, Vec::new())
    } else {
        recover_segments(&dir, &existing, cap)?
    };

    let next_segment = sealed
        .iter()
        .map(|s| s.index())
        .chain(std::iter::once(active.index()))
        .max()
        .unwrap_or(SegmentIndex::ZERO)
        .next();

    let notify = Arc::new(Notify::new());

    let appender = ServerWalAppender {
        dir: dir.clone(),
        config: config.clone(),
        active,
        sealed,
        next_segment,
        batches_since_sync: 0,
        notify: Arc::clone(&notify),
    };

    let reader = ServerWalReader {
        dir,
        segment_capacity: cap,
        committed_segment: meta.committed_segment,
        committed_offset: meta.committed_offset,
        read_segment: meta.committed_segment,
        read_offset: meta.committed_offset,
    };

    Ok((appender, reader, notify))
}

pub struct ServerWalAppender {
    dir: PathBuf,
    config: ServerWalConfig,
    active: Segment<Active>,
    sealed: Vec<Segment<Sealed>>,
    next_segment: SegmentIndex,
    batches_since_sync: u32,
    notify: Arc<Notify>,
}

impl ServerWalAppender {
    fn rotate(&mut self) -> Result<(), WalError> {
        let idx = self.next_segment;
        let cap = self.config.wal.segment_size;
        let new = Segment::create(&self.dir, idx, cap)?;
        self.next_segment = idx.next();

        let old = std::mem::replace(&mut self.active, new);
        old.flush()?;
        self.sealed.push(old.seal());
        Ok(())
    }

    fn maybe_sync(&mut self) -> Result<(), WalError> {
        match &self.config.wal.sync_policy {
            WalSyncPolicy::EveryBatch => self.active.flush()?,
            WalSyncPolicy::Periodic { batches, .. } => {
                if self.batches_since_sync >= *batches {
                    self.active.flush()?;
                    self.batches_since_sync = 0;
                }
            }
            WalSyncPolicy::OsManaged => self.active.flush_async()?,
        }
        Ok(())
    }
}

impl WalAppender for ServerWalAppender {
    fn append(&mut self, batch: &WireBatch) -> Result<(), WalError> {
        let threshold = self.config.rotation_threshold;
        let at_cap = self.active.append(batch, threshold)?;
        self.batches_since_sync += 1;
        self.maybe_sync()?;

        if at_cap {
            self.rotate()?;
        }

        self.notify.notify_one();
        Ok(())
    }
}

pub struct ServerWalReader {
    dir: PathBuf,
    segment_capacity: u64,
    committed_segment: u64,
    committed_offset: u64,
    read_segment: u64,
    read_offset: u64,
}

impl ServerWalReader {
    /// Read all available entries from the current read position.
    /// Returns empty vec if caught up.
    pub fn read_available(&mut self) -> Result<Vec<WireBatch>, WalError> {
        let segments = crate::segment::list_segments(&self.dir)?;
        let mut out = Vec::new();

        for (idx, _) in &segments {
            let idx_val: u64 = (*idx).into();
            if idx_val < self.read_segment {
                continue;
            }

            let seg = Segment::open_for_recovery(&self.dir, *idx, self.segment_capacity)?;

            // For the current segment, start from read_offset. For later segments, start from 0.
            let start = if idx_val == self.read_segment {
                self.read_offset
            } else {
                0
            };

            let (records, end_offset) = seg.read_records_from(start)?;

            for record in records {
                out.push(record.into_wire_batch());
            }

            // Advance read position to the end of this segment
            self.read_segment = idx_val;
            self.read_offset = end_offset;
        }

        Ok(out)
    }

    /// Persist the current read position as committed and GC consumed segments.
    pub fn commit(&mut self) -> Result<(), WalError> {
        self.committed_segment = self.read_segment;
        self.committed_offset = self.read_offset;

        // Persist meta
        let json = serde_json::to_string(&ServerMeta {
            committed_segment: self.committed_segment,
            committed_offset: self.committed_offset,
        })
        .map_err(|e| WalError::Unknown(e.into()))?;

        let tmp = self.dir.join(".server-wal.meta.tmp");
        fs::write(&tmp, json.as_bytes())?;
        fs::rename(&tmp, self.dir.join(META_FILENAME))?;

        // GC: delete segments fully before the committed segment
        let segments = crate::segment::list_segments(&self.dir)?;
        for (idx, path) in &segments {
            let idx_val: u64 = (*idx).into();
            if idx_val < self.committed_segment {
                let _ = fs::remove_file(path);
            }
        }

        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ServerMeta {
    committed_segment: u64,
    committed_offset: u64,
}

struct LoadedMeta {
    committed_segment: u64,
    committed_offset: u64,
}

fn load_meta(dir: &Path) -> LoadedMeta {
    let Ok(content) = fs::read_to_string(dir.join(META_FILENAME)) else {
        return LoadedMeta {
            committed_segment: 0,
            committed_offset: 0,
        };
    };

    match serde_json::from_str::<ServerMeta>(&content) {
        Ok(m) => LoadedMeta {
            committed_segment: m.committed_segment,
            committed_offset: m.committed_offset,
        },
        Err(e) => {
            tracing::warn!("corrupt server-wal.meta, starting from zero: {e}");
            LoadedMeta {
                committed_segment: 0,
                committed_offset: 0,
            }
        }
    }
}

fn recover_segments(
    dir: &Path,
    existing: &[(SegmentIndex, PathBuf)],
    capacity: u64,
) -> Result<(Segment<Active>, Vec<Segment<Sealed>>), WalError> {
    let mut sealed = Vec::with_capacity(existing.len().saturating_sub(1));

    for (idx, _) in &existing[..existing.len() - 1] {
        sealed.push(Segment::open_for_recovery(dir, *idx, capacity)?.seal());
    }

    let (last_idx, _) = existing.last().unwrap();
    let active = Segment::open_for_recovery(dir, *last_idx, capacity)?;

    tracing::info!(
        sealed = sealed.len(),
        active = %active.index(),
        "server WAL recovery complete"
    );
    Ok((active, sealed))
}
