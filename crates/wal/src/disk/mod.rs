use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use photon_core::types::batch::WireBatch;
use photon_core::types::config::{WalConfig, WalMeta, WalSyncPolicy};
use photon_core::types::wal::{SegmentIndex, WalOffset};

pub(crate) mod segment;

use self::segment::{Active, RECORD_OVERHEAD, Sealed, Segment};

use crate::ports::{Wal, WalAppender, WalError};

#[derive(Clone, Debug)]
pub struct DiskWalConfig {
    pub wal: WalConfig,
    /// Fraction of segment capacity that triggers rotation (0.0, 1.0)
    pub rotation_threshold: f64,
    /// WAL budget fraction for soft warning (log and nudge sender)
    pub soft_pressure_ratio: f64,
    /// WAL budget fraction for hard action (drop oldest sealed segments)
    pub hard_pressure_ratio: f64,
}

impl Default for DiskWalConfig {
    fn default() -> Self {
        Self {
            wal: WalConfig::default(),
            rotation_threshold: 0.95,
            soft_pressure_ratio: 0.80,
            hard_pressure_ratio: 0.95,
        }
    }
}

pub fn default_wal_dir() -> PathBuf {
    PathBuf::from(".photon").join("wal")
}

const META_FILENAME: &str = "wal.meta";

pub fn open_disk_wal(
    dir: impl Into<PathBuf>,
    config: DiskWalConfig,
) -> Result<(DiskWalAppender, DiskWalManager), WalError> {
    let dir = dir.into();
    fs::create_dir_all(&dir)?;

    let (cursor, consumed) = load_meta(&dir);
    let existing = segment::list_segments(&dir)?;
    let cap = config.wal.segment_size;

    let (active, sealed) = if existing.is_empty() {
        (Segment::create(&dir, SegmentIndex::ZERO, cap)?, Vec::new())
    } else {
        recover_segments(&dir, &existing, cap)?
    };

    // Count total records across all segments to determine next_offset
    let mut total_records = 0u64;
    for seg in &sealed {
        total_records += seg.record_count()? as u64;
    }
    total_records += active.record_count()? as u64;

    let stale_on_disk = u64::from(consumed).saturating_sub(u64::from(cursor));
    let to_replay = total_records.saturating_sub(stale_on_disk);
    if to_replay > 0 {
        tracing::info!(
            records = to_replay,
            "WAL recovery: replaying unconsumed records"
        );
    }

    let next_offset = consumed.advance(to_replay);

    let bytes_used_val =
        active.bytes_used() + sealed.iter().map(segment::Segment::bytes_used).sum::<u64>();

    let next_segment = sealed
        .iter()
        .map(segment::Segment::index)
        .chain(std::iter::once(active.index()))
        .max()
        .unwrap_or(SegmentIndex::ZERO)
        .next();

    let sealed = Arc::new(Mutex::new(sealed));
    let bytes_used = Arc::new(AtomicU64::new(bytes_used_val));
    let next_offset = Arc::new(AtomicU64::new(u64::from(next_offset)));

    let appender = DiskWalAppender {
        dir: dir.clone(),
        config: config.clone(),
        active,
        sealed: Arc::clone(&sealed),
        bytes_used: Arc::clone(&bytes_used),
        next_offset: Arc::clone(&next_offset),
        next_segment,
        batches_since_sync: 0,
    };

    let manager = DiskWalManager {
        sealed,
        cursor: Arc::new(Mutex::new(cursor)),
        consumed: Arc::new(Mutex::new(consumed)),
        dir,
        config,
        bytes_used,
    };

    Ok((appender, manager))
}

pub struct DiskWalAppender {
    dir: PathBuf,
    config: DiskWalConfig,
    active: Segment<Active>,
    sealed: Arc<Mutex<Vec<Segment<Sealed>>>>,
    bytes_used: Arc<AtomicU64>,
    next_offset: Arc<AtomicU64>,
    next_segment: SegmentIndex,
    batches_since_sync: u32,
}

impl DiskWalAppender {
    fn rotate(&mut self) -> Result<SegmentIndex, WalError> {
        let idx = self.next_segment;
        let cap = self.config.wal.segment_size;
        let new = Segment::create(&self.dir, idx, cap)?;
        self.next_segment = idx.next();

        let old = std::mem::replace(&mut self.active, new);
        old.flush()?;
        let sealed_seg = old.seal();
        self.sealed.lock().unwrap().push(sealed_seg);
        Ok(idx)
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

    fn enforce_budget(&mut self) -> Result<(), WalError> {
        let Some(budget) = self.config.wal.max_total_size else {
            return Ok(());
        };

        let used = self.bytes_used.load(Ordering::Relaxed);
        let ratio = used as f64 / budget as f64;

        if ratio < self.config.soft_pressure_ratio {
            return Ok(());
        }

        if ratio < self.config.hard_pressure_ratio {
            tracing::warn!(used, budget, "WAL soft pressure");
            return Ok(());
        }

        // Hard pressure: drop oldest sealed segments
        let mut sealed = self.sealed.lock().unwrap();
        let mut current_used = used;
        while current_used as f64 / budget as f64 > self.config.soft_pressure_ratio {
            let Some(victim) = sealed.first() else {
                break;
            };
            tracing::error!(segment = %victim.index(), "dropping un-acked segment");

            let victim = sealed.remove(0);
            let freed = victim.bytes_used();
            let _ = victim.mark_acked().delete();
            current_used = current_used.saturating_sub(freed);
        }
        drop(sealed);

        self.bytes_used.store(current_used, Ordering::Relaxed);

        if current_used > budget {
            return Err(WalError::DiskFull {
                budget,
                used: current_used,
            });
        }

        Ok(())
    }
}

impl WalAppender for DiskWalAppender {
    fn append(&mut self, batch: &WireBatch) -> Result<(), WalError> {
        self.enforce_budget()?;

        let threshold = self.config.rotation_threshold;
        let at_cap = self.active.append(batch, threshold)?;
        let added = batch.compressed_payload.len() as u64 + RECORD_OVERHEAD as u64;
        self.bytes_used.fetch_add(added, Ordering::Relaxed);
        self.next_offset.fetch_add(1, Ordering::Relaxed);
        self.batches_since_sync += 1;
        self.maybe_sync()?;

        if at_cap {
            self.rotate()?;
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct DiskWalManager {
    sealed: Arc<Mutex<Vec<Segment<Sealed>>>>,
    /// Records physically deleted from disk.
    cursor: Arc<Mutex<WalOffset>>,
    /// Records the consumer has processed (may be ahead of cursor).
    consumed: Arc<Mutex<WalOffset>>,
    dir: PathBuf,
    config: DiskWalConfig,
    bytes_used: Arc<AtomicU64>,
}

impl DiskWalManager {
    fn persist_meta(&self, cursor: WalOffset, consumed: WalOffset) -> Result<(), WalError> {
        let json = serde_json::to_string(&MetaFile {
            cursor: u64::from(cursor),
            consumed: u64::from(consumed),
        })
        .map_err(|e| WalError::Serialization(Box::new(e)))?;

        let tmp = self.dir.join(".wal.meta.tmp");
        fs::write(&tmp, json.as_bytes())?;
        fs::rename(&tmp, self.dir.join(META_FILENAME))?;
        Ok(())
    }

    /// Read all records after the given offset.
    /// The cursor tracks records physically deleted. Skip = offset - cursor
    /// gives the number of consumed-but-still-on-disk records to skip.
    fn read_after(&self, offset: WalOffset) -> Result<Vec<WireBatch>, WalError> {
        let cursor = *self.cursor.lock().unwrap();
        let skip = u64::from(offset).saturating_sub(u64::from(cursor));
        let mut skipped = 0u64;
        let mut out = Vec::new();

        let sealed = self.sealed.lock().unwrap();
        for seg in sealed.iter() {
            let records = seg.read_records()?;
            for batch in records {
                if skipped < skip {
                    skipped += 1;
                } else {
                    out.push(batch);
                }
            }
        }
        drop(sealed);

        // Read the active segment using the read-only, non-mutating path.
        // `open_for_recovery` would call `scan_valid_extent` and truncate the
        // file — racing a concurrent appender and corrupting the tail.
        let active_segments = segment::list_segments(&self.dir)?;
        if let Some((idx, _)) = active_segments.last() {
            let active = Segment::open_for_live_read(&self.dir, *idx, self.config.wal.segment_size)?;
            for batch in active.read_records()? {
                if skipped < skip {
                    skipped += 1;
                } else {
                    out.push(batch);
                }
            }
        }

        Ok(out)
    }
}

impl Wal for DiskWalManager {
    fn close(&self) -> Result<(), WalError> {
        if self.dir.exists() {
            fs::remove_dir_all(&self.dir)?;
        }
        Ok(())
    }

    /// Record the consumer's position, then delete fully consumed sealed segments.
    fn truncate_through(&self, offset: WalOffset) -> Result<(), WalError> {
        *self.consumed.lock().unwrap() = offset;

        let mut cursor = self.cursor.lock().unwrap();
        let offset_val = u64::from(offset);

        let mut sealed = self.sealed.lock().unwrap();
        let drained = std::mem::take(&mut *sealed);
        for seg in drained {
            let count = seg.record_count()? as u64;
            if u64::from(*cursor) + count <= offset_val {
                let freed = seg.bytes_used();
                let _ = seg.mark_acked().delete();
                self.bytes_used.fetch_sub(freed, Ordering::Relaxed);
                *cursor = cursor.advance(count);
            } else {
                sealed.push(seg);
            }
        }

        Ok(())
    }

    fn sync(&self) -> Result<(), WalError> {
        let cursor = *self.cursor.lock().unwrap();
        let consumed = *self.consumed.lock().unwrap();
        self.persist_meta(cursor, consumed)
    }

    fn read_from(&self, offset: WalOffset) -> Result<Vec<WireBatch>, WalError> {
        self.read_after(offset)
    }

    fn read_meta(&self) -> Result<WalMeta, WalError> {
        Ok(WalMeta {
            cursor: *self.cursor.lock().unwrap(),
            consumed: *self.consumed.lock().unwrap(),
        })
    }

    fn delete_all(&self) -> Result<(), WalError> {
        fs::remove_dir_all(&self.dir)?;
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct MetaFile {
    cursor: u64,
    #[serde(default)]
    consumed: u64,
}

fn load_meta(dir: &Path) -> (WalOffset, WalOffset) {
    let Ok(content) = fs::read_to_string(dir.join(META_FILENAME)) else {
        return (WalOffset::ZERO, WalOffset::ZERO);
    };

    match serde_json::from_str::<MetaFile>(&content) {
        Ok(m) => (WalOffset::from(m.cursor), WalOffset::from(m.consumed)),
        Err(e) => {
            tracing::warn!("corrupt wal.meta, starting from zero: {e}");
            (WalOffset::ZERO, WalOffset::ZERO)
        }
    }
}

fn recover_segments(
    dir: &Path,
    existing: &[(SegmentIndex, PathBuf)],
    capacity: u64,
) -> Result<(Segment<Active>, Vec<Segment<Sealed>>), WalError> {
    let mut sealed = Vec::with_capacity(existing.len() - 1);

    for (idx, _) in &existing[..existing.len() - 1] {
        sealed.push(Segment::open_for_recovery(dir, *idx, capacity)?.seal());
    }

    let (last_idx, _) = existing.last().unwrap();
    let active = Segment::open_for_recovery(dir, *last_idx, capacity)?;

    Ok((active, sealed))
}
