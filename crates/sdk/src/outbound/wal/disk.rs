use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use photon_core::types::batch::WireBatch;
use photon_core::types::config::{WalConfig, WalMeta, WalSyncPolicy};
use photon_core::types::id::RunId;
use photon_core::types::sequence::{SegmentIndex, SequenceNumber};

use super::segment::{self, Active, RECORD_OVERHEAD, Sealed, Segment, WalRecord};

use crate::domain::ports::wal::{WalAppender, WalError, WalManager};

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

pub fn default_wal_dir(run_id: &RunId) -> PathBuf {
    PathBuf::from(".photon")
        .join("wal")
        .join(run_id.to_string())
}

const META_FILENAME: &str = "wal.meta";

pub fn open_disk_wal(
    dir: Option<&Path>,
    run_id: RunId,
    config: DiskWalConfig,
) -> Result<(DiskWalAppender, DiskWalManager), WalError> {
    let dir = dir
        .map(|d| d.join(run_id.to_string()))
        .unwrap_or_else(|| default_wal_dir(&run_id));
    fs::create_dir_all(&dir)?;

    let (committed, next_seg) = load_meta(&dir);
    let existing = segment::list_segments(&dir)?;
    let cap = config.wal.segment_size;

    let (active, sealed) = if existing.is_empty() {
        (Segment::create(&dir, next_seg, cap)?, Vec::new())
    } else {
        recover_segments(&dir, &existing, cap)?
    };

    let bytes_used_val =
        active.bytes_used() + sealed.iter().map(|s| s.bytes_used()).sum::<u64>();

    let next_segment = sealed
        .iter()
        .map(|s| s.index())
        .chain(std::iter::once(active.index()))
        .max()
        .unwrap_or(next_seg)
        .next();

    let sealed = Arc::new(Mutex::new(sealed));
    let bytes_used = Arc::new(AtomicU64::new(bytes_used_val));

    let appender = DiskWalAppender {
        dir: dir.clone(),
        config: config.clone(),
        active,
        sealed: Arc::clone(&sealed),
        bytes_used: Arc::clone(&bytes_used),
        next_segment,
        batches_since_sync: 0,
    };

    let manager = DiskWalManager {
        sealed,
        committed: Arc::new(Mutex::new(committed)),
        dir,
        run_id,
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
        let budget = match self.config.wal.max_total_size {
            Some(b) => b,
            None => return Ok(()),
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
    committed: Arc<Mutex<SequenceNumber>>,
    dir: PathBuf,
    run_id: RunId,
    config: DiskWalConfig,
    bytes_used: Arc<AtomicU64>,
}

impl DiskWalManager {
    fn persist_meta(&self, committed: SequenceNumber) -> Result<(), WalError> {
        let json = serde_json::to_string(&MetaFile {
            run_id: self.run_id.to_string(),
            committed_sequence: u64::from(committed),
            next_segment_index: 0, // not critical for recovery
        })
        .map_err(|e| WalError::Unknown(e.into()))?;

        let tmp = self.dir.join(".wal.meta.tmp");
        fs::write(&tmp, json.as_bytes())?;
        fs::rename(&tmp, self.dir.join(META_FILENAME))?;
        Ok(())
    }

    fn read_after(&self, seq: SequenceNumber) -> Result<Vec<WireBatch>, WalError> {
        let run_id = self.run_id;
        let mut out = Vec::new();

        let sealed = self.sealed.lock().unwrap();
        for seg in sealed.iter() {
            for r in seg.read_records()? {
                if r.sequence_number > seq {
                    out.push(record_to_batch(run_id, r));
                }
            }
        }
        drop(sealed);

        // Read the active segment by opening the latest segment file read-only.
        // The appender may be concurrently writing, but we read only flushed data.
        let active_segments = segment::list_segments(&self.dir)?;
        if let Some((idx, _)) = active_segments.last() {
            let active =
                Segment::open_for_recovery(&self.dir, *idx, self.config.wal.segment_size)?;
            for r in active.read_records()? {
                if r.sequence_number > seq {
                    out.push(record_to_batch(run_id, r));
                }
            }
        }

        out.sort_by_key(|b| b.sequence_number);
        out.dedup_by_key(|b| b.sequence_number);
        Ok(out)
    }

    fn read_next_after(&self, seq: SequenceNumber) -> Result<Option<WireBatch>, WalError> {
        let run_id = self.run_id;

        let sealed = self.sealed.lock().unwrap();
        for seg in sealed.iter() {
            if seg.last_sequence().map(|l| l <= seq).unwrap_or(true) {
                continue;
            }
            if let Some(r) = seg.read_first_record_after(seq)? {
                return Ok(Some(record_to_batch(run_id, r)));
            }
        }
        drop(sealed);

        let active_segments = segment::list_segments(&self.dir)?;
        if let Some((idx, _)) = active_segments.last() {
            let active =
                Segment::open_for_recovery(&self.dir, *idx, self.config.wal.segment_size)?;
            if let Some(r) = active.read_first_record_after(seq)? {
                return Ok(Some(record_to_batch(run_id, r)));
            }
        }

        Ok(None)
    }
}

impl WalManager for DiskWalManager {
    fn truncate_through(&mut self, seq: SequenceNumber) -> Result<(), WalError> {
        *self.committed.lock().unwrap() = seq;

        let mut sealed = self.sealed.lock().unwrap();
        let drained = std::mem::take(&mut *sealed);
        for seg in drained {
            let dominated = seg.last_sequence().map(|l| l <= seq).unwrap_or(true);

            if dominated {
                let freed = seg.bytes_used();
                let _ = seg.mark_acked().delete();
                self.bytes_used.fetch_sub(freed, Ordering::Relaxed);
            } else {
                sealed.push(seg);
            }
        }

        Ok(())
    }

    fn sync(&self) -> Result<(), WalError> {
        let committed = *self.committed.lock().unwrap();
        self.persist_meta(committed)
    }

    fn read_from(&self, seq: SequenceNumber) -> Result<Vec<WireBatch>, WalError> {
        self.read_after(seq)
    }

    fn read_next(&self, after: SequenceNumber) -> Result<Option<WireBatch>, WalError> {
        self.read_next_after(after)
    }

    fn read_meta(&self) -> Result<WalMeta, WalError> {
        Ok(WalMeta {
            committed_sequence: *self.committed.lock().unwrap(),
        })
    }

    fn delete_all(&mut self) -> Result<(), WalError> {
        fs::remove_dir_all(&self.dir)?;
        Ok(())
    }

    fn total_bytes(&self) -> u64 {
        self.bytes_used.load(Ordering::Relaxed)
    }
}

fn record_to_batch(run_id: RunId, r: WalRecord) -> WireBatch {
    WireBatch {
        run_id,
        sequence_number: r.sequence_number,
        compressed_payload: r.compressed_payload,
        crc32: r.batch_crc32,
        created_at: r.created_at,
        point_count: r.point_count,
        uncompressed_size: r.uncompressed_size,
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct MetaFile {
    run_id: String,
    committed_sequence: u64,
    next_segment_index: u64,
}

fn load_meta(dir: &Path) -> (SequenceNumber, SegmentIndex) {
    let Ok(content) = fs::read_to_string(dir.join(META_FILENAME)) else {
        return (SequenceNumber::ZERO, SegmentIndex::ZERO);
    };

    match serde_json::from_str::<MetaFile>(&content) {
        Ok(m) => (
            SequenceNumber::from(m.committed_sequence),
            SegmentIndex::from(m.next_segment_index),
        ),
        Err(e) => {
            tracing::warn!("corrupt wal.meta, starting from zero: {e}");
            (SequenceNumber::ZERO, SegmentIndex::ZERO)
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

    tracing::info!(
        sealed = sealed.len(),
        active = %active.index(),
        "WAL recovery complete"
    );
    Ok((active, sealed))
}
