use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use photon_core::types::batch::AssembledBatch;
use photon_core::types::config::{WalConfig, WalMeta, WalSyncPolicy};
use photon_core::types::id::RunId;
use photon_core::types::sequence::{SegmentIndex, SequenceNumber};

use super::segment::{self, Active, RECORD_OVERHEAD, Sealed, Segment, WalRecord};

use crate::domain::ports::wal::{WalError, WalStorage};

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

pub struct DiskWalStorage {
    dir: PathBuf,
    run_id: RunId,
    config: DiskWalConfig,
    active: Segment<Active>,
    sealed: Vec<Segment<Sealed>>,
    bytes_used: u64,
    next_segment: SegmentIndex,
    committed: SequenceNumber,
    batches_since_sync: u32,
}

impl DiskWalStorage {
    pub fn open(
        dir: Option<&Path>,
        run_id: RunId,
        config: DiskWalConfig,
    ) -> Result<Self, WalError> {
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

        let bytes_used = active.bytes_used() + sealed.iter().map(|s| s.bytes_used()).sum::<u64>();

        let next_segment = sealed
            .iter()
            .map(|s| s.index())
            .chain(std::iter::once(active.index()))
            .max()
            .unwrap_or(next_seg)
            .next();

        Ok(Self {
            dir,
            run_id,
            config,
            active,
            sealed,
            bytes_used,
            next_segment,
            committed,
            batches_since_sync: 0,
        })
    }

    pub fn into_shared(self) -> SharedDiskWal {
        SharedDiskWal(Arc::new(RwLock::new(self)))
    }

    fn rotate(&mut self) -> Result<SegmentIndex, WalError> {
        let idx = self.next_segment;
        let cap = self.config.wal.segment_size;
        let new = Segment::create(&self.dir, idx, cap)?;
        self.next_segment = idx.next();

        let old = std::mem::replace(&mut self.active, new);
        old.flush()?;
        self.sealed.push(old.seal());
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

        let ratio = self.bytes_used as f64 / budget as f64;

        if ratio < self.config.soft_pressure_ratio {
            return Ok(());
        }

        if ratio < self.config.hard_pressure_ratio {
            tracing::warn!(used = self.bytes_used, budget, "WAL soft pressure");
            return Ok(());
        }

        while self.bytes_used as f64 / budget as f64 > self.config.soft_pressure_ratio {
            let Some(victim) = self.sealed.first() else {
                break;
            };
            tracing::error!(segment = %victim.index(), "dropping un-acked segment");

            let victim = self.sealed.remove(0);
            let freed = victim.bytes_used();
            let _ = victim.mark_acked().delete();
            self.bytes_used = self.bytes_used.saturating_sub(freed);
        }

        if self.bytes_used > budget {
            return Err(WalError::DiskFull {
                budget,
                used: self.bytes_used,
            });
        }

        Ok(())
    }

    fn persist_meta(&self) -> Result<(), WalError> {
        let json = serde_json::to_string(&MetaFile {
            run_id: self.run_id.to_string(),
            committed_sequence: u64::from(self.committed),
            next_segment_index: u64::from(self.next_segment),
        })
        .map_err(|e| WalError::Unknown(e.into()))?;

        let tmp = self.dir.join(".wal.meta.tmp");
        fs::write(&tmp, json.as_bytes())?;
        fs::rename(&tmp, self.dir.join(META_FILENAME))?;
        Ok(())
    }

    pub fn append_batch(&mut self, batch: &AssembledBatch) -> Result<(), WalError> {
        self.enforce_budget()?;

        let threshold = self.config.rotation_threshold;
        let at_cap = self.active.append(batch, threshold)?;
        self.bytes_used += batch.compressed_payload.len() as u64 + RECORD_OVERHEAD as u64;
        self.batches_since_sync += 1;
        self.maybe_sync()?;

        if at_cap {
            self.rotate()?;
        }

        Ok(())
    }

    pub fn sync_wal(&self) -> Result<(), WalError> {
        self.active.flush()?;
        self.persist_meta()
    }

    pub fn truncate_through_seq(&mut self, seq: SequenceNumber) -> Result<(), WalError> {
        self.committed = seq;

        let drained = std::mem::take(&mut self.sealed);
        for seg in drained {
            let dominated = seg.last_sequence().map(|l| l <= seq).unwrap_or(true);

            if dominated {
                let freed = seg.bytes_used();
                let _ = seg.mark_acked().delete();
                self.bytes_used = self.bytes_used.saturating_sub(freed);
            } else {
                self.sealed.push(seg);
            }
        }

        Ok(())
    }

    pub fn read_after(&self, seq: SequenceNumber) -> Result<Vec<AssembledBatch>, WalError> {
        let run_id = self.run_id;
        let mut out = Vec::new();

        for seg in &self.sealed {
            for r in seg.read_records()? {
                if r.sequence_number > seq {
                    out.push(record_to_batch(run_id, r));
                }
            }
        }

        for r in self.active.read_records()? {
            if r.sequence_number > seq {
                out.push(record_to_batch(run_id, r));
            }
        }

        out.sort_by_key(|b| b.sequence_number);
        Ok(out)
    }

    pub fn meta(&self) -> WalMeta {
        WalMeta {
            committed_sequence: self.committed,
        }
    }
}

fn record_to_batch(run_id: RunId, r: WalRecord) -> AssembledBatch {
    AssembledBatch {
        run_id,
        sequence_number: r.sequence_number,
        compressed_payload: r.compressed_payload,
        crc32: r.batch_crc32,
        created_at: r.created_at,
        point_count: r.point_count,
        uncompressed_size: r.uncompressed_size,
    }
}

#[derive(Clone)]
pub struct SharedDiskWal(Arc<RwLock<DiskWalStorage>>);

impl SharedDiskWal {
    pub fn open(
        dir: Option<&Path>,
        run_id: RunId,
        config: DiskWalConfig,
    ) -> Result<Self, WalError> {
        DiskWalStorage::open(dir, run_id, config).map(|d| d.into_shared())
    }

}

impl WalStorage for SharedDiskWal {
    fn append(&mut self, batch: &AssembledBatch) -> Result<(), WalError> {
        self.0.write().unwrap().append_batch(batch)
    }

    fn sync(&self) -> Result<(), WalError> {
        self.0.read().unwrap().sync_wal()
    }

    fn rotate_segment(&mut self) -> Result<SegmentIndex, WalError> {
        self.0.write().unwrap().rotate()
    }

    fn truncate_through(&mut self, seq: SequenceNumber) -> Result<(), WalError> {
        self.0.write().unwrap().truncate_through_seq(seq)
    }

    fn read_from(&self, seq: SequenceNumber) -> Result<Vec<AssembledBatch>, WalError> {
        self.0.read().unwrap().read_after(seq)
    }

    fn read_meta(&self) -> Result<WalMeta, WalError> {
        Ok(self.0.read().unwrap().meta())
    }

    fn delete_all(&mut self) -> Result<(), WalError> {
        let inner = self.0.write().unwrap();
        fs::remove_dir_all(&inner.dir)?;
        Ok(())
    }

    fn total_bytes(&self) -> u64 {
        self.0.read().unwrap().bytes_used
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
