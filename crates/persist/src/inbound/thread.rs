use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;
use tokio_util::sync::CancellationToken;

use photon_core::types::wal::WalOffset;
use photon_wal::Wal;

use crate::domain::changeset::ChangeSet;
use crate::domain::service::PersistService;

#[derive(Clone)]
pub struct PersistConfig {
    pub poll_interval: std::time::Duration,
    pub max_batch_read: usize,
}

impl Default for PersistConfig {
    fn default() -> Self {
        Self {
            poll_interval: std::time::Duration::from_millis(100),
            max_batch_read: 1000,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct PersistStats {
    pub batches_persisted: u64,
    pub points_persisted: u64,
}

pub async fn run<S, W>(
    wal: W,
    notify: Arc<tokio::sync::Notify>,
    mut service: S,
    mut finished_runs_rx: tokio::sync::mpsc::UnboundedReceiver<(RunId, SequenceNumber)>,
    config: PersistConfig,
    cancel: CancellationToken,
) -> PersistStats
where
    S: PersistService,
    W: Wal,
{
    tracing::info!("persist consumer started");
    let mut cursor = wal
        .read_meta()
        .map(|m| m.consumed)
        .unwrap_or(WalOffset::ZERO);

    let start = Instant::now();
    let mut stats = PersistStats::default();
    // Finalisation is gated on the run's last-ingested sequence number. 
    let mut pending_finishes: HashMap<RunId, SequenceNumber> = HashMap::new();
    let mut processed_seq: HashMap<RunId, SequenceNumber> = HashMap::new();

    loop {
        // Drain finish signals (run_id, last_seq).
        while let Ok((run_id, seq)) = finished_runs_rx.try_recv() {
            pending_finishes.insert(run_id, seq);
        }

        // Read pending WAL batches
        let mut batches = match wal.read_from(cursor) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("WAL read failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        batches.truncate(config.max_batch_read);

        // Caught up. Exit if cancelled, otherwise wait for a signal.
        if batches.is_empty() && pending_finishes.is_empty() {
            if cancel.is_cancelled() {
                break;
            }
            tokio::select! {
                () = notify.notified() => {}
                () = tokio::time::sleep(config.poll_interval) => {}
                () = cancel.cancelled() => break,
            }
            continue;
        }

        // Build changeset
        let mut changeset = ChangeSet::with_capacity(batches.len());

        // Track the highest seq observed per run in this batch so we can decide
        // whether each pending finalisation is now safe to fire.
        for batch in &batches {
            let entry = processed_seq
                .entry(batch.run_id)
                .or_insert(batch.sequence_number);
            if batch.sequence_number > *entry {
                *entry = batch.sequence_number;
            }
        }

        if let Err(e) = service.write(&batches, &mut changeset) {
            tracing::error!("persist write failed: {e}");
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        // Fire finish_run only for runs whose last-ingested seq we've now seen.
        // Runs with no batches at all (finish_seq = 0) fire immediately.
        let ready: Vec<RunId> = pending_finishes
            .iter()
            .filter(|(run_id, finish_seq)| {
                **finish_seq == SequenceNumber::ZERO
                    || processed_seq
                        .get(*run_id)
                        .is_some_and(|s| s >= *finish_seq)
            })
            .map(|(id, _)| *id)
            .collect();
        for run_id in ready {
            pending_finishes.remove(&run_id);
            processed_seq.remove(&run_id);
            service.finish_run(run_id, &mut changeset);
        }

        // Flush
        match service.flush(&mut changeset).await {
            Ok(()) => {
                let count = batches.len() as u64;
                let points: u64 = batches.iter().map(|b| b.point_count as u64).sum();
                stats.batches_persisted += count;
                stats.points_persisted += points;
                log_persist(&stats, start);

                cursor = cursor.advance(count);
                if let Err(e) = wal.truncate_through(cursor) {
                    tracing::error!("WAL truncate failed: {e}");
                }
                let _ = wal.sync();
            }
            Err(e) => {
                tracing::error!("persist flush failed: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    tracing::info!(
        batches = stats.batches_persisted,
        points = stats.points_persisted,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "persist consumer shut down"
    );

    stats
}

fn log_persist(stats: &PersistStats, start: Instant) {
    let elapsed = start.elapsed().as_secs_f64();
    let throughput = if elapsed > 0.0 {
        stats.points_persisted as f64 / elapsed / 1_000_000.0
    } else {
        0.0
    };
    tracing::info!(
        batches = stats.batches_persisted,
        points = stats.points_persisted,
        throughput_mpts = format!("{throughput:.2}"),
        "persisted"
    );
}
