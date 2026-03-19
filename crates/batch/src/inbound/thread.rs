use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, select, tick};
use lasso::ThreadedRodeo;

use photon_core::types::batch::WireBatch;
use photon_core::types::config::BatchConfig;
use photon_core::types::id::RunId;
use photon_core::types::metric::{MetricBatch, RawPoint};
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_wal::WalAppender;

use crate::domain::service::{BatchError, BatchService, Service};
use crate::domain::types::BatchStats;

pub fn run_batch_thread<K, C, A>(
    run_id: RunId,
    interner: Arc<ThreadedRodeo>,
    codec: K,
    compressor: C,
    wal: A,
    start_sequence: SequenceNumber,
    rx: Receiver<RawPoint>,
    batch_tx: Sender<WireBatch>,
    config: BatchConfig,
) -> Result<BatchStats, BatchError>
where
    K: Codec<MetricBatch>,
    C: Compressor,
    A: WalAppender,
{
    let mut service = Service::new(run_id, interner, codec, compressor, wal, start_sequence);
    let ticker = tick(config.batch_interval);
    let mut pending: Vec<RawPoint> = Vec::with_capacity(config.max_points);
    let mut stats = BatchStats::default();

    loop {
        select! {
            recv(rx) -> msg => {
                match msg {
                    Ok(point) => {
                        pending.push(point);

                        while pending.len() < config.max_points {
                            match rx.try_recv() {
                                Ok(p) => pending.push(p),
                                Err(_) => break,
                            }
                        }

                        if pending.len() >= config.max_points {
                            let wire = service.batch(&pending, &mut stats)?;
                            let _ = batch_tx.send(wire);
                            pending.clear();
                        }
                    }
                    Err(_) => {
                        if !pending.is_empty() {
                            let wire = service.batch(&pending, &mut stats)?;
                            let _ = batch_tx.send(wire);
                            pending.clear();
                        }

                        return Ok(stats);
                    }
                }
            }

            recv(ticker) -> _ => {
                if !pending.is_empty() {
                    let wire = service.batch(&pending, &mut stats)?;
                    let _ = batch_tx.send(wire);
                    pending.clear();
                }
            }
        }
    }
}
