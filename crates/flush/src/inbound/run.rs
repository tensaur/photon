use crossbeam_channel::{Receiver, Sender, select, tick};

use photon_core::types::batch::WireBatch;
use photon_core::types::config::BatchConfig;
use photon_core::types::metric::MetricBatch;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_wal::WalAppender;

use crate::domain::service::{FlushError, FlushService};
use crate::domain::types::{FlushStats, RawPoint};

pub fn run_flush_thread<K, C, A>(
    mut service: FlushService<K, C, A>,
    rx: Receiver<RawPoint>,
    batch_tx: Sender<WireBatch>,
    config: BatchConfig,
) -> Result<FlushStats, FlushError>
where
    K: Codec<MetricBatch>,
    C: Compressor,
    A: WalAppender,
{
    let ticker = tick(config.flush_interval);
    let mut pending: Vec<RawPoint> = Vec::with_capacity(config.max_points);
    let mut stats = FlushStats::default();

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
                            let wire = service.flush(&pending, &mut stats)?;
                            let _ = batch_tx.send(wire);
                            pending.clear();
                        }
                    }
                    Err(_) => {
                        if !pending.is_empty() {
                            let wire = service.flush(&pending, &mut stats)?;
                            let _ = batch_tx.send(wire);
                            pending.clear();
                        }

                        return Ok(stats);
                    }
                }
            }

            recv(ticker) -> _ => {
                if !pending.is_empty() {
                    let wire = service.flush(&pending, &mut stats)?;
                    let _ = batch_tx.send(wire);
                    pending.clear();
                }
            }
        }
    }
}
