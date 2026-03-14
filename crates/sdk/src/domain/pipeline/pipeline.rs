use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use bytes::BytesMut;
use crossbeam_channel::{Receiver, Sender, select, tick};

use photon_core::types::batch::WireBatch;
use photon_core::types::config::BatchConfig;
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::ports::codec::{Codec, CodecError};
use photon_protocol::ports::compress::{CompressionError, Compressor};

use super::assembler::BatchAssembler;
use super::interner::{MetricKey, MetricKeyInterner};
use crate::domain::ports::wal::{WalError, WalStorage};

#[derive(Clone, Copy, Debug)]
pub(crate) struct RawPoint {
    pub key: MetricKey,
    pub value: f64,
    pub step: u64,
    pub timestamp_ns: u64,
}

pub struct Pipeline<K, W, C>
where
    K: Codec<MetricBatch>,
    W: WalStorage,
    C: Compressor,
{
    run_id: RunId,
    rx: Receiver<RawPoint>,
    assembler: BatchAssembler,
    codec: K,
    wal: W,
    compressor: C,
    config: BatchConfig,
    next_sequence: SequenceNumber,
    encode_buf: BytesMut,
    compress_buf: BytesMut,
    batch_tx: Sender<WireBatch>,
}

impl<K, W, C> Pipeline<K, W, C>
where
    K: Codec<MetricBatch>,
    W: WalStorage,
    C: Compressor,
{
    pub fn new(
        run_id: RunId,
        rx: Receiver<RawPoint>,
        interner: Arc<MetricKeyInterner>,
        codec: K,
        wal: W,
        compressor: C,
        config: BatchConfig,
        start_sequence: SequenceNumber,
        batch_tx: Sender<WireBatch>,
    ) -> Self {
        Self {
            run_id,
            rx,
            assembler: BatchAssembler::new(interner),
            codec,
            wal,
            compressor,
            config,
            next_sequence: start_sequence,
            encode_buf: BytesMut::new(),
            compress_buf: BytesMut::new(),
            batch_tx,
        }
    }

    pub fn spawn(self) -> JoinHandle<Result<FlushStats, PipelineError>> {
        thread::spawn(move || self.run())
    }

    fn run(mut self) -> Result<FlushStats, PipelineError> {
        let ticker = tick(self.config.flush_interval);
        let mut pending: Vec<RawPoint> = Vec::with_capacity(self.config.max_points);
        let mut stats = FlushStats::default();

        loop {
            select! {
                recv(self.rx) -> msg => {
                    match msg {
                        Ok(point) => {
                            pending.push(point);

                            while pending.len() < self.config.max_points {
                                match self.rx.try_recv() {
                                    Ok(p) => pending.push(p),
                                    Err(_) => break,
                                }
                            }

                            if pending.len() >= self.config.max_points {
                                self.flush(&mut pending, &mut stats)?;
                            }
                        }
                        Err(_) => {
                            if !pending.is_empty() {
                                self.flush(&mut pending, &mut stats)?;
                            }

                            return Ok(stats);
                        }
                    }
                }

                recv(ticker) -> _ => {
                    if !pending.is_empty() {
                        self.flush(&mut pending, &mut stats)?;
                    }
                }
            }
        }
    }

    fn flush(
        &mut self,
        pending: &mut Vec<RawPoint>,
        stats: &mut FlushStats,
    ) -> Result<(), PipelineError> {
        let point_count = pending.len();

        let batch = self.assembler.assemble(self.run_id, pending);

        self.encode_buf.clear();
        self.codec.encode(&batch, &mut self.encode_buf)?;

        self.assembler.reclaim(batch);

        let uncompressed_size = self.encode_buf.len();

        self.compress_buf.clear();
        self.compressor
            .compress(&self.encode_buf, &mut self.compress_buf)?;

        let crc = crc32fast::hash(&self.compress_buf);

        let assembled = WireBatch {
            run_id: self.run_id,
            sequence_number: self.next_sequence,
            point_count,
            uncompressed_size,
            compressed_payload: self.compress_buf.clone().freeze(),
            crc32: crc,
            created_at: SystemTime::now(),
        };

        self.wal.append(&assembled)?;
        self.next_sequence = self.next_sequence.next();

        stats.batches_flushed += 1;
        stats.points_flushed += point_count as u64;
        stats.bytes_compressed += assembled.compressed_size() as u64;
        stats.bytes_uncompressed += uncompressed_size as u64;

        let _ = self.batch_tx.send(assembled);

        pending.clear();

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct FlushStats {
    pub batches_flushed: u64,
    pub points_flushed: u64,
    pub bytes_compressed: u64,
    pub bytes_uncompressed: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error("WAL write failed")]
    Wal(#[from] WalError),

    #[error("compression failed")]
    Compression(#[from] CompressionError),

    #[error("batch encoding failed")]
    Codec(#[from] CodecError),
}
