use std::thread::{self, JoinHandle};
use std::time::SystemTime;

use bytes::BytesMut;
use crossbeam_channel::{Receiver, select, tick};

use crate::ports::codec::{BatchCodec, CodecError};
use crate::ports::compress::{Compressor, CompressionError};
use crate::ports::resolver::PointResolver;
use crate::ports::wal::{WalStorage, WalError};
use crate::types::batch::AssembledBatch;
use crate::types::config::BatchConfig;
use crate::types::id::RunId;
use crate::types::metric::MetricBatch;
use crate::types::sequence::SequenceNumber;

pub struct BatchBuilder<R, K, W, C>
where
    R: PointResolver,
    K: BatchCodec,
    W: WalStorage,
    C: Compressor,
{
    run_id: RunId,
    rx: Receiver<R::Point>,
    resolver: R,
    codec: K,
    wal: W,
    compressor: C,
    config: BatchConfig,
    next_sequence: SequenceNumber,
    encode_buf: BytesMut,
    compress_buf: BytesMut,
}

impl<R, K, W, C> BatchBuilder<R, K, W, C>
where
    R: PointResolver,
    K: BatchCodec,
    W: WalStorage,
    C: Compressor,
{
    pub fn new(
        run_id: RunId,
        rx: Receiver<R::Point>,
        resolver: R,
        codec: K,
        wal: W,
        compressor: C,
        config: BatchConfig,
        start_sequence: SequenceNumber,
    ) -> Self {
        Self {
            run_id,
            rx,
            resolver,
            codec,
            wal,
            compressor,
            config,
            next_sequence: start_sequence,
            encode_buf: BytesMut::new(),
            compress_buf: BytesMut::new(),
        }
    }

    pub fn spawn(self) -> JoinHandle<Result<BuilderStats, BatchBuilderError>> {
        thread::spawn(move || self.run())
    }

    fn run(mut self) -> Result<BuilderStats, BatchBuilderError> {
        let ticker = tick(self.config.flush_interval);
        let mut pending: Vec<R::Point> = Vec::with_capacity(self.config.max_points);
        let mut stats = BuilderStats::default();

        loop {
            select! {
                recv(self.rx) -> msg => {
                    match msg {
                        Ok(point) => {
                            pending.push(point);

                            if pending.len() >= self.config.max_points {
                                self.flush(&mut pending, &mut stats)?;
                            }
                        }
                        Err(_) => {
                            // Channel disconnected — accumulator was dropped.
                            // Flush remaining points and exit cleanly.
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
        pending: &mut Vec<R::Point>,
        stats: &mut BuilderStats,
    ) -> Result<(), BatchBuilderError> {
        let points = self.resolver.resolve(pending);
        let batch = MetricBatch {
            run_id: self.run_id.clone(),
            points,
        };
        let point_count = batch.len();

        self.encode_buf.clear();
        self.codec.encode(&batch, &mut self.encode_buf)?;
        let uncompressed_size = self.encode_buf.len();

        self.compress_buf.clear();
        self.compressor
            .compress(&self.encode_buf, &mut self.compress_buf)?;

        let crc = crc32fast::hash(&self.compress_buf);

        let assembled = AssembledBatch {
            run_id: self.run_id.clone(),
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

        pending.clear();

        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct BuilderStats {
    pub batches_flushed: u64,
    pub points_flushed: u64,
    pub bytes_compressed: u64,
    pub bytes_uncompressed: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum BatchBuilderError {
    #[error("WAL write failed")]
    Wal(#[from] WalError),

    #[error("compression failed")]
    Compression(#[from] CompressionError),

    #[error("batch encoding failed")]
    Codec(#[from] CodecError),
}
