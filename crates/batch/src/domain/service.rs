use std::sync::Arc;
use std::time::SystemTime;

use bytes::BytesMut;

use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::metric::MetricBatch;
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::ports::codec::{Codec, CodecError};
use photon_protocol::ports::compress::{CompressionError, Compressor};
use photon_wal::WalAppender;
use photon_wal::ports::WalError;

use super::assembler::BatchAssembler;
use super::interner::MetricKeyInterner;
use super::types::{BatchStats, RawPoint};

#[derive(Debug, thiserror::Error)]
pub enum BatchError {
    #[error("WAL write failed")]
    Wal(#[from] WalError),

    #[error("compression failed")]
    Compression(#[from] CompressionError),

    #[error("batch encoding failed")]
    Codec(#[from] CodecError),
}

pub trait BatchService {
    fn batch(
        &mut self,
        points: &[RawPoint],
        stats: &mut BatchStats,
    ) -> Result<WireBatch, BatchError>;
}

pub struct Service<K, C, A>
where
    K: Codec<MetricBatch>,
    C: Compressor,
    A: WalAppender,
{
    run_id: RunId,
    assembler: BatchAssembler,
    codec: K,
    compressor: C,
    wal: A,
    next_sequence: SequenceNumber,
    encode_buf: BytesMut,
    compress_buf: BytesMut,
}

impl<K, C, A> Service<K, C, A>
where
    K: Codec<MetricBatch>,
    C: Compressor,
    A: WalAppender,
{
    pub fn new(
        run_id: RunId,
        interner: Arc<MetricKeyInterner>,
        codec: K,
        compressor: C,
        wal: A,
        start_sequence: SequenceNumber,
    ) -> Self {
        Self {
            run_id,
            assembler: BatchAssembler::new(interner),
            codec,
            compressor,
            wal,
            next_sequence: start_sequence,
            encode_buf: BytesMut::new(),
            compress_buf: BytesMut::new(),
        }
    }
}

impl<K, C, A> BatchService for Service<K, C, A>
where
    K: Codec<MetricBatch>,
    C: Compressor,
    A: WalAppender,
{
    fn batch(
        &mut self,
        points: &[RawPoint],
        stats: &mut BatchStats,
    ) -> Result<WireBatch, BatchError> {
        let point_count = points.len();

        let batch = self.assembler.assemble(self.run_id, points);

        self.encode_buf.clear();
        self.codec.encode(&batch, &mut self.encode_buf)?;

        self.assembler.reclaim(batch);

        let uncompressed_size = self.encode_buf.len();

        self.compress_buf.clear();
        self.compressor
            .compress(&self.encode_buf, &mut self.compress_buf)?;

        let crc = crc32fast::hash(&self.compress_buf);

        let wire = WireBatch {
            run_id: self.run_id,
            sequence_number: self.next_sequence,
            point_count,
            uncompressed_size,
            compressed_payload: self.compress_buf.clone().freeze(),
            crc32: crc,
            created_at: SystemTime::now(),
        };

        self.wal.append(&wire)?;
        self.next_sequence = self.next_sequence.next();

        stats.batches_created += 1;
        stats.points_batched += point_count as u64;
        stats.bytes_compressed += wire.compressed_size() as u64;
        stats.bytes_uncompressed += uncompressed_size as u64;

        Ok(wire)
    }
}
