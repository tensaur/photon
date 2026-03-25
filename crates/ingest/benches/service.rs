use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use bytes::BytesMut;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};

use photon_core::types::batch::WireBatch;
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch, MetricPoint};
use photon_core::types::sequence::SequenceNumber;
use photon_ingest::domain::service::{IngestService, Service};
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::CompressorKind;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;
use photon_store::memory::watermark::InMemoryWatermarkStore;
use photon_wal::open_in_memory_wal;

const BATCH_SIZES: &[usize] = &[100, 1_000, 10_000, 100_000];

const METRIC_NAMES: &[&str] = &[
    "train/loss",
    "train/accuracy",
    "train/lr",
    "val/loss",
    "val/accuracy",
];

fn make_wire_batch(
    run_id: RunId,
    n: usize,
    seq: SequenceNumber,
    compressor: &impl Compressor,
    codec: &impl Codec<MetricBatch>,
) -> WireBatch {
    let keys: Vec<Metric> = METRIC_NAMES
        .iter()
        .map(|name| Metric::new_unchecked(*name))
        .collect();
    let num_keys = keys.len();
    let points = (0..n)
        .map(|i| MetricPoint {
            key_index: (i % num_keys) as u32,
            value: 1.0 / (1.0 + i as f64 * 0.001),
            step: i as u64,
            timestamp_ms: i as u64,
        })
        .collect();
    let batch = MetricBatch {
        run_id,
        keys,
        points,
    };

    let mut encode_buf = BytesMut::new();
    codec.encode(&batch, &mut encode_buf).unwrap();
    let uncompressed_size = encode_buf.len();

    let mut compress_buf = BytesMut::new();
    compressor.compress(&encode_buf, &mut compress_buf).unwrap();
    let crc = crc32fast::hash(&compress_buf);

    WireBatch {
        run_id,
        sequence_number: seq,
        compressed_payload: compress_buf.freeze(),
        crc32: crc,
        created_at: SystemTime::now(),
        point_count: n,
        uncompressed_size,
    }
}

fn bench_service(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest/service");
    let rt = tokio::runtime::Runtime::new().unwrap();
    let codec = CodecKind::default();
    let compressor = CompressorKind::default();

    for &size in BATCH_SIZES {
        let id = BenchmarkId::from_parameter(size);
        let run_id = RunId::new();
        let seq_counter = AtomicU64::new(0);

        let (wal_appender, _wal) = open_in_memory_wal();
        let notify = Arc::new(tokio::sync::Notify::new());

        let service = Service::new(InMemoryWatermarkStore::new(), wal_appender, notify);

        group.bench_function(id, |b| {
            b.iter_batched(
                || {
                    let seq = seq_counter.fetch_add(1, Ordering::Relaxed);
                    make_wire_batch(run_id, size, SequenceNumber::from(seq), &compressor, &codec)
                },
                |wire_batch| rt.block_on(service.ingest(&wire_batch)).unwrap(),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_service);
criterion_main!(benches);
