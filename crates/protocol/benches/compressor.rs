use bytes::BytesMut;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch, MetricPoint};
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::CompressorKind;
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;

const BATCH_SIZES: &[usize] = &[100, 1_000, 10_000, 100_000];

const METRIC_NAMES: &[&str] = &[
    "train/loss",
    "train/accuracy",
    "train/lr",
    "val/loss",
    "val/accuracy",
];

fn make_metric_batch(run_id: RunId, n: usize) -> MetricBatch {
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
    MetricBatch {
        run_id,
        keys,
        points,
    }
}

fn encode_batch(codec: &CodecKind, run_id: RunId, size: usize) -> Vec<u8> {
    let batch = make_metric_batch(run_id, size);
    let mut buf = BytesMut::new();
    codec.encode(&batch, &mut buf).unwrap();
    buf.to_vec()
}

fn bench_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("compressor/compress");
    let codec = CodecKind::default();

    for &size in BATCH_SIZES {
        let raw = encode_batch(&codec, RunId::new(), size);

        for compressor in CompressorKind::all_variants() {
            let id = BenchmarkId::new(compressor.name(), size);
            group.bench_with_input(id, &raw, |b, raw| {
                b.iter_batched(
                    || BytesMut::with_capacity(raw.len()),
                    |mut buf| {
                        compressor.compress(raw, &mut buf).unwrap();
                        buf
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

fn bench_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("compressor/decompress");
    let codec = CodecKind::default();

    for &size in BATCH_SIZES {
        let raw = encode_batch(&codec, RunId::new(), size);

        for compressor in CompressorKind::all_variants() {
            let mut compressed = BytesMut::new();
            compressor.compress(&raw, &mut compressed).unwrap();
            let compressed = compressed.freeze();

            let id = BenchmarkId::new(compressor.name(), size);
            group.bench_with_input(id, &compressed, |b, compressed| {
                b.iter_batched(
                    || BytesMut::with_capacity(raw.len()),
                    |mut buf| {
                        compressor.decompress(compressed, &mut buf).unwrap();
                        buf
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_compress, bench_decompress);
criterion_main!(benches);
