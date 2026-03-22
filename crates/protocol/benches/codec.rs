use bytes::BytesMut;
use criterion::{BenchmarkId, BatchSize, Criterion, criterion_group, criterion_main};

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch, MetricPoint};
use photon_protocol::codec::CodecKind;
use photon_protocol::ports::codec::Codec;

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

fn bench_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec/encode");
    let run_id = RunId::new();

    for codec in CodecKind::all_variants() {
        for &size in BATCH_SIZES {
            let batch = make_metric_batch(run_id, size);
            let id = BenchmarkId::new(codec.name(), size);

            group.bench_with_input(id, &batch, |b, batch| {
                b.iter_batched(
                    || BytesMut::with_capacity(size * 32),
                    |mut buf| {
                        codec.encode(batch, &mut buf).unwrap();
                        buf
                    },
                    BatchSize::SmallInput,
                );
            });
        }
    }

    group.finish();
}

fn bench_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec/decode");
    let run_id = RunId::new();

    for codec in CodecKind::all_variants() {
        for &size in BATCH_SIZES {
            let batch = make_metric_batch(run_id, size);
            let mut buf = BytesMut::new();
            codec.encode(&batch, &mut buf).unwrap();
            let encoded = buf.freeze();
            let id = BenchmarkId::new(codec.name(), size);

            group.bench_with_input(id, &encoded, |b, encoded| {
                b.iter(|| {
                    let _: MetricBatch = codec.decode(encoded).unwrap();
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
