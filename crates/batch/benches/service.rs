use std::sync::Arc;

use criterion::{BenchmarkId, BatchSize, Criterion, criterion_group, criterion_main};
use lasso::ThreadedRodeo;

use photon_batch::domain::service::{BatchService, Service};
use photon_batch::domain::types::BatchStats;
use photon_core::types::id::RunId;
use photon_core::types::metric::{MetricKey, RawPoint};
use photon_core::types::sequence::SequenceNumber;
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::CompressorKind;
use photon_wal::memory::open_in_memory_wal;

const BATCH_SIZES: &[usize] = &[100, 1_000, 10_000, 100_000];

const METRIC_NAMES: &[&str] = &[
    "train/loss",
    "train/accuracy",
    "train/lr",
    "val/loss",
    "val/accuracy",
];

fn make_raw_points(interner: &Arc<ThreadedRodeo>, n: usize) -> Vec<RawPoint> {
    let keys: Vec<MetricKey> = METRIC_NAMES
        .iter()
        .map(|name| {
            let spur = interner.get_or_intern(name);
            MetricKey::new(lasso::Key::into_usize(spur))
        })
        .collect();
    let num_keys = keys.len();
    (0..n)
        .map(|i| RawPoint {
            key: keys[i % num_keys],
            value: 1.0 / (1.0 + i as f64 * 0.001),
            step: i as u64,
            timestamp_ns: i as u64 * 1_000_000,
        })
        .collect()
}

fn bench_service(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch/service");
    let run_id = RunId::new();
    let interner = Arc::new(ThreadedRodeo::default());

    for &size in BATCH_SIZES {
        let points = make_raw_points(&interner, size);
        let id = BenchmarkId::from_parameter(size);

        group.bench_with_input(id, &points, |b, points| {
            b.iter_batched(
                || {
                    let (wal_appender, _wal_manager) = open_in_memory_wal();
                    let service = Service::new(
                        run_id,
                        Arc::clone(&interner),
                        CodecKind::default(),
                        CompressorKind::default(),
                        wal_appender,
                        SequenceNumber::ZERO,
                    );
                    let stats = BatchStats::default();
                    (service, stats)
                },
                |(mut service, mut stats)| {
                    service.batch(points, &mut stats).unwrap()
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_service);
criterion_main!(benches);
