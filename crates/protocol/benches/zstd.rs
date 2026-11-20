//! zstd compression of encoded metric batches through the public
//! `Compressor` API. Runs natively and on `wasm32-wasip1` under wasmtime.

use std::hint::black_box;

use bytes::BytesMut;
use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};

use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, MetricBatch, MetricPoint, Step};
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::{CompressorKind, ZstdCompressor};
use photon_protocol::ports::codec::Codec;
use photon_protocol::ports::compress::Compressor;

/// Points per batch: a timer flush, a busy flush, and the default
/// `BatchConfig::max_points`.
const BATCH_POINTS: &[usize] = &[100, 1_000, 10_000];

const RUN_ID: &str = "0192f3c4-5a6b-7c8d-9e0f-a1b2c3d4e5f6";

/// Logged every step.
const TRAIN_METRICS: &[&str] = &[
    "train/loss",
    "train/accuracy",
    "train/lr",
    "train/grad_norm",
    "train/tokens_per_sec",
    "sys/gpu_mem_gb",
];

/// Logged every `EVAL_EVERY` steps.
const EVAL_METRICS: &[&str] = &["val/loss", "val/accuracy"];
const EVAL_EVERY: u64 = 50;

/// `SplitMix64` with a fixed seed, identical on every target.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1_u64 << 53) as f64
    }

    /// Roughly normal in [-1.5, 1.5).
    fn noise(&mut self) -> f64 {
        self.unit() + self.unit() + self.unit() - 1.5
    }
}

/// A training run's metric stream: noisy decaying loss, rising accuracy,
/// a warmup and decay learning rate, throughput jitter, and a near-constant
/// memory gauge. Uses only exactly rounded float ops, so batches match
/// across targets.
fn metric_batch(points: usize) -> MetricBatch {
    let keys: Vec<Metric> = TRAIN_METRICS
        .iter()
        .chain(EVAL_METRICS)
        .map(|name| Metric::new_unchecked(*name))
        .collect();
    let mut rng = Rng(0x7068_6f74_6f6e ^ points as u64);
    let mut batch = MetricBatch {
        run_id: RUN_ID.parse::<RunId>().expect("valid run id"),
        keys,
        points: Vec::with_capacity(points),
    };

    let mut step = 12_000_u64;
    let mut timestamp_ms = 1_731_000_000_000_u64;
    let mut gpu_mem_gb = 37.25;
    while batch.points.len() < points {
        let s = step as f64;
        let loss = 0.35 + 2.5 / (1.0 + s / 2_000.0);
        let mut values = vec![
            loss + 0.04 * rng.noise(),
            (0.95 - 0.6 / (1.0 + s / 1_500.0) + 0.01 * rng.noise()).min(1.0),
            3e-4 * (s / 2_000.0).min(1.0) * (1.0 - s / 200_000.0),
            0.8 + 0.25 * rng.noise() + if rng.unit() < 0.01 { 4.0 } else { 0.0 },
            (48_000.0 + 900.0 * rng.noise()).round(),
            gpu_mem_gb,
        ];
        if step.is_multiple_of(EVAL_EVERY) {
            values.push(loss + 0.05 + 0.01 * rng.noise());
            values.push(0.94 - 0.6 / (1.0 + s / 1_500.0) + 0.005 * rng.noise());
        }
        if rng.unit() < 0.002 {
            gpu_mem_gb += 0.25;
        }

        for (key_index, value) in values.into_iter().enumerate() {
            if batch.points.len() == points {
                break;
            }
            batch.points.push(MetricPoint {
                key_index: key_index as u32,
                value,
                step: Step::new(step),
                timestamp_ms: timestamp_ms + key_index as u64 / 3,
            });
        }
        step += 1;
        timestamp_ms += 330 + rng.next_u64() % 40;
    }
    batch
}

fn bench_zstd(c: &mut Criterion) {
    let codec = CodecKind::default();
    // The SDK batcher's default: a fresh `ZstdCompressor` per call.
    let kind = CompressorKind::default();
    // One long-lived compressor, as the server holds it.
    let reused = ZstdCompressor::default();

    let mut group = c.benchmark_group("zstd");
    for &points in BATCH_POINTS {
        let mut raw = BytesMut::new();
        codec.encode(&metric_batch(points), &mut raw).unwrap();
        let raw = raw.freeze();

        let mut compressed = BytesMut::new();
        kind.compress(&raw, &mut compressed).unwrap();
        let compressed = compressed.freeze();
        let mut decoded = BytesMut::new();
        reused.decompress(&compressed, &mut decoded).unwrap();
        assert!(decoded == raw, "zstd round trip changed the batch");
        eprintln!(
            "zstd {points} points: {} -> {} bytes ({:.2}x)",
            raw.len(),
            compressed.len(),
            raw.len() as f64 / compressed.len() as f64,
        );

        group.throughput(Throughput::Bytes(raw.len() as u64));
        bench_compressor(&mut group, "kind", &kind, points, &raw, &compressed);
        bench_compressor(&mut group, "reused", &reused, points, &raw, &compressed);
    }
    group.finish();
}

fn bench_compressor<C: Compressor>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    name: &str,
    compressor: &C,
    points: usize,
    raw: &[u8],
    compressed: &[u8],
) {
    group.bench_with_input(
        BenchmarkId::new(format!("compress/{name}"), points),
        raw,
        |b, raw| {
            let mut output = BytesMut::with_capacity(raw.len());
            b.iter(|| {
                output.clear();
                compressor.compress(black_box(raw), &mut output).unwrap();
            });
        },
    );
    group.bench_with_input(
        BenchmarkId::new(format!("decompress/{name}"), points),
        compressed,
        |b, compressed| {
            let mut output = BytesMut::with_capacity(raw.len());
            b.iter(|| {
                output.clear();
                compressor
                    .decompress(black_box(compressed), &mut output)
                    .unwrap();
            });
        },
    );
}

criterion_group!(benches, bench_zstd);
criterion_main!(benches);
