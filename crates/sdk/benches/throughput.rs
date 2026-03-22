use std::time::Instant;

use photon_wal::WalKind;

fn main() {
    let points: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000);

    let mut run = photon::Run::builder()
        .wal(WalKind::Memory)
        .max_points_per_batch(10_000)
        .channel_capacity(1_000_000)
        .start()
        .expect("failed to start run");

    // Warmup
    for step in 0..100_000u64 {
        let loss = 1.0 / (1.0 + step as f64 * 0.05);
        run.log("train/loss", loss, step).unwrap();
        run.log("train/accuracy", 1.0 - loss, step).unwrap();
    }
    let _ = run.finish();

    // Measured run
    let mut run = photon::Run::builder()
        .wal(WalKind::Memory)
        .max_points_per_batch(10_000)
        .channel_capacity(1_000_000)
        .start()
        .expect("failed to start run");

    let t0 = Instant::now();

    for step in 0..points {
        let loss = 1.0 / (1.0 + step as f64 * 0.05);
        let accuracy = 1.0 - loss;

        run.log("train/loss", loss, step).unwrap();
        run.log("train/accuracy", accuracy, step).unwrap();

        if step % 10 == 0 {
            let lr = 0.001 * 0.95_f64.powi(step as i32 / 10);
            run.log("train/lr", lr, step).unwrap();
        }

        if step % 50 == 0 {
            run.log("val/loss", loss * 1.1, step).unwrap();
        }
    }

    let log_elapsed = t0.elapsed();
    let stats = run.finish().expect("finish failed");
    let total_elapsed = t0.elapsed();

    let log_throughput = stats.points as f64 / log_elapsed.as_secs_f64();
    let total_throughput = stats.points as f64 / total_elapsed.as_secs_f64();

    eprintln!("\n--- SDK Benchmark ---");
    eprintln!("Points:             {}", stats.points);
    eprintln!("Batches:            {}", stats.batches);
    eprintln!("Points dropped:     {}", stats.points_dropped);
    eprintln!("Log:                {log_elapsed:.2?}");
    eprintln!("Total:              {total_elapsed:.2?}");
    eprintln!("Throughput (log):   {:.2} M pts/s", log_throughput / 1_000_000.0);
    eprintln!("Throughput (total): {:.2} M pts/s", total_throughput / 1_000_000.0);
}
