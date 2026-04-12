use std::thread;
use std::time::Duration;

fn main() {
    let mut run = photon::Run::builder()
        .endpoint("[::1]:50051")
        .max_points_per_batch(50)
        .start()
        .expect("failed to start run");

    println!("Run: {}", run.id());

    // Simulate a training loop
    let total_steps: u64 = 50_000;
    let mut rng_state: u64 = 42;
    let mut noise = || -> f64 {
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        (rng_state as f64) / (u64::MAX as f64)
    };

    for step in 0..total_steps {
        let t = step as f64;

        // Exponential decay with noise
        let loss = 0.9 * (-t / 20_000.0).exp() + 0.05 + 0.02 * (noise() - 0.5);
        let accuracy = (1.0 - loss + 0.015 * (noise() - 0.5)).clamp(0.0, 1.0);

        run.log("train/loss", loss, step).unwrap();
        run.log("train/accuracy", accuracy, step).unwrap();

        if step % 10 == 0 {
            let cycle = (t % 10_000.0) / 10_000.0;
            let lr =
                1e-4 + 0.5 * (1e-3 - 1e-4) * (1.0 + (std::f64::consts::PI * cycle).cos());
            run.log("train/lr", lr, step).unwrap();
        }

        if step % 100 == 0 {
            thread::sleep(Duration::from_millis(60));
        }
    }

    println!("Logged: {} points", run.points_logged());

    let stats = run.finish().expect("finish failed");

    println!("\n--- Results ---");
    println!("Points:       {}", stats.points);
    println!("Dropped:      {}", stats.points_dropped);
    println!("Batches:      {}", stats.batches);
    println!("Bytes (raw):  {}", stats.bytes_uncompressed);
    println!("Bytes (wire): {}", stats.bytes_compressed);
    println!("Sent:         {}", stats.batches_sent);
    println!("Acked:        {}", stats.batches_acked);

    println!("\nDone!");
}
