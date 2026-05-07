use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() {
    let mut run = photon::Run::builder()
        .endpoint("[::1]:50051")
        .max_points_per_batch(10_000)
        .channel_capacity(10_000_000)
        .start()
        .expect("failed to start run");

    println!("Run: {}", run.id());

    let mut rng_state: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x9E37_79B9_7F4A_7C15)
        | 1;
    let mut noise = || -> f64 {
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        (rng_state as f64) / (u64::MAX as f64)
    };

    let n: u64 = 10_000_000;
    let decay_tau = (n as f64) * (0.05 + 0.25 * noise());
    let loss_floor = 0.02 + 0.15 * noise();
    let loss_scale = 0.5 + 0.6 * noise();
    let loss_noise = 0.005 + 0.05 * noise();
    let acc_noise = 0.005 + 0.03 * noise();
    let lr_peak = 1e-4 + 5e-3 * noise();
    let lr_period = (n as f64) * (0.02 + 0.12 * noise());

    for step in 0..n {
        let t = step as f64;

        let loss = loss_scale * (-t / decay_tau).exp()
            + loss_floor
            + loss_noise * (noise() - 0.5);
        run.log("train/loss", loss, step).unwrap();

        if step % 10 == 0 {
            let accuracy = (1.0 - loss + acc_noise * (noise() - 0.5)).clamp(0.0, 1.0);
            run.log("train/accuracy", accuracy, step).unwrap();
        }

        if step % 100 == 0 {
            let cycle = (t % lr_period) / lr_period;
            let lr = 1e-4 + 0.5 * (lr_peak - 1e-4) * (1.0 + (std::f64::consts::PI * cycle).cos());
            run.log("train/lr", lr, step).unwrap();

            thread::sleep(Duration::from_millis(1));
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
