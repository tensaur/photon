fn main() {
    let mut run = photon::Run::builder()
        .endpoint("[::1]:50051")
        .max_points_per_batch(50)
        .start()
        .expect("failed to start run");

    println!("Run: {}", run.id());

    // Simulate a training loop
    for step in 0..200 {
        let loss = 1.0 / (1.0 + step as f64 * 0.05);
        let accuracy = 1.0 - loss;

        run.log("train/loss", loss, step).unwrap();
        run.log("train/accuracy", accuracy, step).unwrap();

        if step % 10 == 0 {
            let lr = 0.001 * 0.95_f64.powi(step as i32 / 10);
            run.log("train/lr", lr, step).unwrap();
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

    assert!(stats.batches > 0);
    assert_eq!(stats.points, 420);
    assert_eq!(stats.points_dropped, 0);

    println!("\nAll checks passed!");
}
