//! End-to-end wire example: spawns the ingest server and SDK client in a single process.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::net::TcpListener;

use photon_hook::noop::NoOpHook;
use photon_ingest::domain::service::Service as IngestService;
use photon_ingest::inbound::handler;
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::CompressorKind;
use photon_store::memory::metric::InMemoryMetricStore;
use photon_store::memory::watermark::InMemoryWatermarkStore;
use photon_transport::codec::CodecTransport;
use photon_transport::tcp::TcpTransport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("[::1]:0").await?;
    let addr: SocketAddr = listener.local_addr()?;
    println!("Server listening on {addr}");

    let codec = CodecKind::default();
    let compressor = CompressorKind::default();

    let watermark_store = InMemoryWatermarkStore::new();
    let metric_store = InMemoryMetricStore::new();

    let ingest_service = Arc::new(IngestService::new(
        watermark_store,
        metric_store,
        NoOpHook,
        compressor,
        codec,
    ));

    // Spawn server in background
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept failed");
            let bt = TcpTransport::accept(stream);
            let transport = CodecTransport::new(codec, bt);
            let service = Arc::clone(&ingest_service);

            tokio::spawn(async move {
                handler::handle_stream(&service, &transport).await;
            });
        }
    });

    // Give the server a moment to start accepting connections
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let endpoint = format!("[::1]:{}", addr.port());
    let client_handle = tokio::task::spawn_blocking(move || {
        let mut run = photon::Run::builder()
            .endpoint(endpoint)
            .max_points_per_batch(10_000)
            .channel_capacity(1_000_000)
            .compressor(compressor)
            .codec(codec)
            .start()
            .expect("failed to start run");

        println!("Run: {}", run.id());

        let t0 = Instant::now();

        // Simulate a training loop
        for step in 0..10_000_000u64 {
            let loss = 1.0 / (1.0 + step as f64 * 0.05);
            let accuracy = 1.0 - loss;

            run.log("train/loss", loss, step).unwrap();
            run.log("train/accuracy", accuracy, step).unwrap();

            if step % 10 == 0 {
                let lr = 0.001 * 0.95_f64.powi(step as i32 / 10);
                run.log("train/lr", lr, step).unwrap();
            }

            if step % 50 == 0 {
                let val_loss = loss * 1.1;
                run.log("val/loss", val_loss, step).unwrap();
            }
        }

        let log_elapsed = t0.elapsed();
        println!(
            "Logged: {} points in {:.2?}",
            run.points_logged(),
            log_elapsed
        );

        let stats = run.finish().expect("finish failed");
        let total_elapsed = t0.elapsed();

        println!("\n--- Pipeline Stats ---");
        println!("Points logged:    {}", stats.points);
        println!("Points dropped:   {}", stats.points_dropped);
        println!("Batches flushed:  {}", stats.batches);
        println!("Bytes (raw):      {}", stats.bytes_uncompressed);
        println!("Bytes (wire):     {}", stats.bytes_compressed);
        println!("Batches sent:     {}", stats.batches_sent);
        println!("Batches acked:    {}", stats.batches_acked);
        println!("Batches rejected: {}", stats.batches_rejected);

        println!("\n--- Timing ---");
        println!("Log phase:        {:.2?}", log_elapsed);
        println!("Total (inc flush):{:.2?}", total_elapsed);
        println!(
            "Throughput (log): {:.2} M pts/s",
            stats.points as f64 / log_elapsed.as_secs_f64() / 1_000_000.0
        );
        println!(
            "Throughput (e2e): {:.2} M pts/s",
            stats.points as f64 / total_elapsed.as_secs_f64() / 1_000_000.0
        );

        assert!(stats.batches_sent > 0, "expected batches to be sent");
        assert!(stats.batches_acked > 0, "expected batches to be acked");
        assert_eq!(stats.points_dropped, 0, "expected zero drops");

        println!("\nEnd-to-end test passed!");
    });

    client_handle.await?;

    Ok(())
}
