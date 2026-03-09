//! Spawns the ingest server and SDK client in a single process.

use std::net::SocketAddr;
use std::time::Duration;

use tonic::transport::Server;

use photon_hook::noop::NoOpHook;
use photon_ingest::domain::service::Service as IngestService;
use photon_ingest::inbound::grpc::Handler;
use photon_protocol::codec::protobuf::codec::ProtobufCodec;
use photon_protocol::compressor::noop::NoopCompressor;
use photon_store::memory::metric::InMemoryMetricStore;
use photon_store::memory::watermark::InMemoryWatermarkStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("[::1]:0").await?;
    let addr: SocketAddr = listener.local_addr()?;
    println!("Server listening on {addr}");

    let watermark_store = InMemoryWatermarkStore::new();
    let metric_store = InMemoryMetricStore::new();
    let ingest_service =
        IngestService::new(watermark_store, metric_store, NoOpHook, NoopCompressor, ProtobufCodec);
    let grpc_handler = Handler::new(ingest_service);

    // Spawn server in background
    tokio::spawn(async move {
        Server::builder()
            .add_service(grpc_handler.into_server())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("server crashed");
    });

    // Give the server a moment to start accepting connections
    tokio::time::sleep(Duration::from_millis(200)).await;

    let endpoint = format!("http://{addr}");
    let client_handle = tokio::task::spawn_blocking(move || {
        let mut run = photon::Run::builder()
            .endpoint(endpoint)
            .max_points_per_batch(10_000)
            .channel_capacity(1_000_000)
            .start()
            .expect("failed to start run");

        println!("Run: {}", run.id());

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

        println!("Logged: {} points", run.points_logged());

        let stats = run.finish().expect("finish failed");

        println!("\n--- Pipeline Stats ---");
        println!("Points logged:    {}", stats.points);
        println!("Points dropped:   {}", stats.points_dropped);
        println!("Batches flushed:  {}", stats.batches);
        println!("Bytes (raw):      {}", stats.bytes_uncompressed);
        println!("Bytes (wire):     {}", stats.bytes_compressed);
        println!("Batches sent:     {}", stats.batches_sent);
        println!("Batches acked:    {}", stats.batches_acked);

        assert!(stats.batches_sent > 0, "expected batches to be sent");
        assert!(stats.batches_acked > 0, "expected batches to be acked");
        assert_eq!(stats.points_dropped, 0, "expected zero drops");

        println!("\nEnd-to-end test passed!");
    });

    client_handle.await?;

    Ok(())
}
