//! End-to-end wire example: spawns the ingest server and SDK client in a single process.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use photon_core::types::event::PhotonEvent;
use photon_ingest::domain::service::Service as IngestService;
use photon_ingest::inbound::handler;
use photon_persist::domain::projections::downsample::DownsampleConfig;
use photon_persist::domain::service::{PersistConfig, Service as PersistService};
use photon_persist::inbound::thread as persist_thread;
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::CompressorKind;
use photon_protocol::compressor::ZstdCompressor;
use photon_store::clickhouse::bucket::ClickHouseBucketStore;
use photon_store::clickhouse::experiment::ClickHouseExperimentStore;
use photon_store::clickhouse::metric::ClickHouseMetricStore;
use photon_store::clickhouse::project::ClickHouseProjectStore;
use photon_store::clickhouse::run::ClickHouseRunStore;
use photon_store::clickhouse::watermark::ClickHouseWatermarkStore;
use photon_store::clickhouse::{ClientBuilder, migrate};
use photon_transport::codec::CodecTransport;
use photon_transport::tcp::TcpTransport;
use photon_wal::{DiskWalConfig, open_disk_wal};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let listener = TcpListener::bind("[::1]:0").await?;
    let addr: SocketAddr = listener.local_addr()?;
    println!("Server listening on {addr}");

    let codec = CodecKind::default();
    let compressor = CompressorKind::default();
    let cancel = CancellationToken::new();
    let notify = Arc::new(tokio::sync::Notify::new());

    // ClickHouse
    let client = ClientBuilder::new().with_env().build();
    migrate(&client).await?;

    let metric_store = ClickHouseMetricStore::new(client.clone());
    let bucket_store = ClickHouseBucketStore::new(client.clone());
    let watermark_store = ClickHouseWatermarkStore::new(client.clone());
    let run_store = ClickHouseRunStore::new(client.clone());
    let experiment_store = ClickHouseExperimentStore::new(client.clone());
    let project_store = ClickHouseProjectStore::new(client);
    let (event_tx, _) = tokio::sync::broadcast::channel::<PhotonEvent>(256);

    // Server WAL
    let (wal_appender, wal_manager) =
        open_disk_wal(".photon/server-wal", DiskWalConfig::default())?;

    // Ingest service (WAL-backed)
    let ingest_service = IngestService::new(wal_appender, notify.clone());

    // Seed dedup cache from persisted watermarks + unconsumed WAL tail
    ingest_service.seed(&watermark_store, &wal_manager).await;

    // Persist consumer
    let persist_service = PersistService::new(
        ZstdCompressor::default(),
        codec,
        metric_store,
        watermark_store,
        bucket_store,
        event_tx,
        DownsampleConfig::default(),
    );
    let persist_cancel = cancel.clone();
    let persist_handle = tokio::spawn(persist_thread::run(
        wal_manager,
        notify,
        persist_service,
        PersistConfig::default(),
        persist_cancel,
    ));

    // Spawn server in background
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept failed");
            let bt = TcpTransport::accept(stream);
            let transport = CodecTransport::new(codec, bt);
            let service = ingest_service.clone();
            let run_store = run_store.clone();
            let experiment_store = experiment_store.clone();
            let project_store = project_store.clone();

            tokio::spawn(async move {
                handler::handle_envelope(
                    &service,
                    &run_store,
                    &experiment_store,
                    &project_store,
                    &transport,
                )
                .await;
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
        for step in 0..100_000_000u64 {
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

    cancel.cancel();
    persist_handle.await?;

    println!("Done.");
    Ok(())
}
