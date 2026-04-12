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
use photon_persist::domain::service::Service as PersistService;
use photon_persist::inbound::{PersistConfig, thread as persist_thread};
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
    let (finished_runs_tx, finished_runs_rx) = tokio::sync::mpsc::unbounded_channel();

    // ClickHouse
    let client = ClientBuilder::new().with_env().build();
    migrate(&client).await?;

    let metric_store = ClickHouseMetricStore::new(client.clone());
    let bucket_store = ClickHouseBucketStore::new(client.clone());
    let watermark_store = ClickHouseWatermarkStore::new(client.clone());
    let run_store = ClickHouseRunStore::new(client.clone());
    let experiment_store = ClickHouseExperimentStore::new(client.clone());
    let project_store = ClickHouseProjectStore::new(client.clone());
    let finalised_store =
        photon_store::clickhouse::finalised::ClickHouseFinalisedStore::new(client);
    let (event_tx, _) = tokio::sync::broadcast::channel::<PhotonEvent>(256);

    // Server WAL
    let (wal_appender, wal_manager) =
        open_disk_wal(".photon/server-wal", DiskWalConfig::default())?;

    // Ingest hexagon
    let ingest_service = IngestService::new(
        wal_appender,
        notify.clone(),
        run_store,
        experiment_store,
        project_store,
        event_tx.clone(),
        finished_runs_tx,
    );

    // Seed dedup cache from persisted watermarks + unconsumed WAL tail
    ingest_service.seed(&watermark_store, &wal_manager).await;

    // Persist consumer
    let persist_service = PersistService::new(
        ZstdCompressor::default(),
        codec,
        metric_store,
        watermark_store,
        bucket_store,
        finalised_store,
        event_tx,
        DownsampleConfig::default(),
    );
    let persist_cancel = cancel.clone();
    let persist_handle = tokio::spawn(persist_thread::run(
        wal_manager,
        notify,
        persist_service,
        finished_runs_rx,
        PersistConfig::default(),
        persist_cancel,
    ));

    // Spawn server in background
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept failed");
            let bt = TcpTransport::accept(stream);
            let transport = CodecTransport::new(codec, bt);
            let svc = ingest_service.clone();
            tokio::spawn(async move { handler::handle_envelope(&svc, &transport).await });
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

        // Simulate a training loop with realistic noise
        let mut rng_state: u64 = 42;
        let mut noise = || -> f64 {
            // xorshift64 → uniform [0,1)
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            (rng_state as f64) / (u64::MAX as f64)
        };

        for step in 0..1_000_000u64 {
            let t = step as f64;

            // Exponential decay with noisy plateaus and a spike around step 400k
            let base_loss = 0.8 * (-t / 150_000.0).exp() + 0.1;
            let spike = 0.3 * (-(((t - 400_000.0) / 20_000.0).powi(2))).exp();
            let loss = base_loss + spike + 0.02 * (noise() - 0.5);

            let accuracy = (1.0 - base_loss + 0.015 * (noise() - 0.5)).clamp(0.0, 1.0);

            run.log("train/loss", loss, step).unwrap();
            run.log("train/accuracy", accuracy, step).unwrap();

            if step % 10 == 0 {
                // Cosine-annealed learning rate with warm restarts every 200k steps
                let cycle = (t % 200_000.0) / 200_000.0;
                let lr = 1e-4 + 0.5 * (1e-3 - 1e-4) * (1.0 + (std::f64::consts::PI * cycle).cos());
                run.log("train/lr", lr, step).unwrap();
            }

            if step % 50 == 0 {
                let val_loss = base_loss * 1.15 + spike * 1.3 + 0.03 * (noise() - 0.5);
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
