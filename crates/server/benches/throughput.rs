use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use tokio::net::TcpListener;

use photon_ingest::domain::service::Service as IngestService;
use photon_ingest::inbound::handler;
use photon_protocol::codec::CodecKind;
use photon_store::memory::experiment::InMemoryExperimentStore;
use photon_store::memory::project::InMemoryProjectStore;
use photon_store::memory::run::InMemoryRunStore;
use photon_transport::codec::CodecTransport;
use photon_transport::tcp::TcpTransport;
use photon_wal::open_in_memory_wal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let points: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000_000);

    let codec = CodecKind::default();

    let listener = TcpListener::bind("[::1]:0").await?;
    let addr: SocketAddr = listener.local_addr()?;

    let (wal_appender, _wal) = open_in_memory_wal();
    let notify = Arc::new(tokio::sync::Notify::new());

    let ingest_service = IngestService::new(wal_appender, notify);
    let run_store = InMemoryRunStore::new();
    let experiment_store = InMemoryExperimentStore::new();
    let project_store = InMemoryProjectStore::new();

    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept failed");
            let bt = TcpTransport::accept(stream);
            let transport = CodecTransport::new(codec, bt);
            let service = ingest_service.clone();
            let rs = run_store.clone();
            let es = experiment_store.clone();
            let ps = project_store.clone();

            tokio::spawn(async move {
                handler::handle_envelope(&service, &rs, &es, &ps, &transport).await;
            });
        }
    });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let endpoint = format!("[::1]:{}", addr.port());
    tokio::task::spawn_blocking(move || {
        // Warmup
        let mut warmup = photon::Run::builder()
            .endpoint(&endpoint)
            .max_points_per_batch(10_000)
            .channel_capacity(1_000_000)
            .start()
            .expect("failed to start warmup run");

        for step in 0..100_000u64 {
            let loss = 1.0 / (1.0 + step as f64 * 0.05);
            warmup.log("train/loss", loss, step).unwrap();
            warmup.log("train/accuracy", 1.0 - loss, step).unwrap();
        }
        let _ = warmup.finish();

        // Measured run
        let mut run = photon::Run::builder()
            .endpoint(&endpoint)
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
        let compression_ratio = if stats.bytes_compressed > 0 {
            stats.bytes_uncompressed as f64 / stats.bytes_compressed as f64
        } else {
            0.0
        };

        eprintln!("\n--- E2E Benchmark ---");
        eprintln!("Points:             {}", stats.points);
        eprintln!("Batches:            {}", stats.batches);
        eprintln!("Batches acked:      {}", stats.batches_acked);
        eprintln!("Points dropped:     {}", stats.points_dropped);
        eprintln!("Compression:        {compression_ratio:.2}x");
        eprintln!("Log:                {log_elapsed:.2?}");
        eprintln!("Total:              {total_elapsed:.2?}");
        eprintln!(
            "Throughput (log):   {:.2} M pts/s",
            log_throughput / 1_000_000.0
        );
        eprintln!(
            "Throughput (total): {:.2} M pts/s",
            total_throughput / 1_000_000.0
        );
    })
    .await?;

    Ok(())
}
