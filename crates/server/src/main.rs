use std::sync::Arc;

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use photon_downsample::selector::noop::NoOpSelector;
use photon_hook::noop::NoOpHook;
use photon_ingest::domain::service::Service as IngestService;
use photon_ingest::inbound::handler as ingest_handler;
use photon_protocol::codec::CodecChoice;
use photon_protocol::compressor::zstd::ZstdCompressor;
use photon_query::domain::service::Service as QueryService;
use photon_query::domain::tier::TierSelector;
use photon_query::inbound::handler as query_handler;
use photon_store::memory::bucket::InMemoryBucketStore;
use photon_store::memory::compaction::InMemoryCompactionCursor;
use photon_store::memory::metric::InMemoryMetricStore;
use photon_store::memory::watermark::InMemoryWatermarkStore;
use photon_transport::tcp::TcpTransport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let ingest_addr = "[::1]:50051";
    let query_addr = "[::1]:50052";
    let wire_codec = CodecChoice::default();

    // Shared stores
    let metric_store = InMemoryMetricStore::new();
    let bucket_store = InMemoryBucketStore::new();
    let compaction_cursor = InMemoryCompactionCursor::new();
    let watermark_store = InMemoryWatermarkStore::new();

    // Ingest service
    let ingest_service = Arc::new(IngestService::new(
        watermark_store,
        metric_store.clone(),
        NoOpHook,
        ZstdCompressor::default(),
        CodecChoice::default(),
    ));

    // Query service
    let query_service = Arc::new(QueryService::new(
        NoOpSelector,
        bucket_store,
        metric_store,
        compaction_cursor,
        TierSelector::default(),
    ));

    // Ingest listener
    let ingest_listener = TcpListener::bind(ingest_addr).await?;
    tracing::info!("ingest listening on {ingest_addr}");

    let ingest_wire = wire_codec.clone();
    let ingest_svc = Arc::clone(&ingest_service);
    tokio::spawn(async move {
        loop {
            let (stream, peer) = ingest_listener.accept().await.expect("accept failed");
            tracing::debug!("ingest connection from {peer}");

            let transport = TcpTransport::from_stream(stream, ingest_wire.clone());
            let service = Arc::clone(&ingest_svc);

            tokio::spawn(async move {
                ingest_handler::handle_stream(&service, &transport).await;
                tracing::debug!("ingest connection from {peer} closed");
            });
        }
    });

    // Query listener
    let query_listener = TcpListener::bind(query_addr).await?;
    tracing::info!("query listening on {query_addr}");

    let query_wire = wire_codec.clone();
    let query_svc = Arc::clone(&query_service);
    tokio::spawn(async move {
        loop {
            let (stream, peer) = query_listener.accept().await.expect("accept failed");
            tracing::debug!("query connection from {peer}");

            let transport = TcpTransport::from_stream(stream, query_wire.clone());
            let service = Arc::clone(&query_svc);

            tokio::spawn(async move {
                query_handler::handle_request(&service, &transport).await;
                tracing::debug!("query connection from {peer} closed");
            });
        }
    });

    // Wait for shutdown
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");

    Ok(())
}
