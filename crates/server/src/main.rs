use std::sync::Arc;

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use photon_hook::noop::NoOpHook;
use photon_ingest::domain::service::Service;
use photon_ingest::inbound::handler;
use photon_protocol::codec::CodecChoice;
use photon_protocol::compressor::zstd::ZstdCompressor;
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

    let addr = "[::1]:50051";

    let watermark_store = InMemoryWatermarkStore::new();
    let metric_store = InMemoryMetricStore::new();
    let hook = NoOpHook;
    let compressor = ZstdCompressor::default();
    let codec = CodecChoice::default();
    let wire_codec = CodecChoice::default();

    let ingest_service = Arc::new(Service::new(
        watermark_store,
        metric_store,
        hook,
        compressor,
        codec,
    ));

    let listener = TcpListener::bind(addr).await?;
    tracing::info!("photon server listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        tracing::debug!("accepted connection from {peer}");

        let transport = TcpTransport::from_stream(stream, wire_codec.clone());
        let service = Arc::clone(&ingest_service);

        tokio::spawn(async move {
            handler::handle_stream(&service, &transport).await;
            tracing::debug!("connection from {peer} closed");
        });
    }
}
