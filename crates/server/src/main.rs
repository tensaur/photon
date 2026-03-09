use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

use photon_hook::noop::NoOpHook;
use photon_ingest::domain::service::Service;
use photon_ingest::inbound::grpc::Handler;
use photon_protocol::codec::protobuf::codec::ProtobufCodec;
use photon_protocol::compressor::noop::NoopCompressor;
use photon_store::memory::metric::InMemoryMetricStore;
use photon_store::memory::watermark::InMemoryWatermarkStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let addr = "[::1]:50051".parse()?;

    let watermark_store = InMemoryWatermarkStore::new();
    let metric_store = InMemoryMetricStore::new();
    let hook = NoOpHook;
    let compressor = NoopCompressor;
    let codec = ProtobufCodec;

    let ingest_service = Service::new(watermark_store, metric_store, hook, compressor, codec);
    let grpc_handler = Handler::new(ingest_service);

    tracing::info!("photon server listening on {addr}");

    Server::builder()
        .add_service(grpc_handler.into_server())
        .serve(addr)
        .await?;

    Ok(())
}
