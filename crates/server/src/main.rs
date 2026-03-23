use std::sync::Arc;

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use photon_downsample::selector::noop::NoOpSelector;
use photon_hook::noop::NoOpHook;
use photon_ingest::domain::service::Service as IngestService;
use photon_ingest::inbound::handler as ingest_handler;
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::ZstdCompressor;
use photon_query::domain::service::Service as QueryService;
use photon_query::domain::tier::TierSelector;
use photon_query::inbound::handler as query_handler;
use photon_store::clickhouse::ClickHouseStore;
use photon_transport::codec::CodecTransport;
use photon_transport::http::HttpTransport;
use photon_transport::serve;
use photon_transport::tcp::TcpTransport;

#[cfg(feature = "dashboard")]
mod dashboard;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let ingest_addr = "[::1]:50051";
    let query_addr = "[::1]:50052";

    let codec = CodecKind::default();

    let store = ClickHouseStore::from_env();
    let watermark_store = store.clone();
    let metric_writer = store.clone();
    let bucket_reader = store.clone();
    let metric_reader = store.clone();
    let compaction_cursor = store.clone();

    // Ingest service
    let ingest_service = Arc::new(IngestService::new(
        watermark_store,
        metric_writer,
        NoOpHook,
        ZstdCompressor::default(),
        codec,
    ));

    // Query service
    let query_service = Arc::new(QueryService::new(
        NoOpSelector,
        bucket_reader,
        metric_reader,
        compaction_cursor,
        TierSelector::default(),
    ));

    // Ingest (TCP)
    let ingest_listener = TcpListener::bind(ingest_addr).await?;
    tracing::info!("ingest listening on {ingest_addr}");

    tokio::spawn(serve(
        ingest_listener,
        ingest_service,
        move |svc, stream| async move {
            let bt = TcpTransport::accept(stream);
            let transport = CodecTransport::new(codec, bt);
            ingest_handler::handle_stream(&svc, &transport).await;
        },
    ));

    // Query (HTTP)
    let query_listener = TcpListener::bind(query_addr).await?;
    tracing::info!("query listening on http://{query_addr}/query");

    tokio::spawn(serve(
        query_listener,
        query_service,
        move |svc, stream| async move {
            let bt = match HttpTransport::accept(stream).await {
                Ok(bt) => bt,
                Err(e) => {
                    tracing::warn!("HTTP accept error: {e}");
                    return;
                }
            };

            let transport = CodecTransport::new(codec, bt);
            query_handler::handle(&svc, &transport).await;
        },
    ));

    // Dashboard
    #[cfg(feature = "dashboard")]
    {
        let dashboard_listener = TcpListener::bind("[::1]:50053").await?;
        tracing::info!("dashboard at http://[::1]:50053");
        tokio::spawn(dashboard::serve(dashboard_listener));
    }

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    store.flush().await;

    Ok(())
}
