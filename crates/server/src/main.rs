use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use photon_downsample::selector::noop::NoOpSelector;
use photon_ingest::domain::service::Service as IngestService;
use photon_ingest::inbound::handler as ingest_handler;
use photon_persist::domain::service::Service as PersistService;
use photon_persist::inbound::thread as persist_thread;
use photon_persist::inbound::thread::PersistConfig;
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::ZstdCompressor;
use photon_query::domain::service::Service as QueryService;
use photon_query::domain::tier::TierSelector;
use photon_query::inbound::handler as query_handler;
use photon_store::clickhouse::bucket::ClickHouseBucketStore;
use photon_store::clickhouse::compaction::ClickHouseCompactionCursor;
use photon_store::clickhouse::metric::ClickHouseMetricStore;
use photon_store::clickhouse::watermark::ClickHouseWatermarkStore;
use photon_store::clickhouse::{ClientBuilder, migrate};
use photon_store::ports::watermark::WatermarkWriter;
use photon_transport::codec::CodecTransport;
use photon_transport::http::HttpTransport;
use photon_transport::serve;
use photon_transport::tcp::TcpTransport;
use photon_wal::{DiskWalConfig, open_disk_wal};

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
    let cancel = CancellationToken::new();
    let notify = Arc::new(tokio::sync::Notify::new());

    // ClickHouse
    let client = ClientBuilder::new().with_env().build();
    migrate(&client).await?;

    let metric_store = ClickHouseMetricStore::new(client.clone());
    let bucket_store = ClickHouseBucketStore::new(client.clone());
    let compaction_cursor = ClickHouseCompactionCursor::new(client.clone());
    let watermark_store = ClickHouseWatermarkStore::new(client);

    // Server WAL (same implementation as client, different config)
    let (wal_appender, wal_manager) =
        open_disk_wal(".photon/server-wal", DiskWalConfig::default())?;

    // Ingest hexagon (WAL-backed)
    let ingest_service = Arc::new(IngestService::new(wal_appender, notify.clone()));

    // Seed dedup cache from persisted watermarks
    let watermarks = watermark_store.read_all().await?;
    if !watermarks.is_empty() {
        tracing::info!(
            runs = watermarks.len(),
            "seeded dedup cache from watermarks"
        );
        ingest_service.seed_watermarks(&watermarks);
    }

    // Persist hexagon
    let persist_service = PersistService::new(
        ZstdCompressor::default(),
        codec,
        metric_store.clone(),
        watermark_store,
    );
    let persist_handle = tokio::spawn(persist_thread::run(
        wal_manager,
        notify,
        persist_service,
        PersistConfig::default(),
        cancel.clone(),
    ));

    // Query service
    let query_service = Arc::new(QueryService::new(
        NoOpSelector,
        bucket_store,
        metric_store,
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
            ingest_handler::handle(&svc, &transport).await;
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
    cancel.cancel();
    let _ = persist_handle.await;

    Ok(())
}
