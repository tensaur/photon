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
use photon_store::memory::experiment::InMemoryExperimentStore;
use photon_store::memory::project::InMemoryProjectStore;
use photon_store::memory::run::InMemoryRunStore;
use photon_store::ports::watermark::WatermarkReader;
use photon_subscription::SubscriptionHook;
use photon_transport::router::Router;
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
    let api_addr = "[::1]:50052";

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

    // Entity stores (in-memory for now)
    let run_store = InMemoryRunStore::new();
    let experiment_store = InMemoryExperimentStore::new();
    let project_store = InMemoryProjectStore::new();
    let subscription = SubscriptionHook::new();

    // Server WAL
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

    let query_service = Arc::new(QueryService::new(
        NoOpSelector,
        bucket_store,
        metric_store,
        compaction_cursor,
        run_store.clone(),
        experiment_store.clone(),
        project_store.clone(),
        TierSelector::default(),
    ));

    let ingest_listener = TcpListener::bind(ingest_addr).await?;
    tracing::info!("ingest listening on {ingest_addr}");

    tokio::spawn(photon_transport::serve(ingest_listener, codec, move |t| {
        let svc = ingest_service.clone();
        let run_store = run_store.clone();
        let experiment_store = experiment_store.clone();
        let project_store = project_store.clone();
        async move {
            ingest_handler::handle_envelope(&svc, &run_store, &experiment_store, &project_store, &t)
                .await
        }
    }));

    let router = Router::new(codec)
        .request_response("/api/query", move |t| {
            let svc = query_service.clone();
            async move { query_handler::handle(&svc, &t).await }
        })
        .websocket("/api/ws", move |t| {
            let events = subscription.subscribe();
            async move { photon_subscription::handle(&t, events).await }
        });

    #[cfg(feature = "dashboard")]
    let router = router.fallback(dashboard::get_file);

    let api_listener = TcpListener::bind(api_addr).await?;
    tracing::info!("api listening on http://{api_addr}");

    tokio::spawn(async move { router.serve(api_listener).await });

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    cancel.cancel();
    let _ = persist_handle.await;

    Ok(())
}
