use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use photon_core::types::event::PhotonEvent;
use photon_downsample::selector::noop::NoOpSelector;
use photon_hook::Hook;
use photon_ingest::domain::service::Service as IngestService;
use photon_ingest::inbound::handler as ingest_handler;
use photon_persist::domain::projections::downsample::DownsampleConfig;
use photon_persist::domain::service::Service as PersistService;
use photon_persist::inbound::{PersistConfig, thread as persist_thread};
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::ZstdCompressor;
use photon_query::domain::service::Service as QueryService;
use photon_query::domain::tier::TierSelector;
use photon_query::inbound::handler as query_handler;
use photon_store::clickhouse::bucket::ClickHouseBucketStore;
use photon_store::clickhouse::compaction::ClickHouseCompactionCursor;
use photon_store::clickhouse::experiment::ClickHouseExperimentStore;
use photon_store::clickhouse::metric::ClickHouseMetricStore;
use photon_store::clickhouse::project::ClickHouseProjectStore;
use photon_store::clickhouse::run::ClickHouseRunStore;
use photon_store::clickhouse::watermark::ClickHouseWatermarkStore;
use photon_store::clickhouse::{ClientBuilder, migrate};
use photon_subscription::SubscriptionHook;
use photon_transport::router::Router;
use photon_wal::{DiskWalConfig, open_disk_wal};

#[cfg(feature = "dashboard")]
mod dashboard;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let ingest_addr = std::env::var("PHOTON_INGEST_ADDR").unwrap_or_else(|_| "[::1]:50051".into());
    let api_addr = std::env::var("PHOTON_API_ADDR").unwrap_or_else(|_| "[::1]:50052".into());

    let codec = CodecKind::default();
    let cancel = CancellationToken::new();
    let notify = Arc::new(tokio::sync::Notify::new());
    let (finished_runs_tx, finished_runs_rx) = tokio::sync::mpsc::unbounded_channel();

    // ClickHouse
    let client = ClientBuilder::new().with_env().build();
    migrate(&client).await?;

    let metric_store = ClickHouseMetricStore::new(client.clone());
    let bucket_store = ClickHouseBucketStore::new(client.clone());
    let compaction_cursor = ClickHouseCompactionCursor::new(client.clone());
    let watermark_store = ClickHouseWatermarkStore::new(client.clone());
    let run_store = ClickHouseRunStore::new(client.clone());
    let experiment_store = ClickHouseExperimentStore::new(client.clone());
    let project_store = ClickHouseProjectStore::new(client);

    // Pipeline event channel
    let (event_tx, _) = tokio::sync::broadcast::channel::<PhotonEvent>(256);

    // Subscription hook (maps pipeline events → WebSocket subscription events)
    let subscription = SubscriptionHook::new();
    subscription.spawn(event_tx.subscribe());

    // Server WAL
    let (wal_appender, wal_manager) =
        open_disk_wal(".photon/server-wal", DiskWalConfig::default())?;

    // Ingest hexagon (WAL-backed)
    let ingest_service = IngestService::new(wal_appender, notify.clone());

    // Seed dedup cache from persisted watermarks + unconsumed WAL tail
    ingest_service.seed(&watermark_store, &wal_manager).await;

    // Persist hexagon
    let persist_service = PersistService::new(
        ZstdCompressor::default(),
        codec,
        metric_store.clone(),
        watermark_store,
        bucket_store.clone(),
        event_tx,
        DownsampleConfig::default(),
    );
    let persist_handle = tokio::spawn(persist_thread::run(
        wal_manager,
        notify,
        persist_service,
        finished_runs_rx,
        PersistConfig::default(),
        cancel.clone(),
    ));

    let query_service = QueryService::new(
        NoOpSelector,
        bucket_store,
        metric_store,
        compaction_cursor,
        run_store.clone(),
        experiment_store.clone(),
        project_store.clone(),
        TierSelector::default(),
    );

    let ingest_listener = TcpListener::bind(&ingest_addr).await?;
    tracing::info!("ingest listening on {ingest_addr}");

    let ingest_handle = tokio::spawn(photon_transport::serve(
        ingest_listener,
        codec,
        cancel.clone(),
        move |t| {
            let svc = ingest_service.clone();
            let run_store = run_store.clone();
            let experiment_store = experiment_store.clone();
            let project_store = project_store.clone();
            let finished_runs_tx = finished_runs_tx.clone();

            async move {
                ingest_handler::handle_envelope(
                    &svc,
                    &run_store,
                    &experiment_store,
                    &project_store,
                    &finished_runs_tx,
                    &t,
                )
                .await;
            }
        },
    ));

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

    let api_listener = TcpListener::bind(&api_addr).await?;
    tracing::info!("api listening on http://{api_addr}");

    let api_cancel = cancel.clone();
    let api_handle = tokio::spawn(async move { router.serve(api_listener, api_cancel).await });

    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    cancel.cancel();
    let _ = tokio::join!(persist_handle, ingest_handle, api_handle);

    Ok(())
}
