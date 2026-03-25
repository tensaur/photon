use std::sync::Arc;

use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use photon_downsample::selector::noop::NoOpSelector;
use photon_ingest::domain::service::Service as IngestService;
use photon_ingest::inbound::handler as ingest_handler;
use photon_protocol::codec::CodecKind;
use photon_protocol::compressor::ZstdCompressor;
use photon_query::domain::service::Service as QueryService;
use photon_query::domain::tier::TierSelector;
use photon_query::inbound::handler as query_handler;
use photon_store::memory::bucket::InMemoryBucketStore;
use photon_store::memory::compaction::InMemoryCompactionCursor;
use photon_store::memory::experiment::InMemoryExperimentStore;
use photon_store::memory::metric::InMemoryMetricStore;
use photon_store::memory::project::InMemoryProjectStore;
use photon_store::memory::run::InMemoryRunStore;
use photon_store::memory::watermark::InMemoryWatermarkStore;
use photon_subscription::SubscriptionHook;
use photon_transport::router::Router;

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

    let metric_store = InMemoryMetricStore::new();
    let bucket_store = InMemoryBucketStore::new();
    let compaction_cursor = InMemoryCompactionCursor::new();
    let watermark_store = InMemoryWatermarkStore::new();
    let run_store = InMemoryRunStore::new();
    let experiment_store = InMemoryExperimentStore::new();
    let project_store = InMemoryProjectStore::new();
    let subscription = SubscriptionHook::new();

    let ingest_service = Arc::new(IngestService::new(
        watermark_store,
        metric_store.clone(),
        subscription.clone(),
        ZstdCompressor::default(),
        codec,
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

    Ok(())
}
