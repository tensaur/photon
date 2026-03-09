use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use bytes::BytesMut;

use photon_core::types::id::RunId;
use photon_core::types::query::{MetricQuery, MetricSeries, QueryRequest, QueryResponse};
use photon_protocol::ports::codec::Codec;

use crate::domain::service::QueryService;

struct AppState<S, C> {
    service: S,
    codec: C,
}

pub fn router<S, C>(service: S, codec: C) -> Router
where
    S: QueryService + Send + Sync + 'static,
    C: Codec<MetricQuery>
        + Codec<MetricSeries>
        + Codec<QueryRequest>
        + Codec<QueryResponse>
        + 'static,
{
    let state = Arc::new(AppState { service, codec });

    Router::new()
        .route("/api/v1/runs/{run_id}/metrics", get(list_metrics::<S, C>))
        .route("/api/v1/query", post(query::<S, C>))
        .route("/api/v1/query/batch", post(query_batch::<S, C>))
        .with_state(state)
}

async fn list_metrics<S, C>(
    State(state): State<Arc<AppState<S, C>>>,
    Path(run_id): Path<RunId>,
) -> impl IntoResponse
where
    S: QueryService + Send + Sync + 'static,
    C: Send + Sync + 'static,
{
    match state.service.list_metrics(&run_id).await {
        Ok(keys) => {
            let names: Vec<&str> = keys.iter().map(|k| k.as_str()).collect();
            (StatusCode::OK, serde_json::to_vec(&names).unwrap()).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn query<S, C>(
    State(state): State<Arc<AppState<S, C>>>,
    body: Bytes,
) -> impl IntoResponse
where
    S: QueryService + Send + Sync + 'static,
    C: Codec<MetricQuery> + Codec<MetricSeries> + 'static,
{
    let q: MetricQuery = match state.codec.decode(&body) {
        Ok(q) => q,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let series: MetricSeries = match state.service.query(&q).await {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let mut buf = BytesMut::new();
    match Codec::<MetricSeries>::encode(&state.codec, &series, &mut buf) {
        Ok(()) => (StatusCode::OK, buf.freeze()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn query_batch<S, C>(
    State(state): State<Arc<AppState<S, C>>>,
    body: Bytes,
) -> impl IntoResponse
where
    S: QueryService + Send + Sync + 'static,
    C: Codec<QueryRequest> + Codec<QueryResponse> + 'static,
{
    let request: QueryRequest = match state.codec.decode(&body) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };

    let response: QueryResponse = match state.service.query_batch(&request).await {
        Ok(r) => r,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };

    let mut buf = BytesMut::new();
    match Codec::<QueryResponse>::encode(&state.codec, &response, &mut buf) {
        Ok(()) => (StatusCode::OK, buf.freeze()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
