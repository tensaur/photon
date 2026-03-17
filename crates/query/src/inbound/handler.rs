use std::sync::Arc;

use crate::domain::message::{QueryMessage, QueryResult};
use photon_transport::ports::{Transport, TransportError};

use crate::domain::service::QueryService;

/// Transport-agnostic query handler.
pub async fn handle_request<S, T>(service: &Arc<S>, transport: &T)
where
    S: QueryService + Send + Sync + 'static,
    T: Transport<QueryResult, QueryMessage>,
{
    loop {
        let msg = match transport.recv().await {
            Ok(msg) => msg,
            Err(TransportError::StreamClosed(_)) => break,
            Err(e) => {
                tracing::warn!("query transport error: {e}");
                break;
            }
        };

        let result = match msg {
            QueryMessage::ListMetrics(run_id) => match service.list_metrics(&run_id).await {
                Ok(metrics) => QueryResult::Metrics(metrics),
                Err(e) => QueryResult::Error(e.to_string()),
            },
            QueryMessage::Query(query) => match service.query(&query).await {
                Ok(series) => QueryResult::Series(series),
                Err(e) => QueryResult::Error(e.to_string()),
            },
            QueryMessage::QueryBatch(request) => match service.query_batch(&request).await {
                Ok(response) => QueryResult::BatchResponse(response),
                Err(e) => QueryResult::Error(e.to_string()),
            },
        };

        if transport.send(&result).await.is_err() {
            break;
        }
    }
}
