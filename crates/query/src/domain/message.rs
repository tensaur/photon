use serde::{Deserialize, Serialize};

use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::query::{MetricQuery, MetricSeries, QueryRequest, QueryResponse};

/// Envelope for query requests over a transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum QueryMessage {
    ListMetrics(RunId),
    Query(MetricQuery),
    QueryBatch(QueryRequest),
}

/// Envelope for query responses over a transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum QueryResult {
    Metrics(Vec<Metric>),
    Series(MetricSeries),
    BatchResponse(QueryResponse),
    Error(String),
}
