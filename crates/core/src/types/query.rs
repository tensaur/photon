use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::types::id::RunId;
use crate::types::metric::Metric;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricQuery {
    pub run_id: RunId,
    pub key: Metric,
    pub step_range: Range<u64>,
    pub target_points: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricSeries {
    pub run_id: RunId,
    pub key: Metric,
    pub data: SeriesData,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SeriesData {
    Raw {
        points: Vec<DataPoint>,
    },
    Aggregated {
        points: Vec<DataPoint>,
        envelope: Vec<RangePoint>,
    },
}

impl SeriesData {
    pub fn points(&self) -> &[DataPoint] {
        match self {
            Self::Raw { points } | Self::Aggregated { points, .. } => points,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataPoint {
    pub step: u64,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RangePoint {
    pub step_start: u64,
    pub step_end: u64,
    pub min: f64,
    pub max: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryRequest {
    pub queries: Vec<MetricQuery>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    pub series: Vec<MetricSeries>,
}

/// Envelope for query requests over a transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum QueryMessage {
    ListRuns,
    ListMetrics(RunId),
    Query(MetricQuery),
    QueryBatch(QueryRequest),
}

/// Envelope for query responses over a transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum QueryResult {
    Runs(Vec<RunId>),
    Metrics(Vec<Metric>),
    Series(MetricSeries),
    BatchResponse(QueryResponse),
    Error(String),
}
