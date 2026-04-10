use std::ops::Range;

use serde::{Deserialize, Serialize};

use crate::domain::experiment::Experiment;
use crate::domain::project::Project;
use crate::domain::run::Run;
use crate::types::bucket::Bucket;
use crate::types::error::ApiError;
use crate::types::id::RunId;
use crate::types::metric::{Metric, Step};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricQuery {
    pub run_id: RunId,
    pub key: Metric,
    pub step_range: Range<Step>,
    pub target_points: usize,
    #[serde(default)]
    pub subscribe: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricSeries {
    pub run_id: RunId,
    pub key: Metric,
    pub data: SeriesData,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SeriesData {
    Raw { points: Vec<DataPoint> },
    Bucketed { buckets: Vec<Bucket> },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DataPoint {
    pub step: Step,
    pub value: f64,
}

impl From<&DataPoint> for [f64; 2] {
    fn from(p: &DataPoint) -> Self {
        [p.step.as_u64() as f64, p.value]
    }
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
    ListExperiments,
    ListProjects,
    ListMetrics(RunId),
    Query(MetricQuery),
    QueryBatch(QueryRequest),
    Unsubscribe(SubscriptionId),
    IsFinalized(RunId),
}

/// Envelope for query responses over a transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum QueryResult {
    Runs(Vec<Run>),
    Experiments(Vec<Experiment>),
    Projects(Vec<Project>),
    Metrics(Vec<Metric>),
    Series(MetricSeries),
    BatchResponse(QueryResponse),
    Finalized { run_id: RunId, finalized: bool },
    Error(ApiError),
}
