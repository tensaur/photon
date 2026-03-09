use std::ops::Range;

use crate::types::id::RunId;
use crate::types::metric::Metric;

#[derive(Clone, Debug)]
pub struct MetricQuery {
    pub run_id: RunId,
    pub key: Metric,
    pub step_range: Range<u64>,
    pub target_points: usize,
}

#[derive(Clone, Debug)]
pub struct MetricSeries {
    pub run_id: RunId,
    pub key: Metric,
    pub data: SeriesData,
}

#[derive(Clone, Debug)]
pub enum SeriesData {
    Raw {
        points: Vec<DataPoint>,
    },
    Aggregated {
        points: Vec<DataPoint>,
        envelope: Vec<RangePoint>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct DataPoint {
    pub step: u64,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RangePoint {
    pub step: u64,
    pub min: f64,
    pub max: f64,
}

#[derive(Clone, Debug)]
pub struct QueryRequest {
    pub queries: Vec<MetricQuery>,
}

#[derive(Clone, Debug)]
pub struct QueryResponse {
    pub series: Vec<MetricSeries>,
}
