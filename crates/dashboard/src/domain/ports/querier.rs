use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::query::{MetricQuery, MetricSeries, QueryRequest, QueryResponse};

use crate::domain::error::{ListMetricsError, ListRunsError, QueryMetricsError};

#[cfg(not(target_arch = "wasm32"))]
pub trait MetricQuerier: Clone + Send + Sync + 'static {
    fn list_runs(&self) -> impl Future<Output = Result<Vec<RunId>, ListRunsError>> + Send;

    fn list_metrics(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<Metric>, ListMetricsError>> + Send;

    fn query(
        &self,
        q: &MetricQuery,
    ) -> impl Future<Output = Result<MetricSeries, QueryMetricsError>> + Send;

    fn query_batch(
        &self,
        request: &QueryRequest,
    ) -> impl Future<Output = Result<QueryResponse, QueryMetricsError>> + Send;
}

#[cfg(target_arch = "wasm32")]
pub trait MetricQuerier: Clone + 'static {
    fn list_runs(&self) -> impl Future<Output = Result<Vec<RunId>, ListRunsError>>;

    fn list_metrics(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<Metric>, ListMetricsError>>;

    fn query(
        &self,
        q: &MetricQuery,
    ) -> impl Future<Output = Result<MetricSeries, QueryMetricsError>>;

    fn query_batch(
        &self,
        request: &QueryRequest,
    ) -> impl Future<Output = Result<QueryResponse, QueryMetricsError>>;
}
