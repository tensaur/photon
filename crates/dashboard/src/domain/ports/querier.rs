use std::future::Future;

use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::Run;
use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::query::{MetricQuery, MetricSeries, QueryRequest, QueryResponse};

use crate::domain::error::{
    ListExperimentsError, ListMetricsError, ListProjectsError, ListRunsError, QueryMetricsError,
};

#[cfg(not(target_arch = "wasm32"))]
pub trait MetricQuerier: Clone + Send + Sync + 'static {
    fn list_runs(&self) -> impl Future<Output = Result<Vec<Run>, ListRunsError>> + Send;

    fn list_experiments(
        &self,
    ) -> impl Future<Output = Result<Vec<Experiment>, ListExperimentsError>> + Send;

    fn list_projects(&self)
    -> impl Future<Output = Result<Vec<Project>, ListProjectsError>> + Send;

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

    fn is_finalized(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<bool, QueryMetricsError>> + Send;
}

#[cfg(target_arch = "wasm32")]
pub trait MetricQuerier: Clone + 'static {
    fn list_runs(&self) -> impl Future<Output = Result<Vec<Run>, ListRunsError>>;

    fn list_experiments(
        &self,
    ) -> impl Future<Output = Result<Vec<Experiment>, ListExperimentsError>>;

    fn list_projects(&self) -> impl Future<Output = Result<Vec<Project>, ListProjectsError>>;

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

    fn is_finalized(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<bool, QueryMetricsError>>;
}
