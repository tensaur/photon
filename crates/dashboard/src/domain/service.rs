use std::future::Future;

use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::Run;
use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::id::SubscriptionId;
use photon_core::types::query::{MetricQuery, MetricSeries, QueryRequest, QueryResponse};

use crate::domain::error::{
    ListExperimentsError, ListMetricsError, ListProjectsError, ListRunsError,
    QueryMetricsError, SubscribeError, UnsubscribeError,
};
use crate::domain::ports::{MetricQuerier, MetricSubscriber};

#[cfg(not(target_arch = "wasm32"))]
pub trait DashboardService: Clone + Send + Sync + 'static {
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

    fn subscribe(
        &self,
        query: &MetricQuery,
    ) -> impl Future<Output = Result<(), SubscribeError>> + Send;

    fn unsubscribe(
        &self,
        sub_id: SubscriptionId,
    ) -> impl Future<Output = Result<(), UnsubscribeError>> + Send;

    fn is_finalized(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<bool, QueryMetricsError>> + Send;
}

#[cfg(target_arch = "wasm32")]
pub trait DashboardService: 'static {
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

    fn subscribe(
        &self,
        query: &MetricQuery,
    ) -> impl Future<Output = Result<(), SubscribeError>>;

    fn unsubscribe(
        &self,
        sub_id: SubscriptionId,
    ) -> impl Future<Output = Result<(), UnsubscribeError>>;

    fn is_finalized(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<bool, QueryMetricsError>>;
}

#[derive(Debug, Clone)]
pub struct Service<Q, S>
where
    Q: MetricQuerier,
    S: MetricSubscriber,
{
    querier: Q,
    subscriber: S,
}

impl<Q, S> Service<Q, S>
where
    Q: MetricQuerier,
    S: MetricSubscriber,
{
    pub fn new(querier: Q, subscriber: S) -> Self {
        Self {
            querier,
            subscriber,
        }
    }
}

impl<Q, S> DashboardService for Service<Q, S>
where
    Q: MetricQuerier,
    S: MetricSubscriber,
{
    async fn list_runs(&self) -> Result<Vec<Run>, ListRunsError> {
        self.querier.list_runs().await
    }

    async fn list_experiments(&self) -> Result<Vec<Experiment>, ListExperimentsError> {
        self.querier.list_experiments().await
    }

    async fn list_projects(&self) -> Result<Vec<Project>, ListProjectsError> {
        self.querier.list_projects().await
    }

    async fn list_metrics(&self, run_id: &RunId) -> Result<Vec<Metric>, ListMetricsError> {
        self.querier.list_metrics(run_id).await
    }

    async fn query(&self, q: &MetricQuery) -> Result<MetricSeries, QueryMetricsError> {
        self.querier.query(q).await
    }

    async fn query_batch(
        &self,
        request: &QueryRequest,
    ) -> Result<QueryResponse, QueryMetricsError> {
        self.querier.query_batch(request).await
    }

    async fn subscribe(&self, query: &MetricQuery) -> Result<(), SubscribeError> {
        self.subscriber.subscribe(query).await
    }

    async fn unsubscribe(&self, sub_id: SubscriptionId) -> Result<(), UnsubscribeError> {
        self.subscriber.unsubscribe(sub_id).await
    }

    async fn is_finalized(&self, run_id: &RunId) -> Result<bool, QueryMetricsError> {
        self.querier.is_finalized(run_id).await
    }
}
