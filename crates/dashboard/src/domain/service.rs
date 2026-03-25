use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::query::{MetricQuery, MetricSeries, QueryRequest, QueryResponse};

use crate::domain::error::{
    ListMetricsError, ListRunsError, QueryMetricsError, SubscribeError, UnsubscribeError,
};
use crate::domain::ports::{MetricQuerier, MetricSubscriber};

#[cfg(not(target_arch = "wasm32"))]
pub trait DashboardService: Send + Sync + 'static {
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

    fn subscribe(&self, run_id: &RunId) -> impl Future<Output = Result<(), SubscribeError>> + Send;

    fn unsubscribe(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<(), UnsubscribeError>> + Send;
}

#[cfg(target_arch = "wasm32")]
pub trait DashboardService: 'static {
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

    fn subscribe(&self, run_id: &RunId) -> impl Future<Output = Result<(), SubscribeError>>;

    fn unsubscribe(&self, run_id: &RunId) -> impl Future<Output = Result<(), UnsubscribeError>>;
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
    async fn list_runs(&self) -> Result<Vec<RunId>, ListRunsError> {
        self.querier.list_runs().await
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

    async fn subscribe(&self, run_id: &RunId) -> Result<(), SubscribeError> {
        self.subscriber.subscribe(run_id).await
    }

    async fn unsubscribe(&self, run_id: &RunId) -> Result<(), UnsubscribeError> {
        self.subscriber.unsubscribe(run_id).await
    }
}
