use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::Run;
use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::query::{
    MetricQuery, MetricSeries, QueryMessage, QueryRequest, QueryResponse, QueryResult,
};
use photon_transport::Transport;

use crate::domain::error::{
    ListExperimentsError, ListMetricsError, ListProjectsError, ListRunsError, QueryMetricsError,
};
use crate::domain::ports::MetricQuerier;

#[derive(Debug, Clone)]
pub struct HttpQuerier<T> {
    transport: T,
}

impl<T> HttpQuerier<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }
}

impl<T> MetricQuerier for HttpQuerier<T>
where
    T: Transport<QueryMessage, QueryResult>,
{
    async fn list_runs(&self) -> Result<Vec<Run>, ListRunsError> {
        self.transport
            .send(&QueryMessage::ListRuns)
            .await
            .map_err(|e| ListRunsError::Unknown(Box::new(e)))?;

        let result = self
            .transport
            .recv()
            .await
            .map_err(|e| ListRunsError::Unknown(Box::new(e)))?;

        match result {
            QueryResult::Runs(runs) => Ok(runs),
            QueryResult::Error(msg) => Err(ListRunsError::Unknown(msg.into())),
            other => Err(ListRunsError::Unknown(
                format!("unexpected response: {other:?}").into(),
            )),
        }
    }

    async fn list_experiments(&self) -> Result<Vec<Experiment>, ListExperimentsError> {
        self.transport
            .send(&QueryMessage::ListExperiments)
            .await
            .map_err(|e| ListExperimentsError::Unknown(Box::new(e)))?;

        let result = self
            .transport
            .recv()
            .await
            .map_err(|e| ListExperimentsError::Unknown(Box::new(e)))?;

        match result {
            QueryResult::Experiments(experiments) => Ok(experiments),
            QueryResult::Error(msg) => Err(ListExperimentsError::Unknown(msg.into())),
            other => Err(ListExperimentsError::Unknown(
                format!("unexpected response: {other:?}").into(),
            )),
        }
    }

    async fn list_projects(&self) -> Result<Vec<Project>, ListProjectsError> {
        self.transport
            .send(&QueryMessage::ListProjects)
            .await
            .map_err(|e| ListProjectsError::Unknown(Box::new(e)))?;

        let result = self
            .transport
            .recv()
            .await
            .map_err(|e| ListProjectsError::Unknown(Box::new(e)))?;

        match result {
            QueryResult::Projects(projects) => Ok(projects),
            QueryResult::Error(msg) => Err(ListProjectsError::Unknown(msg.into())),
            other => Err(ListProjectsError::Unknown(
                format!("unexpected response: {other:?}").into(),
            )),
        }
    }

    async fn list_metrics(&self, run_id: &RunId) -> Result<Vec<Metric>, ListMetricsError> {
        self.transport
            .send(&QueryMessage::ListMetrics(*run_id))
            .await
            .map_err(|e| ListMetricsError::Unknown(Box::new(e)))?;

        let result = self
            .transport
            .recv()
            .await
            .map_err(|e| ListMetricsError::Unknown(Box::new(e)))?;

        match result {
            QueryResult::Metrics(metrics) => Ok(metrics),
            QueryResult::Error(msg) => Err(ListMetricsError::Unknown(msg.into())),
            other => Err(ListMetricsError::Unknown(
                format!("unexpected response: {other:?}").into(),
            )),
        }
    }

    async fn query(&self, q: &MetricQuery) -> Result<MetricSeries, QueryMetricsError> {
        self.transport
            .send(&QueryMessage::Query(q.clone()))
            .await
            .map_err(|e| QueryMetricsError::Unknown(Box::new(e)))?;

        let result = self
            .transport
            .recv()
            .await
            .map_err(|e| QueryMetricsError::Unknown(Box::new(e)))?;

        match result {
            QueryResult::Series(series) => Ok(series),
            QueryResult::Error(msg) => Err(QueryMetricsError::Unknown(msg.into())),
            other => Err(QueryMetricsError::Unknown(
                format!("unexpected response: {other:?}").into(),
            )),
        }
    }

    async fn query_batch(
        &self,
        request: &QueryRequest,
    ) -> Result<QueryResponse, QueryMetricsError> {
        self.transport
            .send(&QueryMessage::QueryBatch(request.clone()))
            .await
            .map_err(|e| QueryMetricsError::Unknown(Box::new(e)))?;

        let result = self
            .transport
            .recv()
            .await
            .map_err(|e| QueryMetricsError::Unknown(Box::new(e)))?;

        match result {
            QueryResult::BatchResponse(response) => Ok(response),
            QueryResult::Error(msg) => Err(QueryMetricsError::Unknown(msg.into())),
            other => Err(QueryMetricsError::Unknown(
                format!("unexpected response: {other:?}").into(),
            )),
        }
    }
}
