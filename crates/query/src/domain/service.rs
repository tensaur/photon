use std::future::Future;

use photon_core::domain::experiment::Experiment;
use photon_core::domain::project::Project;
use photon_core::domain::run::Run;
use photon_core::types::error::ApiError;
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, Step};
use photon_core::types::query::{
    DataPoint, MetricQuery, MetricSeries, QueryMessage, QueryRequest, QueryResponse, QueryResult,
    RangePoint, SeriesData,
};
use photon_downsample::ports::selector::Selector;
use photon_store::ports::ReadError;
use photon_store::ports::ReadRepository;
use photon_store::ports::bucket::BucketReader;
use photon_store::ports::compaction::CompactionCursor;
use photon_store::ports::metric::MetricReader;

use crate::domain::tier::{Resolution, TierSelector};

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("store read failed")]
    Read(#[from] ReadError),
}

pub trait QueryService: Clone + Send + Sync + 'static {
    fn list_runs(&self) -> impl Future<Output = Result<Vec<Run>, QueryError>> + Send;

    fn list_experiments(&self) -> impl Future<Output = Result<Vec<Experiment>, QueryError>> + Send;

    fn list_projects(&self) -> impl Future<Output = Result<Vec<Project>, QueryError>> + Send;

    fn list_metrics(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<Metric>, QueryError>> + Send;

    fn query(
        &self,
        q: &MetricQuery,
    ) -> impl Future<Output = Result<MetricSeries, QueryError>> + Send;

    fn query_batch(
        &self,
        request: &QueryRequest,
    ) -> impl Future<Output = Result<QueryResponse, QueryError>> + Send;
}

/// Map a query message to a result using the given service.
pub async fn dispatch<S: QueryService>(service: &S, msg: QueryMessage) -> QueryResult {
    match msg {
        QueryMessage::ListRuns => match service.list_runs().await {
            Ok(runs) => QueryResult::Runs(runs),
            Err(e) => {
                tracing::error!("query failed: {e}");
                QueryResult::Error(ApiError::Internal)
            }
        },
        QueryMessage::ListExperiments => match service.list_experiments().await {
            Ok(experiments) => QueryResult::Experiments(experiments),
            Err(e) => {
                tracing::error!("query failed: {e}");
                QueryResult::Error(ApiError::Internal)
            }
        },
        QueryMessage::ListProjects => match service.list_projects().await {
            Ok(projects) => QueryResult::Projects(projects),
            Err(e) => {
                tracing::error!("query failed: {e}");
                QueryResult::Error(ApiError::Internal)
            }
        },
        QueryMessage::ListMetrics(run_id) => match service.list_metrics(&run_id).await {
            Ok(metrics) => QueryResult::Metrics(metrics),
            Err(e) => {
                tracing::error!("query failed: {e}");
                QueryResult::Error(ApiError::Internal)
            }
        },
        QueryMessage::Query(query) => match service.query(&query).await {
            Ok(series) => QueryResult::Series(series),
            Err(e) => {
                tracing::error!("query failed: {e}");
                QueryResult::Error(ApiError::Internal)
            }
        },
        QueryMessage::QueryBatch(request) => match service.query_batch(&request).await {
            Ok(response) => QueryResult::BatchResponse(response),
            Err(e) => {
                tracing::error!("query failed: {e}");
                QueryResult::Error(ApiError::Internal)
            }
        },
    }
}

#[derive(Clone)]
pub struct Service<S, B, M, C, R, E, P>
where
    S: Selector,
    B: BucketReader,
    M: MetricReader,
    C: CompactionCursor,
    R: ReadRepository<Run>,
    E: ReadRepository<Experiment>,
    P: ReadRepository<Project>,
{
    selector: S,
    bucket_reader: B,
    metric_reader: M,
    compaction_cursor: C,
    run_reader: R,
    experiment_reader: E,
    project_reader: P,
    tier_selector: TierSelector,
}

impl<S, B, M, C, R, E, P> Service<S, B, M, C, R, E, P>
where
    S: Selector,
    B: BucketReader,
    M: MetricReader,
    C: CompactionCursor,
    R: ReadRepository<Run>,
    E: ReadRepository<Experiment>,
    P: ReadRepository<Project>,
{
    pub fn new(
        selector: S,
        bucket_reader: B,
        metric_reader: M,
        compaction_cursor: C,
        run_reader: R,
        experiment_reader: E,
        project_reader: P,
        tier_selector: TierSelector,
    ) -> Self {
        Self {
            selector,
            bucket_reader,
            metric_reader,
            compaction_cursor,
            run_reader,
            experiment_reader,
            project_reader,
            tier_selector,
        }
    }
}

impl<S, B, M, C, R, E, P> QueryService for Service<S, B, M, C, R, E, P>
where
    S: Selector,
    B: BucketReader,
    M: MetricReader,
    C: CompactionCursor,
    R: ReadRepository<Run>,
    E: ReadRepository<Experiment>,
    P: ReadRepository<Project>,
{
    async fn list_runs(&self) -> Result<Vec<Run>, QueryError> {
        Ok(self.run_reader.list().await?)
    }

    async fn list_experiments(&self) -> Result<Vec<Experiment>, QueryError> {
        Ok(self.experiment_reader.list().await?)
    }

    async fn list_projects(&self) -> Result<Vec<Project>, QueryError> {
        Ok(self.project_reader.list().await?)
    }

    async fn list_metrics(&self, run_id: &RunId) -> Result<Vec<Metric>, QueryError> {
        Ok(self.metric_reader.list_metrics(run_id).await?)
    }

    async fn query(&self, q: &MetricQuery) -> Result<MetricSeries, QueryError> {
        let point_count = self
            .metric_reader
            .count_points(&q.run_id, &q.key, q.step_range.clone())
            .await?;

        let plan = self.tier_selector.pick(point_count, q.target_points);

        let data = match plan.line {
            Resolution::Raw => {
                let points = self
                    .metric_reader
                    .read_points(&q.run_id, &q.key, q.step_range.clone())
                    .await?;

                let selected = if points.len() <= q.target_points {
                    points
                } else {
                    self.selector.select(&points, q.target_points)
                };

                SeriesData::Raw {
                    points: selected
                        .into_iter()
                        .map(|(step, value)| DataPoint { step, value })
                        .collect(),
                }
            }
            Resolution::Bucketed(tier_index) => {
                let compacted_through = self
                    .compaction_cursor
                    .get(&q.run_id, &q.key, tier_index)
                    .await?
                    .unwrap_or(Step::ZERO);

                let bucket_end = compacted_through.min(q.step_range.end);
                let raw_start = compacted_through.max(q.step_range.start);

                // Bucketed history
                let buckets = if q.step_range.start < bucket_end {
                    self.bucket_reader
                        .read_buckets(
                            &q.run_id,
                            &q.key,
                            tier_index,
                            q.step_range.start..bucket_end,
                        )
                        .await?
                } else {
                    Vec::new()
                };

                // Raw tail
                let raw_tail = if raw_start < q.step_range.end {
                    self.metric_reader
                        .read_points(&q.run_id, &q.key, raw_start..q.step_range.end)
                        .await?
                } else {
                    Vec::new()
                };

                // Envelope
                let envelope: Vec<RangePoint> = match plan.envelope {
                    Resolution::Bucketed(env_tier) if env_tier != tier_index => {
                        let env_buckets = self
                            .bucket_reader
                            .read_buckets(
                                &q.run_id,
                                &q.key,
                                env_tier,
                                q.step_range.start..bucket_end,
                            )
                            .await?;
                        env_buckets
                            .iter()
                            .map(|b| RangePoint {
                                step_start: b.step_start,
                                step_end: b.step_end,
                                min: b.min,
                                max: b.max,
                            })
                            .collect()
                    }
                    _ => buckets
                        .iter()
                        .map(|b| RangePoint {
                            step_start: b.step_start,
                            step_end: b.step_end,
                            min: b.min,
                            max: b.max,
                        })
                        .collect(),
                };

                // Combine bucket values with raw tail
                let combined: Vec<(Step, f64)> = buckets
                    .iter()
                    .map(|b| (b.step_start, b.value))
                    .chain(raw_tail.into_iter())
                    .collect();

                let selected = if combined.len() <= q.target_points {
                    combined
                } else {
                    self.selector.select(&combined, q.target_points)
                };

                SeriesData::Aggregated {
                    points: selected
                        .into_iter()
                        .map(|(step, value)| DataPoint { step, value })
                        .collect(),
                    envelope,
                }
            }
        };

        Ok(MetricSeries {
            run_id: q.run_id,
            key: q.key.clone(),
            data,
        })
    }

    async fn query_batch(&self, request: &QueryRequest) -> Result<QueryResponse, QueryError> {
        let futures: Vec<_> = request.queries.iter().map(|q| self.query(q)).collect();
        let series = futures_util::future::try_join_all(futures).await?;
        Ok(QueryResponse { series })
    }
}
