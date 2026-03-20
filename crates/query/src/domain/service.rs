use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::query::{
    DataPoint, MetricQuery, MetricSeries, QueryMessage, QueryRequest, QueryResponse, QueryResult,
    RangePoint, SeriesData,
};
use photon_downsample::ports::selector::Selector;
use photon_store::ports::ReadError;
use photon_store::ports::bucket::BucketReader;
use photon_store::ports::compaction::CompactionCursor;
use photon_store::ports::metric::MetricReader;

use crate::domain::tier::{Resolution, TierSelector};

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("store read failed")]
    Read(#[from] ReadError),
}

pub trait QueryService: Send + Sync + 'static {
    fn list_runs(&self) -> impl Future<Output = Result<Vec<RunId>, QueryError>> + Send;

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
            Err(e) => QueryResult::Error(e.to_string()),
        },
        QueryMessage::ListMetrics(run_id) => match service.list_metrics(&run_id).await {
            Ok(metrics) => QueryResult::Metrics(metrics),
            Err(e) => QueryResult::Error(e.to_string()),
        },
        QueryMessage::Query(query) => match service.query(&query).await {
            Ok(series) => QueryResult::Series(series),
            Err(e) => QueryResult::Error(e.to_string()),
        },
        QueryMessage::QueryBatch(request) => match service.query_batch(&request).await {
            Ok(response) => QueryResult::BatchResponse(response),
            Err(e) => QueryResult::Error(e.to_string()),
        },
    }
}

pub struct Service<S, B, M, C>
where
    S: Selector,
    B: BucketReader,
    M: MetricReader,
    C: CompactionCursor,
{
    selector: S,
    bucket_reader: B,
    metric_reader: M,
    compaction_cursor: C,
    tier_selector: TierSelector,
}

impl<S, B, M, C> Service<S, B, M, C>
where
    S: Selector,
    B: BucketReader,
    M: MetricReader,
    C: CompactionCursor,
{
    pub fn new(
        selector: S,
        bucket_reader: B,
        metric_reader: M,
        compaction_cursor: C,
        tier_selector: TierSelector,
    ) -> Self {
        Self {
            selector,
            bucket_reader,
            metric_reader,
            compaction_cursor,
            tier_selector,
        }
    }
}

impl<S, B, M, C> QueryService for Service<S, B, M, C>
where
    S: Selector,
    B: BucketReader,
    M: MetricReader,
    C: CompactionCursor,
{
    async fn list_runs(&self) -> Result<Vec<RunId>, QueryError> {
        Ok(self.metric_reader.list_runs().await?)
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
                        .map(|(s, v)| DataPoint { step: s, value: v })
                        .collect(),
                }
            }
            Resolution::Bucketed(tier_index) => {
                let compacted_through = self
                    .compaction_cursor
                    .get(&q.run_id, &q.key, tier_index)
                    .await?
                    .unwrap_or(0);

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
                let combined: Vec<(u64, f64)> = buckets
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
                        .map(|(s, v)| DataPoint { step: s, value: v })
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
