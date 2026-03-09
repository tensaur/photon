use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_core::types::query::{
    DataPoint, MetricQuery, MetricSeries, QueryRequest, QueryResponse, RangePoint, SeriesData,
};
use photon_downsample::ports::selector::Selector;
use photon_store::ports::bucket::BucketReader;
use photon_store::ports::metric::MetricReader;

use crate::domain::tier::{Resolution, TierSelector};

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("store read failed")]
    Read(#[from] photon_store::ports::ReadError),
}

pub trait QueryService {
    fn query(
        &self,
        q: &MetricQuery,
    ) -> impl Future<Output = Result<MetricSeries, QueryError>> + Send;

    fn query_batch(
        &self,
        request: &QueryRequest,
    ) -> impl Future<Output = Result<QueryResponse, QueryError>> + Send;

    fn list_metrics(
        &self,
        run_id: &RunId,
    ) -> impl Future<Output = Result<Vec<Metric>, QueryError>> + Send;
}

pub struct Service<S, B, M>
where
    S: Selector,
    B: BucketReader,
    M: MetricReader,
{
    selector: S,
    bucket_reader: B,
    metric_reader: M,
    tier_selector: TierSelector,
}

impl<S, B, M> Service<S, B, M>
where
    S: Selector,
    B: BucketReader,
    M: MetricReader,
{
    pub fn new(selector: S, bucket_reader: B, metric_reader: M, tier_selector: TierSelector) -> Self {
        Self {
            selector,
            bucket_reader,
            metric_reader,
            tier_selector,
        }
    }
}

impl<S, B, M> QueryService for Service<S, B, M>
where
    S: Selector,
    B: BucketReader,
    M: MetricReader,
{
    async fn query(&self, q: &MetricQuery) -> Result<MetricSeries, QueryError> {
        let plan = self.tier_selector.pick(&q.step_range, q.target_points);

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
            Resolution::Bucketed(line_width) => {
                let line_buckets = self
                    .bucket_reader
                    .read_buckets(&q.run_id, &q.key, line_width, q.step_range.clone())
                    .await?;

                let candidates: Vec<(u64, f64)> =
                    line_buckets.iter().map(|b| (b.step, b.value)).collect();

                let selected = if candidates.len() <= q.target_points {
                    candidates
                } else {
                    self.selector.select(&candidates, q.target_points)
                };

                let envelope = match plan.envelope {
                    Resolution::Bucketed(env_width) if env_width != line_width => {
                        let env_buckets = self
                            .bucket_reader
                            .read_buckets(&q.run_id, &q.key, env_width, q.step_range.clone())
                            .await?;
                        env_buckets
                            .iter()
                            .map(|b| RangePoint {
                                step: b.step,
                                min: b.min,
                                max: b.max,
                            })
                            .collect()
                    }
                    _ => line_buckets
                        .iter()
                        .map(|b| RangePoint {
                            step: b.step,
                            min: b.min,
                            max: b.max,
                        })
                        .collect(),
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
        let mut series = Vec::with_capacity(request.queries.len());

        for q in &request.queries {
            series.push(self.query(q).await?);
        }

        Ok(QueryResponse { series })
    }

    async fn list_metrics(&self, run_id: &RunId) -> Result<Vec<Metric>, QueryError> {
        Ok(self.metric_reader.list_metrics(run_id).await?)
    }
}

