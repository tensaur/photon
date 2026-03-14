use photon_core::types::bucket::BucketEntry;
use photon_core::types::id::RunId;
use photon_core::types::metric::Metric;
use photon_store::ports::bucket::BucketWriter;
use photon_store::ports::compaction::CompactionCursor;
use photon_store::ports::metric::MetricReader;

use crate::ports::aggregator::Aggregator;
use crate::reducer::Reducer;

pub struct Compactor<A, M, B, C>
where
    A: Aggregator,
    M: MetricReader,
    B: BucketWriter,
    C: CompactionCursor,
{
    aggregator: A,
    divisors: Vec<usize>,
    metric_reader: M,
    bucket_writer: B,
    cursor: C,
}

impl<A, M, B, C> Compactor<A, M, B, C>
where
    A: Aggregator,
    M: MetricReader,
    B: BucketWriter,
    C: CompactionCursor,
{
    pub fn new(
        aggregator: A,
        divisors: Vec<usize>,
        metric_reader: M,
        bucket_writer: B,
        cursor: C,
    ) -> Self {
        Self {
            aggregator,
            divisors,
            metric_reader,
            bucket_writer,
            cursor,
        }
    }

    /// Compact a single metric key for a single run.
    /// Reads raw points past each tier's cursor, reduces and writes buckets.
    pub async fn compact(
        &self,
        run_id: &RunId,
        key: &Metric,
    ) -> Result<CompactionResult, CompactionError> {
        let mut total_buckets = 0;

        for (tier_index, &divisor) in self.divisors.iter().enumerate() {
            let cursor_step = self
                .cursor
                .get(run_id, key, tier_index)
                .await
                .map_err(CompactionError::Read)?
                .unwrap_or(0);

            // Read raw points past the cursor
            let points = self
                .metric_reader
                .read_points(run_id, key, cursor_step..u64::MAX)
                .await
                .map_err(CompactionError::Read)?;

            // Not enough points to close a bucket at this tier
            if points.len() < divisor {
                continue;
            }

            let complete = (points.len() / divisor) * divisor;
            let to_process = &points[..complete];

            let mut reducer = Reducer::new(self.aggregator.clone(), vec![divisor]);
            let mut entries = Vec::new();

            for &(step, value) in to_process {
                for (_, bucket) in reducer.push(step, value) {
                    entries.push(BucketEntry {
                        key: key.clone(),
                        tier: tier_index,
                        bucket,
                    });
                }
            }

            if entries.is_empty() {
                continue;
            }

            self.bucket_writer
                .write_buckets(run_id, &entries)
                .await
                .map_err(CompactionError::Write)?;

            // Advance cursor
            let last_step = entries.last().unwrap().bucket.step_end;
            self.cursor
                .advance(run_id, key, tier_index, last_step + 1)
                .await
                .map_err(CompactionError::Write)?;

            total_buckets += entries.len();
        }

        Ok(CompactionResult {
            buckets_written: total_buckets,
        })
    }

    /// Final compaction when a run finishes. Flushes partial buckets.
    pub async fn compact_run(&self, run_id: &RunId) -> Result<CompactionResult, CompactionError> {
        let keys = self
            .metric_reader
            .list_metrics(run_id)
            .await
            .map_err(CompactionError::Read)?;

        let mut total_buckets = 0;

        for key in &keys {
            for (tier_index, &divisor) in self.divisors.iter().enumerate() {
                let cursor_step = self
                    .cursor
                    .get(run_id, &key, tier_index)
                    .await
                    .map_err(CompactionError::Read)?
                    .unwrap_or(0);

                let points = self
                    .metric_reader
                    .read_points(run_id, &key, cursor_step..u64::MAX)
                    .await
                    .map_err(CompactionError::Read)?;

                if points.is_empty() {
                    continue;
                }

                let mut reducer = Reducer::new(self.aggregator.clone(), vec![divisor]);
                let mut entries = Vec::new();

                for &(step, value) in &points {
                    for (_, bucket) in reducer.push(step, value) {
                        entries.push(BucketEntry {
                            key: key.clone(),
                            tier: tier_index,
                            bucket,
                        });
                    }
                }

                // Flush partial bucket
                for (_, bucket) in reducer.flush() {
                    entries.push(BucketEntry {
                        key: key.clone(),
                        tier: tier_index,
                        bucket,
                    });
                }

                if entries.is_empty() {
                    continue;
                }

                self.bucket_writer
                    .write_buckets(run_id, &entries)
                    .await
                    .map_err(CompactionError::Write)?;

                let last_step = entries.last().unwrap().bucket.step_end;
                self.cursor
                    .advance(run_id, &key, tier_index, last_step + 1)
                    .await
                    .map_err(CompactionError::Write)?;

                total_buckets += entries.len();
            }
        }

        Ok(CompactionResult {
            buckets_written: total_buckets,
        })
    }
}

#[derive(Clone, Debug)]
pub struct CompactionResult {
    pub buckets_written: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    #[error("read failed during compaction")]
    Read(#[source] photon_store::ports::ReadError),

    #[error("write failed during compaction")]
    Write(#[source] photon_store::ports::WriteError),
}
