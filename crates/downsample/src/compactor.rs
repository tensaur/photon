use photon_core::types::bucket::BucketEntry;
use photon_core::types::id::RunId;
use photon_core::types::metric::{Metric, Step};
use photon_store::ports::bucket::BucketWriter;
use photon_store::ports::compaction::CompactionCursor;
use photon_store::ports::metric::MetricReader;

use crate::ports::aggregator::Aggregator;
use crate::reducer::Reducer;

/// Offline compactor for rebuild, backfill, and repair.
///
/// Reads raw points from the store and reduces them into buckets.
/// Not the primary hot path — the streaming [`Reducer`] in the persist
/// projection handles live downsampling.
pub struct Compactor<A, M, B, C>
where
    A: Aggregator,
    M: MetricReader,
    B: BucketWriter,
    C: CompactionCursor,
{
    aggregator: A,
    widths: Vec<u64>,
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
        widths: Vec<u64>,
        metric_reader: M,
        bucket_writer: B,
        cursor: C,
    ) -> Self {
        Self {
            aggregator,
            widths,
            metric_reader,
            bucket_writer,
            cursor,
        }
    }

    /// Compact a single metric key for a single run.
    /// Reads raw points past the cursor, reduces and writes buckets.
    pub async fn compact(
        &self,
        run_id: &RunId,
        key: &Metric,
    ) -> Result<CompactionResult, CompactionError> {
        let cursor_step = self
            .cursor
            .get(run_id, key, 0)
            .await
            .map_err(CompactionError::Read)?
            .unwrap_or(Step::ZERO);

        let points = self
            .metric_reader
            .read_points(run_id, key, cursor_step..Step::MAX)
            .await
            .map_err(CompactionError::Read)?;

        if points.is_empty() {
            return Ok(CompactionResult { buckets_written: 0 });
        }

        let mut reducer = Reducer::new(self.aggregator.clone(), self.widths.clone());
        let mut entries = Vec::new();

        for &(step, value) in &points {
            for (tier, bucket) in reducer.push(step, value) {
                entries.push(BucketEntry {
                    run_id: *run_id,
                    key: key.clone(),
                    tier,
                    bucket,
                });
            }
        }

        // Flush partial buckets
        for (tier, bucket) in reducer.flush() {
            entries.push(BucketEntry {
                run_id: *run_id,
                key: key.clone(),
                tier,
                bucket,
            });
        }

        if entries.is_empty() {
            return Ok(CompactionResult { buckets_written: 0 });
        }

        self.bucket_writer
            .write_buckets(&entries)
            .await
            .map_err(CompactionError::Write)?;

        let last_step = entries.last().unwrap().bucket.step_end;
        self.cursor
            .advance(run_id, key, 0, last_step + 1)
            .await
            .map_err(CompactionError::Write)?;

        Ok(CompactionResult {
            buckets_written: entries.len(),
        })
    }

    /// Final compaction when a run finishes. Processes all metrics.
    pub async fn compact_run(&self, run_id: &RunId) -> Result<CompactionResult, CompactionError> {
        let keys = self
            .metric_reader
            .list_metrics(run_id)
            .await
            .map_err(CompactionError::Read)?;

        let mut total_buckets = 0;
        for key in &keys {
            let result = self.compact(run_id, key).await?;
            total_buckets += result.buckets_written;
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
