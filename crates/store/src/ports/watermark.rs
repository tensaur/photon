use std::future::Future;

use photon_core::types::id::RunId;
use photon_core::types::sequence::SequenceNumber;

use super::{ReadError, WriteError};

/// Persist per-run dedup watermarks. Written by the persist consumer.
pub trait WatermarkWriter: Send + Sync + Clone + 'static {
    fn write_watermarks(
        &self,
        entries: &[(RunId, SequenceNumber)],
    ) -> impl Future<Output = Result<(), WriteError>> + Send;
}

/// Read persisted watermarks. Used once at startup to seed the dedup cache.
pub trait WatermarkReader: Send + Sync + Clone + 'static {
    fn read_all(
        &self,
    ) -> impl Future<Output = Result<Vec<(RunId, SequenceNumber)>, ReadError>> + Send;
}
