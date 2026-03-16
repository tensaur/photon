use serde::{Deserialize, Serialize};

use crate::types::sequence::SequenceNumber;

/// Domain representation of a server acknowledgment.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckResult {
    pub sequence_number: SequenceNumber,
    pub status: AckStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AckStatus {
    Ok,
    Duplicate,
    Rejected,
}
