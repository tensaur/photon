use crate::types::sequence::SequenceNumber;

/// Domain representation of a server acknowledgment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AckResult {
    pub sequence_number: SequenceNumber,
    pub status: AckStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AckStatus {
    Ok,
    Duplicate,
    Rejected,
}
