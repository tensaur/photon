use clickhouse::Row;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct MetricRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub run_id: Uuid,
    pub key: String,
    pub step: u64,
    pub value: f64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct BucketRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub run_id: Uuid,
    pub key: String,
    pub tier: u32,
    pub step_start: u64,
    pub step_end: u64,
    pub value: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct WatermarkRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub run_id: Uuid,
    pub sequence: u64,
}

#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct CompactionCursorRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub run_id: Uuid,
    pub key: String,
    pub tier: u32,
    pub offset: u64,
}
