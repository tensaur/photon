CREATE DATABASE IF NOT EXISTS photon;

CREATE TABLE IF NOT EXISTS photon.metrics (
    run_id UUID,
    key String,
    step UInt64,
    value Float64,
    timestamp_ms UInt64
) ENGINE = MergeTree()
ORDER BY (run_id, key, step);

CREATE TABLE IF NOT EXISTS photon.watermarks (
    run_id UUID,
    sequence UInt64
) ENGINE = ReplacingMergeTree(sequence)
ORDER BY (run_id);

CREATE TABLE IF NOT EXISTS photon.buckets (
    run_id UUID,
    key String,
    tier UInt32,
    step_start UInt64,
    step_end UInt64,
    value Float64,
    min Float64,
    max Float64
) ENGINE = MergeTree()
ORDER BY (run_id, key, tier, step_start);

CREATE TABLE IF NOT EXISTS photon.compaction_cursors (
    run_id UUID,
    key String,
    tier UInt32,
    offset UInt64
) ENGINE = ReplacingMergeTree(offset)
ORDER BY (run_id, key, tier);
