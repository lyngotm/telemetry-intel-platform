-- Telemetry Intelligence Platform — Initial Schema
-- This file runs automatically on first PostgreSQL container startup.

-- ============================================================
-- DEVICES TABLE
-- Registry of all known telemetry-producing devices.
-- ============================================================
CREATE TABLE devices (
    device_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_name     VARCHAR(255) NOT NULL,
    device_type     VARCHAR(100) NOT NULL,
    location        VARCHAR(255),
    firmware_version VARCHAR(50),
    metadata        JSONB DEFAULT '{}',
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for filtering devices by type
CREATE INDEX idx_devices_type ON devices(device_type);

-- ============================================================
-- TELEMETRY EVENTS TABLE (Partitioned by month)
-- Stores all validated telemetry readings.
-- Parent table defines the schema; child partitions hold the data.
-- ============================================================
CREATE TABLE telemetry_events (
    event_id        UUID NOT NULL DEFAULT gen_random_uuid(),
    device_id       UUID NOT NULL REFERENCES devices(device_id),
    metric_type     VARCHAR(100) NOT NULL,
    value           DOUBLE PRECISION NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    metadata        JSONB DEFAULT '{}',
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, timestamp)  -- timestamp must be in PK for partitioning
) PARTITION BY RANGE (timestamp);

-- Create partitions for current and next month
-- (In production, automate partition creation via pg_partman or a cron job)
CREATE TABLE telemetry_events_2026_05 PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE telemetry_events_2026_06 PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

CREATE TABLE telemetry_events_2026_07 PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');

-- Indexes on partitioned table (automatically created on each partition)
CREATE INDEX idx_telemetry_device_time ON telemetry_events(device_id, timestamp DESC);
CREATE INDEX idx_telemetry_metric_type ON telemetry_events(metric_type, timestamp DESC);

-- ============================================================
-- DEAD LETTER EVENTS TABLE
-- Tracks messages that failed validation in the Ingestion Consumer.
-- ============================================================
CREATE TABLE dead_letter_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_topic  VARCHAR(100) NOT NULL,
    original_payload JSONB NOT NULL,
    error_reason    TEXT NOT NULL,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for recent failures (most common query pattern for debugging)
CREATE INDEX idx_dlq_failed_at ON dead_letter_events(failed_at DESC);

-- ============================================================
-- Migration 002: Create anomalies table for the Anomaly Detection Service
-- Stores detected anomalies with statistical context for downstream diagnosis.
-- ============================================================

CREATE TABLE IF NOT EXISTS anomalies (
    anomaly_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id UUID NOT NULL REFERENCES devices(device_id),
    metric_type VARCHAR(100) NOT NULL,
    observed_value DOUBLE PRECISION NOT NULL,
    expected_range JSONB NOT NULL,
    z_score DOUBLE PRECISION NOT NULL,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acknowledged', 'resolved')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_device_id ON anomalies(device_id);
CREATE INDEX IF NOT EXISTS idx_anomalies_detected_at ON anomalies(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_device_time ON anomalies(device_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies(severity);

