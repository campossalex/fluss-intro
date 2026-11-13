-- PostgreSQL analytics tables for the Fluss Intro lab (database `shipments`).
-- Run once by the postgres image's initdb (docker-entrypoint-initdb.d), as user root,
-- which POSTGRES_USER / POSTGRES_DB in docker-compose.yml already created with the database.
-- Lab 4 writes the three tables from Flink, through the shipments_pg JDBC catalog.

-- ── Job A sink ────────────────────────────────────────────────────────────────
-- 5-minute tumbling window aggregation of scan events by carrier + location.
-- Answers: which carriers and cities are busiest in each window?
CREATE TABLE IF NOT EXISTS scan_window_5m (
    window_start  TIMESTAMP    NOT NULL,
    window_end    TIMESTAMP    NOT NULL,
    carrier_code  VARCHAR(10)  NOT NULL,
    carrier_name  VARCHAR(50)  NOT NULL,
    location      VARCHAR(50)  NOT NULL,
    scan_count    BIGINT,
    PRIMARY KEY (window_start, window_end, carrier_code, location)
);

-- ── Job B sink ────────────────────────────────────────────────────────────────
-- 5-minute tumbling window breakdown of shipments by status + carrier.
-- Answers: how many shipments advanced to each status in each window?
CREATE TABLE IF NOT EXISTS delivery_summary (
    window_start   TIMESTAMP   NOT NULL,
    window_end     TIMESTAMP   NOT NULL,
    carrier_code   VARCHAR(10) NOT NULL,
    carrier_name   VARCHAR(50) NOT NULL,
    status         VARCHAR(30) NOT NULL,
    shipment_count BIGINT,
    PRIMARY KEY (window_start, window_end, carrier_code, status)
);

-- ── Job C sink ────────────────────────────────────────────────────────────────
-- 5-minute tumbling window of completed deliveries per carrier.
-- Includes SLA (avg_delivery_days) for dashboard comparison.
CREATE TABLE IF NOT EXISTS transit_time_5m (
    window_start    TIMESTAMP   NOT NULL,
    window_end      TIMESTAMP   NOT NULL,
    carrier_code    VARCHAR(10) NOT NULL,
    carrier_name    VARCHAR(50) NOT NULL,
    sla_days        INT,
    delivered_count BIGINT,
    PRIMARY KEY (window_start, window_end, carrier_code)
);
