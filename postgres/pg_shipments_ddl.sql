-- PostgreSQL for the Fluss Intro lab (database `shipments`).
-- Run once by the postgres image's initdb (docker-entrypoint-initdb.d), as user root,
-- which POSTGRES_USER / POSTGRES_DB in docker-compose.yml already created with the database.
-- A second `docker compose up` on the same volume skips it; `docker compose down -v`
-- re-runs it.
--
--   route_reference   the carriers: the source of truth that postgres-cdc replicates into
--                     fluss.app.route_reference (Lab 3), edited through the shipments_pg
--                     JDBC catalog (Lab 5), read by scan-gen for its carrier list
--   the three *_5m / summary tables   written by the Lab 4 jobs, read by Grafana
--
-- The CDC prerequisites — a replication user, a slot and a publication — are set up
-- here, except wal_level: it is a postmaster setting, so it is on the server command
-- line in docker-compose.yml.

-- ── Carriers (Lab 3 — the postgres-cdc source) ──────────────────────────────
CREATE TABLE IF NOT EXISTS route_reference (
    carrier_code       VARCHAR(10) PRIMARY KEY,
    carrier_name       VARCHAR(50) NOT NULL,
    hub_city           VARCHAR(50),
    avg_delivery_days  INT
);

INSERT INTO route_reference VALUES
  ('DHL', 'DHL Express',         'Frankfurt', 2),
  ('UPS', 'UPS Worldwide',       'Cologne',   3),
  ('FDX', 'FedEx International', 'Amsterdam', 2),
  ('GLS', 'GLS Group',           'Bologna',   4),
  ('TNT', 'TNT Express',         'Liège',     3);

-- Full before-images, so an UPDATE reaches Flink as a proper -U/+U pair.
ALTER TABLE route_reference REPLICA IDENTITY FULL;

-- The user the postgres-cdc source in flink_sql/ddl/03_sources.sql connects as:
-- REPLICATION for the stream, SELECT for the snapshot.
CREATE USER cdc_user WITH PASSWORD 'admin1' REPLICATION;
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON route_reference TO cdc_user;

-- The logical replication slot and publication the connector reads through (pgoutput).
-- Pre-created so the CDC table can name them.
SELECT pg_create_logical_replication_slot('flink_cdc_routes_slot', 'pgoutput');
CREATE PUBLICATION routes_pub FOR TABLE route_reference;

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
