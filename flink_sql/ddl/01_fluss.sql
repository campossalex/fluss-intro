-- =====================================================================
-- The Fluss catalog and every Fluss table  (Lab 1 Steps 1-2, Lab 2 Step 4,
-- Lab 3 Steps 8 and 10)
--
-- Open-source Fluss infers the table type from the DDL: a PRIMARY KEY makes a PK table,
-- no key a Log table. (Ververica's Fluss also accepts a 'table.type' option; OSS Fluss
-- 1.0 rejects it as an unknown property.)
--
-- Nothing here is lost when a session ends:
--
--   - The tables live in Fluss itself. Once created, fluss.app.* is there for every
--     session, every job and every restart of the SQL Gateway.
--   - The `fluss` catalog REGISTRATION survives too: docker-compose.yml enables a
--     file-based CatalogStore, shared by sql-client and sql-gateway.
--
-- So this file only really needs to run once. It is still loaded into every job's
-- session by submit.sh (with -i), and every statement is IF NOT EXISTS so that replaying
-- it is a no-op.
-- =====================================================================


-- ------------------------------------------------ Step 1: the Fluss catalog
-- Registers Fluss in Flink, so Fluss tables can be created and queried from SQL.
CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);


-- ------------------------------------ Step 2: database + scan_events_log (Log table)
-- A Log table is append-only: every event is kept and can be replayed from any point,
-- with no retention window. The WATERMARK lets it drive event-time windows (Lab 4).
-- The scan-gen container starts writing as soon as this table exists.
CREATE DATABASE IF NOT EXISTS fluss.app;

CREATE TABLE IF NOT EXISTS fluss.app.scan_events_log (
  `scan_id`      STRING,
  `shipment_id`  STRING,
  `carrier_code` STRING,
  `status`       STRING,
  `location`     STRING,
  `scan_time`    TIMESTAMP(3),
  WATERMARK FOR `scan_time` AS `scan_time` - INTERVAL '5' SECOND
);


-- --------------------------------------- Step 4: shipment_status (PK table)
-- One row per shipment_id: every write with the same key is an upsert.
CREATE TABLE IF NOT EXISTS fluss.app.shipment_status (
  `shipment_id`  STRING,
  `carrier_code` STRING,
  `status`       STRING,
  `location`     STRING,
  `last_scan`    TIMESTAMP(3),
  PRIMARY KEY (`shipment_id`) NOT ENFORCED
) WITH (
  'bucket.num' = '3'
);


-- --------------------------------------- Step 8: route_reference (PK table)
-- Carrier metadata, one row per carrier: the lookup side of Lab 3's temporal join.
-- seed/route_reference.sql loads its five rows.
CREATE TABLE IF NOT EXISTS fluss.app.route_reference (
  `carrier_code`      STRING,
  `carrier_name`      STRING,
  `hub_city`          STRING,
  `avg_delivery_days` INT,
  PRIMARY KEY (`carrier_code`) NOT ENFORCED
) WITH (
  'bucket.num' = '1'
);


-- --------------------------------------- Step 10: scans_enriched (Log table)
-- Every scan, forever, with the carrier metadata: the source of all Lab 4 analytics.
CREATE TABLE IF NOT EXISTS fluss.app.scans_enriched (
  `scan_id`           STRING,
  `shipment_id`       STRING,
  `carrier_code`      STRING,
  `carrier_name`      STRING,
  `status`            STRING,
  `location`          STRING,
  `hub_city`          STRING,
  `avg_delivery_days` INT,
  `scan_time`         TIMESTAMP(3),
  WATERMARK FOR `scan_time` AS `scan_time` - INTERVAL '5' SECOND
);
