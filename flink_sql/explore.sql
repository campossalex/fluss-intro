-- =====================================================================
-- Interactive queries  (the SELECT and upsert steps of every lab)
--
-- NOT submitted by start.sh, and deliberately outside ddl/ and jobs/ so submit.sh never
-- picks it up. Each `-- ---- title` section below is one entry in the web SQL editor's
-- examples menu (http://localhost:8084). From a terminal instead:
--
--   docker compose exec sql-client /opt/flink/bin/sql-client.sh
--
-- The fluss and shipments_pg catalog registrations are kept by the CatalogStore, and the
-- tables by Fluss and PostgreSQL, so nothing has to be loaded first. Press `q` to leave a
-- result grid.
-- =====================================================================


-- ------------------------------------- Lab 1, Step 3: scans landing in Fluss
-- One row per scan, status moving PICKED_UP -> IN_TRANSIT -> OUT_FOR_DELIVERY ->
-- DELIVERED. No ingestion job: scan-gen writes into the table directly. Cancel the query
-- once you have seen enough.
SELECT * FROM fluss.app.scan_events_log;


-- ------------------------------------- Lab 2, Step 6: pick a shipment_id
-- Copy one of these for the point lookup below.
SET 'execution.runtime-mode' = 'batch';
SELECT DISTINCT `shipment_id` FROM fluss.app.scan_events_log LIMIT 5;
SET 'execution.runtime-mode' = 'streaming';


-- ------------------------------------- Lab 2, Step 6: point lookup
-- Replace SHP-XXXXX with a value from the query above. A PK table serves it as a
-- point read: always exactly one row, the shipment's latest scan. Run it again after a
-- few seconds and the status may have moved on.
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM fluss.app.shipment_status
WHERE `shipment_id` = 'SHP-XXXXX';
SET 'execution.runtime-mode' = 'streaming';


-- ------------------------------------- Lab 2, Step 7: Log vs PK row counts
-- The Log table grows with every scan; the PK table holds one row per shipment.
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) AS total_scans      FROM fluss.app.scan_events_log;
SELECT COUNT(*) AS active_shipments FROM fluss.app.shipment_status;
SET 'execution.runtime-mode' = 'streaming';


-- ------------------------------------- Lab 3, Step 9: the reference table
-- Five rows, one per carrier.
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM fluss.app.route_reference;
SET 'execution.runtime-mode' = 'streaming';


-- ------------------------------------- Lab 3, Step 11: enriched scans
-- Rows now carry carrier_name, hub_city and avg_delivery_days from route_reference.
SELECT * FROM fluss.app.scans_enriched;


-- ------------------------------------- Lab 4, Step 13: the PostgreSQL tables
-- Empty until the Lab 4 jobs have closed their first 5-minute window. A bounded read:
-- a JDBC table is scanned once, not followed.
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) AS scan_window_rows FROM shipments_pg.shipments.scan_window_5m;
SET 'execution.runtime-mode' = 'streaming';


-- ------------------------------------- Lab 4, Step 16: scans per carrier
-- A bounded read of what Job A has written so far. Empty for the first ~5 minutes.
SET 'execution.runtime-mode' = 'batch';
SELECT carrier_name, SUM(scan_count) AS total_scans
FROM shipments_pg.shipments.scan_window_5m
GROUP BY carrier_name
ORDER BY total_scans DESC;
SET 'execution.runtime-mode' = 'streaming';


-- ------------------------------------- Lab 5, Step 20: update a carrier SLA
-- An upsert: carrier_code DHL already exists, so Fluss overwrites the row in place.
-- The running scan-enrichment job picks it up for every scan after this point, and the
-- next closed window in transit_time_5m shows sla_days = 1 for DHL — no restart.
INSERT INTO fluss.app.route_reference VALUES
  ('DHL', 'DHL Express', 'Frankfurt', 1);
