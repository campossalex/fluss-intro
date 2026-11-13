-- =====================================================================
-- JOB: shipment-status  (Lab 2, Step 5)
--
--   fluss.app.scan_events_log  ->  fluss.app.shipment_status (PK table)
--
-- Reads every scan and upserts it into the PK table. Because shipment_id is the primary
-- key, only each shipment's latest scan survives: the Log table keeps the history, this
-- job materialises the current state.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh status
-- =====================================================================

SET 'pipeline.name' = 'shipment-status';

INSERT INTO fluss.app.shipment_status
SELECT
  `shipment_id`,
  `carrier_code`,
  `status`,
  `location`,
  `scan_time` AS `last_scan`
FROM fluss.app.scan_events_log;
