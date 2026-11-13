-- =====================================================================
-- JOB: delivery-summary  (Lab 4, Step 15 — Job B)
--
--   fluss.app.scans_enriched  ->  TUMBLE 5 min  ->  shipments_pg.shipments.delivery_summary
--
-- Shipments per status and carrier in each window. COUNT(DISTINCT shipment_id) avoids
-- double-counting a shipment scanned more than once in the same window.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh delivery-summary
-- =====================================================================

SET 'pipeline.name' = 'delivery-summary';

INSERT INTO shipments_pg.shipments.delivery_summary
SELECT
  window_start,
  window_end,
  `carrier_code`,
  `carrier_name`,
  `status`,
  COUNT(DISTINCT `shipment_id`) AS shipment_count
FROM TABLE(
  TUMBLE(TABLE fluss.app.scans_enriched, DESCRIPTOR(`scan_time`), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, `carrier_code`, `carrier_name`, `status`;
