-- =====================================================================
-- JOB: transit-time-5m  (Lab 4, Step 16 — Job C)
--
--   fluss.app.scans_enriched (DELIVERED only)  ->  TUMBLE 5 min  ->  shipments_pg.shipments.transit_time_5m
--
-- Completed deliveries per carrier and window, with the SLA (avg_delivery_days) that the
-- Lab 3 lookup join attached, so Grafana can compare throughput against each promise.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh transit-time
-- =====================================================================

SET 'pipeline.name' = 'transit-time-5m';

INSERT INTO shipments_pg.shipments.transit_time_5m
SELECT
  window_start,
  window_end,
  `carrier_code`,
  `carrier_name`,
  `avg_delivery_days` AS sla_days,
  COUNT(*) AS delivered_count
FROM TABLE(
  TUMBLE(TABLE fluss.app.scans_enriched, DESCRIPTOR(`scan_time`), INTERVAL '5' MINUTE)
)
WHERE `status` = 'DELIVERED'
GROUP BY window_start, window_end, `carrier_code`, `carrier_name`, `avg_delivery_days`;
