-- =====================================================================
-- JOB: scan-window-5m  (Lab 4, Step 14 — Job A)
--
--   fluss.app.scans_enriched  ->  TUMBLE 5 min  ->  shipments_pg.shipments.scan_window_5m
--
-- Scan volume per window, carrier and location: which carriers and cities are busiest.
-- Each window is written once it closes (5 minutes plus the 5-second watermark delay).
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh scan-window
-- =====================================================================

SET 'pipeline.name' = 'scan-window-5m';

INSERT INTO shipments_pg.shipments.scan_window_5m
SELECT
  window_start,
  window_end,
  `carrier_code`,
  `carrier_name`,
  `location`,
  COUNT(*) AS scan_count
FROM TABLE(
  TUMBLE(TABLE fluss.app.scans_enriched, DESCRIPTOR(`scan_time`), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, `carrier_code`, `carrier_name`, `location`;
