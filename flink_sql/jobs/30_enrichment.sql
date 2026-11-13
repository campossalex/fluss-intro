-- =====================================================================
-- JOB: scan-enrichment  (Lab 3, Step 11)
--
--   fluss.app.scan_events_log  ⋈  fluss.app.route_reference  ->  fluss.app.scans_enriched
--
-- FOR SYSTEM_TIME AS OF t.proc_time makes this a LOOKUP join: each scan looks up its
-- carrier in the route_reference PK table at the moment it is processed. Update a row
-- in route_reference (Lab 5, Step 20) and new scans pick it up with no restart.
--
-- PROCTIME() is added inline via a subquery; Flink uses it only as the processing-time
-- clock for the lookup, and it is not written to the sink.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh enrich
-- =====================================================================

SET 'pipeline.name' = 'scan-enrichment';

INSERT INTO fluss.app.scans_enriched
SELECT
  t.`scan_id`,
  t.`shipment_id`,
  t.`carrier_code`,
  r.`carrier_name`,
  t.`status`,
  t.`location`,
  r.`hub_city`,
  r.`avg_delivery_days`,
  t.`scan_time`
FROM (
  SELECT *, PROCTIME() AS `proc_time`
  FROM fluss.app.scan_events_log
) AS t
JOIN fluss.app.route_reference FOR SYSTEM_TIME AS OF t.`proc_time` AS r
  ON t.`carrier_code` = r.`carrier_code`;
