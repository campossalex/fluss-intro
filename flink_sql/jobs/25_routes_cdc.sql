-- =====================================================================
-- JOB: routes-cdc  (Lab 3, Step 8)
--
--   PostgreSQL route_reference (CDC)  ->  fluss.app.route_reference (PK table)
--
-- Snapshots the carriers, then keeps the Fluss table in sync with every later INSERT,
-- UPDATE and DELETE in PostgreSQL. route_reference is a PK table, so the changelog lands
-- as upserts and deletes — and the running scan-enrichment lookup join sees each change
-- for the next scan it processes.
--
-- Needs route_reference_cdc in the session: ddl/03_sources.sql.
--
-- Submit on its own with:  docker compose exec sql-client /opt/submit.sh routes
-- =====================================================================

SET 'pipeline.name' = 'routes-cdc';

INSERT INTO fluss.app.route_reference
SELECT * FROM route_reference_cdc;
