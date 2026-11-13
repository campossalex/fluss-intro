-- =====================================================================
-- The postgres-cdc source table  (Lab 3, Step 8)
--
-- Unlike everything in 01_fluss.sql and 02_postgres.sql, this table lives in Flink's
-- built-in, in-memory catalog (`source_catalog`): it exists only in the session that
-- created it. A new session needs it again — submit.sh loads it into every job's session
-- with -i, and the SQL editor re-creates it in a new session while routes-cdc runs.
--
-- Needs flink-sql-connector-postgres-cdc in lib/ — see scripts/download-jars.sh.
-- =====================================================================


-- ------------------------------------ Step 8: the postgres-cdc source table
-- Snapshots PostgreSQL's route_reference on startup, then streams every INSERT, UPDATE
-- and DELETE. It reads database `shipments` as cdc_user, set up for this by
-- postgres/pg_shipments_ddl.sql: wal_level=logical, a replication user, and the slot
-- and publication named below.
CREATE TABLE IF NOT EXISTS route_reference_cdc (
  `carrier_code`      STRING,
  `carrier_name`      STRING,
  `hub_city`          STRING,
  `avg_delivery_days` INT,
  PRIMARY KEY (`carrier_code`) NOT ENFORCED
) WITH (
  'connector'                 = 'postgres-cdc',
  'hostname'                  = 'postgres',
  'port'                      = '5432',
  'username'                  = 'cdc_user',
  'password'                  = 'admin1',
  'database-name'             = 'shipments',
  'schema-name'               = 'public',
  'table-name'                = 'route_reference',
  'slot.name'                 = 'flink_cdc_routes_slot',
  'decoding.plugin.name'      = 'pgoutput',
  'debezium.publication.name' = 'routes_pub'
);
