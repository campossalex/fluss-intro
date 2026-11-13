-- =====================================================================
-- The PostgreSQL catalog  (Lab 4 Step 12)
--
-- A JDBC catalog exposes the tables of the `shipments` database (postgres container,
-- created by postgres/pg_shipments_ddl.sql) to Flink as shipments_pg.shipments.<table>,
-- with their columns and primary keys read from PostgreSQL itself. The primary keys are
-- what make the three Lab 4 sinks upsert each window's row.
--
-- Like the fluss catalog, the registration is kept by the file-based CatalogStore, so it
-- survives New session and gateway restarts.
--
-- Needs flink-connector-jdbc-core + flink-connector-jdbc-postgres, not the all-in-one
-- flink-connector-jdbc 3.3.0 JAR — see scripts/download-jars.sh.
-- =====================================================================

CREATE CATALOG IF NOT EXISTS shipments_pg WITH (
  'type'             = 'jdbc',
  'base-url'         = 'jdbc:postgresql://postgres:5432',
  'default-database' = 'shipments',
  'username'         = 'root',
  'password'         = 'admin1'
);
