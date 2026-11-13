#!/usr/bin/env bash
# scripts/download-jars.sh — fetch the connector JARs into ./lib/ for Flink.
#
# lib/*.jar is mounted read-only at /opt/flink/lib/extra/ in every Flink container
# (jobmanager, taskmanager, sql-client, sql-gateway) and copied into /opt/flink/lib/ by
# their entrypoints. The JARs are committed, so this only matters after a version bump.
#
# Safe to re-run — present JARs are skipped.

set -euo pipefail

MAVEN="https://repo1.maven.org/maven2"
FLINK_VERSION="1.20"
FLUSS_VERSION="1.0.0"
JDBC_CONNECTOR_VERSION="3.3.0-1.20"   # core + postgres modules, see below
POSTGRES_DRIVER_VERSION="42.7.4"

LIB_DIR="$(dirname "$0")/../lib"
mkdir -p "$LIB_DIR"

download() {
  local filename="$1" url="$2"
  if [ -f "$LIB_DIR/$filename" ]; then
    echo "  [skip] $filename already present"
    return
  fi
  echo "  [download] $filename"
  curl -fsSL -o "$LIB_DIR/$filename.part" "$url"
  mv "$LIB_DIR/$filename.part" "$LIB_DIR/$filename"
}

# Fluss JARs of any other version, left by an earlier run. Every lib/*.jar goes onto the
# Flink classpath, so a stale one would load two Fluss versions side by side.
for stale in "$LIB_DIR"/fluss-*.jar; do
  [ -e "$stale" ] || continue
  case "$(basename "$stale")" in
    *"-${FLUSS_VERSION}.jar") ;;
    *) echo "  [remove] $(basename "$stale") (not Fluss ${FLUSS_VERSION})"; rm -f "$stale" ;;
  esac
done

echo "==> Fluss: the Flink connector (catalog, Log and PK tables, lookup joins)..."
download "fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar" \
  "$MAVEN/org/apache/fluss/fluss-flink-${FLINK_VERSION}/${FLUSS_VERSION}/fluss-flink-${FLINK_VERSION}-${FLUSS_VERSION}.jar"

# Lab 4: the shipments_pg JDBC catalog and its three sinks.
#
# The core and postgres modules, NOT the all-in-one flink-connector-jdbc JAR. That one
# registers both the deprecated and the new JdbcCatalogFactory under 'jdbc', and every
# CREATE CATALOG ... 'type' = 'jdbc' fails with
#   Multiple factories for identifier 'jdbc' that implement 'CatalogFactory'
# The core module also carries the 'connector' = 'jdbc' table factory.
echo "==> Flink JDBC connector + PostgreSQL driver..."
download "flink-connector-jdbc-core-${JDBC_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-connector-jdbc-core/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-core-${JDBC_CONNECTOR_VERSION}.jar"

download "flink-connector-jdbc-postgres-${JDBC_CONNECTOR_VERSION}.jar" \
  "$MAVEN/org/apache/flink/flink-connector-jdbc-postgres/${JDBC_CONNECTOR_VERSION}/flink-connector-jdbc-postgres-${JDBC_CONNECTOR_VERSION}.jar"

if [ -f "$LIB_DIR/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar" ]; then
  echo "  [remove] flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar (duplicate 'jdbc' catalog factory, see above)"
  rm -f "$LIB_DIR/flink-connector-jdbc-${JDBC_CONNECTOR_VERSION}.jar"
fi

download "postgresql-${POSTGRES_DRIVER_VERSION}.jar" \
  "$MAVEN/org/postgresql/postgresql/${POSTGRES_DRIVER_VERSION}/postgresql-${POSTGRES_DRIVER_VERSION}.jar"

echo ""
echo "==> JARs in $LIB_DIR:"
ls -lh "$LIB_DIR"
echo ""
echo "Done. Run ./start.sh (or 'docker compose up -d') to start the stack."
