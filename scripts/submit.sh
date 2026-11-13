#!/usr/bin/env bash
# Submits the lab's Flink SQL as separate jobs, one per lab step, sharing one set of DDL.
# Runs INSIDE the sql-client container, where it is mounted at /opt/submit.sh:
#
#   docker compose exec sql-client /opt/submit.sh                    all of them, in order
#   docker compose exec sql-client /opt/submit.sh status enrich      some, by name
#
# Names, in lab order:
#   status            jobs/20_shipment_status.sql   Lab 2  scan_events_log -> shipment_status (PK)
#   routes            jobs/25_routes_cdc.sql        Lab 3  Postgres CDC    -> route_reference (PK)
#   enrich            jobs/30_enrichment.sql        Lab 3  lookup join     -> scans_enriched
#   scan-window       jobs/40_scan_window_5m.sql    Lab 4  Job A           -> PostgreSQL scan_window_5m
#   delivery-summary  jobs/41_delivery_summary.sql  Lab 4  Job B           -> PostgreSQL delivery_summary
#   transit-time      jobs/42_transit_time_5m.sql   Lab 4  Job C           -> PostgreSQL transit_time_5m
#
# Every session first loads flink_sql/ddl/*.sql with -i: the fluss and shipments_pg
# catalogs, every Fluss table, and the postgres-cdc source table. The last one lives in
# Flink's in-memory catalog, so it must be created again in every session — which is why
# the DDL rides along with each job. It is IF NOT EXISTS throughout.
set -euo pipefail

readonly SQL_DIR=/opt/sql

JOBS=("$@")
[ ${#JOBS[@]} -eq 0 ] && JOBS=(status routes enrich scan-window delivery-summary transit-time)

WORK="$(mktemp -d /tmp/pipeline.XXXXXX)"
trap 'rm -rf "$WORK"' EXIT

# -i takes ONE file, not a list: concatenate the DDL in sorted order.
INIT="$WORK/init.sql"
for f in "$SQL_DIR"/ddl/*.sql; do
  printf -- '-- ===== %s =====\n' "$(basename "$f")" >> "$INIT"
  cat "$f" >> "$INIT"
  printf '\n' >> "$INIT"
done

job_file() {
  case "$1" in
    status)           echo "$SQL_DIR/jobs/20_shipment_status.sql" ;;
    routes)           echo "$SQL_DIR/jobs/25_routes_cdc.sql" ;;
    enrich)           echo "$SQL_DIR/jobs/30_enrichment.sql" ;;
    scan-window)      echo "$SQL_DIR/jobs/40_scan_window_5m.sql" ;;
    delivery-summary) echo "$SQL_DIR/jobs/41_delivery_summary.sql" ;;
    transit-time)     echo "$SQL_DIR/jobs/42_transit_time_5m.sql" ;;
    *)                return 1 ;;
  esac
}

FAILED=""

submit_one() {
  local name="$1" file out
  if ! file="$(job_file "$name")"; then
    echo "unknown job '$name' (expected: status, routes, enrich, scan-window, delivery-summary, transit-time)" >&2
    FAILED="$FAILED $name"
    return
  fi

  echo
  echo "--- ${name}: $(basename "$file") ---"
  out="$(/opt/flink/bin/sql-client.sh -i "$INIT" -f "$file" 2>&1)" || true
  printf '%s\n' "$out" | grep -vE '^(WARNING|[A-Z][a-z]{2} [0-9]{2}, [0-9]{4})' || true

  # Judge by evidence, not by exit status: sql-client exits 0 after
  # "[ERROR] Could not execute SQL statement" as well.
  if printf '%s\n' "$out" | grep -q '\[ERROR\]'; then
    echo "  !! ${name} failed — see the [ERROR] above" >&2
    FAILED="$FAILED $name"
  elif ! printf '%s\n' "$out" | grep -qE 'Job ID: *[0-9a-f]{32}|JobID [0-9a-f]{32}'; then
    echo "  !! ${name} did not produce a Job ID — nothing was submitted" >&2
    FAILED="$FAILED $name"
  fi
}

for name in "${JOBS[@]}"; do submit_one "$name"; done

echo
if [ -n "$FAILED" ]; then
  echo "FAILED:${FAILED}" >&2
  exit 1
fi
echo "all requested steps done"
