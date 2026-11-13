#!/usr/bin/env bash
# start.sh — Fluss Intro lab bootstrap
#
# The fluss_intro labs on open source — Apache Flink 1.20 and Apache Fluss 1.0, with
# PostgreSQL and Grafana — all in Docker Compose.
#
# Usage:
#   ./start.sh                   bring everything up and submit every lab job (demo mode)
#   ./start.sh --services-only   bring the stack up, submit nothing (the workshop mode)
#   ./start.sh --reset           docker compose down -v first: empty Fluss, empty PostgreSQL

set -euo pipefail
cd "$(dirname "$0")"

# ── Options ────────────────────────────────────────────────────────────────────

SERVICES_ONLY=0
RESET=0

usage() {
  cat <<'USAGE'
Usage: ./start.sh [--services-only] [--reset]

  (no options)      Start the stack, create every table and submit every Flink job —
                    the finished state of all the labs.
  --services-only   Start the stack (Fluss, Flink, PostgreSQL, Grafana, the generator)
                    but create and submit nothing. This is the state attendees start the
                    lab from; the manual steps are printed at the end either way.
  --reset           Remove every container and volume first. Fluss tables, PostgreSQL
                    rows and the Flink catalog store are all rebuilt.
  -h, --help        Show this message.
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --services-only) SERVICES_ONLY=1; shift ;;
    --reset)         RESET=1; shift ;;
    -h|--help)       usage; exit 0 ;;
    *)               echo "ERROR: unknown option '$1'" >&2; echo >&2; usage >&2; exit 2 ;;
  esac
done

# ── Prerequisite checks ────────────────────────────────────────────────────────

require_cmd() { command -v "$1" &>/dev/null || { echo "ERROR: '$1' not found"; exit 1; }; }
require_cmd docker
require_cmd curl
require_cmd jq

# ── The manual runbook ─────────────────────────────────────────────────────────

print_manual_steps() {
  cat <<'STEPS'

  ── Run the labs step by step ────────────────────────────────────────────────

  WORKSHOP.md walks through every step. In short:

  1. Open the Flink SQL editor: http://localhost:8084  (or the Pipeline Builder at
     http://localhost:8084/pipeline). The Examples menu has every lab step in order.
     Creating fluss.app.scan_events_log (Lab 1, Step 2) starts the data flowing.

  2. Submit the jobs — from the editor, or from a terminal:

       docker compose exec sql-client /opt/submit.sh status            # Lab 2
       docker compose exec sql-client /opt/submit.sh routes enrich     # Lab 3
       docker compose exec sql-client /opt/submit.sh scan-window delivery-summary transit-time   # Lab 4

  3. Lab 5 — Grafana: http://localhost:3000 (datasource and dashboard provisioned)

  ── Useful commands ──────────────────────────────────────────────────────────

  Running jobs:
       curl -s http://localhost:8081/jobs/overview | jq -r '.jobs[] | "\(.state) \(.name)"'

  The generator's output:
       docker compose logs -f scan-gen

  Change a route in PostgreSQL and watch CDC carry it to Fluss:
       docker compose exec postgres psql -U root -d shipments -c \
         "UPDATE route_reference SET avg_delivery_days = 1 WHERE carrier_code = 'DHL';"

  A shipment's current state, straight from the Fluss PK table:
       curl -s http://localhost:9051/shipments/sample
       curl -s http://localhost:9051/shipments/SHP-12345

  What the analytics jobs have written:
       docker compose exec postgres psql -U root -d shipments -c \
         "SELECT carrier_name, SUM(scan_count) FROM scan_window_5m GROUP BY 1 ORDER BY 2 DESC;"

STEPS
}

# ── Step 1: Connector JARs ─────────────────────────────────────────────────────

if [ -z "$(ls -A lib/fluss-flink-*.jar 2>/dev/null)" ] || [ -z "$(ls -A lib/postgresql-*.jar 2>/dev/null)" ]; then
  echo "==> Downloading connector JARs..."
  bash scripts/download-jars.sh
else
  echo "==> [skip] lib/ already populated ($(ls lib/*.jar 2>/dev/null | wc -l | tr -d ' ') JARs)"
fi

# ── Step 2: Start Docker Compose services ─────────────────────────────────────

if [ "$RESET" = "1" ]; then
  echo ""
  echo "==> Removing containers and volumes..."
  docker compose down -v --remove-orphans
fi

echo ""
echo "==> Starting Docker Compose services..."
docker compose up -d --remove-orphans

echo ""
echo "==> Waiting for core services to be healthy..."

wait_healthy() {
  local service="$1" max_attempts="${2:-30}" attempt=0
  echo -n "    $service "
  until [ "$(docker compose ps "$service" --format '{{.Health}}' 2>/dev/null)" = "healthy" ]; do
    attempt=$((attempt + 1))
    if [ "$attempt" -ge "$max_attempts" ]; then
      echo " TIMEOUT"
      echo "ERROR: $service did not become healthy within $(( max_attempts * 5 ))s"
      docker compose logs "$service" --tail=20
      exit 1
    fi
    echo -n "."
    sleep 5
  done
  echo " OK"
}

wait_healthy postgres 24
wait_healthy coordinator-server 36
wait_healthy tablet-server 36
wait_healthy jobmanager 24
wait_healthy sql-gateway 24
wait_healthy lookup-api 36

echo -n "    sql-editor "
attempt=0
until curl -sf http://localhost:8084/api/config >/dev/null; do
  attempt=$((attempt + 1))
  [ "$attempt" -ge 36 ] && { echo " TIMEOUT"; docker compose logs sql-editor --tail=20; exit 1; }
  echo -n "."
  sleep 5
done
echo " OK"

# ── --services-only stops here ────────────────────────────────────────────────

if [ "$SERVICES_ONLY" = "1" ]; then
  cat <<'BANNER'

══════════════════════════════════════════════════════════════
  Fluss Intro — services running, nothing created yet
══════════════════════════════════════════════════════════════

  Environment home    →  http://localhost        links to everything below
  Flink SQL editor    →  http://localhost:8084   start here
  Pipeline Builder    →  http://localhost:8084/pipeline
  Flink Web UI        →  http://localhost:8081   (no jobs yet)
  Shipment lookup API →  http://localhost:9051   (answers after Lab 2, Step 4)
  Grafana             →  http://localhost:3000   (empty until Lab 4)

  Fluss (from host):       localhost:9123
  PostgreSQL (from host):  localhost:5432   root/admin1   db shipments

  scan-gen is waiting for fluss.app.scan_events_log (Lab 1, Step 2).
BANNER
  print_manual_steps
  echo "  Stop everything:  docker compose down -v"
  echo "══════════════════════════════════════════════════════════════"
  exit 0
fi

# ── Step 3: Submit the pipeline ───────────────────────────────────────────────
# submit.sh runs inside the sql-client container: it loads all DDL into each session
# with -i (creating every table, which also starts scan-gen) and submits each job with -f.

echo ""
echo "==> Submitting Flink SQL jobs..."
docker compose exec -T sql-client /opt/submit.sh || {
  echo ""
  echo "WARNING: at least one step failed — see the output above."
}

echo ""
echo "==> Verifying..."
sleep 5
curl -s http://localhost:8081/jobs/overview | jq -r '.jobs[] | "    \(.state)  \(.name)"' | sort

# ── Step 4: Status summary ────────────────────────────────────────────────────

cat <<'BANNER'

══════════════════════════════════════════════════════════════
  Fluss Intro — Setup Complete
══════════════════════════════════════════════════════════════

  Environment home    →  http://localhost        links to everything below
  Flink SQL editor    →  http://localhost:8084
  Pipeline Builder    →  http://localhost:8084/pipeline
  Flink Web UI        →  http://localhost:8081
  Shipment lookup API →  http://localhost:9051
  Grafana             →  http://localhost:3000   first 5-minute window in ≤ 5 minutes

  Fluss (from host):       localhost:9123
  PostgreSQL (from host):  localhost:5432   root/admin1   db shipments

  Start again without submitting anything:
    ./start.sh --reset --services-only
BANNER
print_manual_steps
echo "  Stop everything:  docker compose down -v"
echo "══════════════════════════════════════════════════════════════"
