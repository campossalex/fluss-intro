# Fluss Intro (open-source edition): reference

How the stack is wired, how to run the steps from a terminal, and what to do when something
goes wrong. The attendee walkthrough is [WORKSHOP.md](../WORKSHOP.md).

## Architecture

| Service | Image | Role |
|---|---|---|
| `zookeeper`, `coordinator-server`, `tablet-server` | `zookeeper:3.9.1`, `apache/fluss:1.0.0` | The Fluss cluster. `remote.data.dir` is a shared volume (`fluss-remote-data`), mounted in the Flink containers too. |
| `jobmanager`, `taskmanager` | `apache/flink:1.20-java17` | A Flink session cluster, 8 task slots: the five streaming jobs plus interactive queries. |
| `sql-gateway` | same | The Flink SQL Gateway: the SQL engine behind the editor, as a REST API. |
| `sql-client` | same | Idles, for `docker compose exec` and `scripts/submit.sh`. |
| `sql-editor` | `python:3.11-slim` | `sql-editor/app.py`: the SQL editor (`/`) and the Pipeline Builder (`/pipeline`), a Flask proxy in front of the gateway. |
| `homepage` | `nginx:1.27-alpine` | `homepage/index.html` on port 80. |
| `lookup-api` | `python:3.11-slim` | `lookup-api/app.py`: the shipment lookup API on port 9051 — `GET /shipments/<id>` is a pyfluss primary-key lookup on `fluss.app.shipment_status`; `GET /shipments/sample`, `GET /health`, and a UI at `/`. |
| `scan-gen` | `python:3.11-slim` | `datagen/scan_events.py`: appends scans to `fluss.app.scan_events_log` with `pyfluss`; waits until the table exists; re-reads its carriers from PostgreSQL every 30 s. |
| `postgres` | `postgres:15` | Database `shipments` (`postgres/pg_shipments_ddl.sql`): `route_reference`, the CDC source, with `cdc_user`, slot `flink_cdc_routes_slot` and publication `routes_pub`; the three Lab 4 sink tables. Started with `wal_level=logical`. |
| `grafana` | `grafana/grafana:11.3.0` | Provisioned datasource `shipments_dwh` and the dashboard `grafana/dashboards/shipment_tracking.json`. |

The connector JARs in `lib/` (`fluss-flink-1.20-1.0.0`, `flink-sql-connector-postgres-cdc-3.2.1`,
`flink-connector-jdbc-core` and `-postgres` 3.3.0-1.20, the PostgreSQL driver) are mounted into every Flink container and
copied onto the classpath at start. `scripts/download-jars.sh` fetches them again.

## Running the steps by hand

```bash
# Every job of the labs, in order (what ./start.sh does without --services-only)
docker compose exec sql-client /opt/submit.sh

# Some of them, by name: status routes enrich scan-window delivery-summary transit-time
docker compose exec sql-client /opt/submit.sh status

# An interactive SQL client: the fluss and shipments_pg catalogs are already registered;
# only the CDC source table needs loading
docker compose exec sql-client /opt/flink/bin/sql-client.sh -i /opt/sql/ddl/03_sources.sql

# Change a route in PostgreSQL directly
docker compose exec postgres psql -U root -d shipments -c \
  "UPDATE route_reference SET avg_delivery_days = 1 WHERE carrier_code = 'DHL';"

# PostgreSQL
docker compose exec postgres psql -U root -d shipments
```

### Files

| Path | What it holds |
|---|---|
| `flink_sql/ddl/01_fluss.sql` | The `fluss` catalog and the four Fluss tables, cut into `-- --- Step N:` sections |
| `flink_sql/ddl/02_postgres.sql` | The `shipments_pg` JDBC catalog |
| `flink_sql/ddl/03_sources.sql` | The `route_reference_cdc` postgres-cdc source (session-scoped) |
| `flink_sql/jobs/*.sql` | One streaming job per file, each with `SET 'pipeline.name'` |
| `flink_sql/explore.sql` | The interactive queries of every lab, one section each |

The SQL editor's Examples menu and every Pipeline Builder action are cut from these files at
request time, so editing a file changes the editor without a restart.

## What survives a session

- **Fluss tables** live in Fluss: they are there for every session and every job.
- **Catalog registrations** (`fluss`, `shipments_pg`) are kept by Flink's file-based
  CatalogStore (`catalog-store` volume, shared by `sql-gateway` and `sql-client`).
- **`route_reference_cdc`** lives in Flink's in-memory catalog (`source_catalog`, named with
  `table.builtin-catalog-name`): gone with the session. `submit.sh` loads it into every job's
  session with `-i`; the editor and the Pipeline Builder re-create it in a new session while
  `routes-cdc` is running.

## The Pipeline Builder

`/api/pipeline/status` reports, per box, whether its table exists (`SHOW TABLES`) and
whether it returns a row (a background `SELECT 1 … LIMIT 1` probe), and per arrow which
running Flink job writes its table — read from each job's plan, so an edited
`pipeline.name` does not matter. `scan_events_log` is probed whenever it exists, because the
generator is not a Flink job. The page registers `shipments_pg` on load, since its PostgreSQL
tables already exist, and `route_reference_cdc` when `routes-cdc` runs but the session lacks it.
*Add route* and *Update SLA* write PostgreSQL through `shipments_pg`: each is a short bounded
Flink job (a JDBC upsert).

## Implementation notes

- **No `'table.type'`.** The Ververica Platform version declares `'table.type' = 'log'` or
  `'primary-key'`. Open-source Fluss 1.0 rejects it:
  `InvalidConfigException: 'table.type' is not a recognized Fluss table property`. A table
  with a PRIMARY KEY is a PK table; without one, a Log table.
- **UTC everywhere.** `scan_time` is a naive `datetime.now()` from a `TZ=UTC` container; the
  Flink planner is pinned to `table.local-time-zone: UTC` to match.
- **Checkpointing** (30 s) is set in the client configuration of `sql-gateway` and
  `sql-client`, because it is baked into the JobGraph by whoever submits the job.
- **Interactive queries** run with at-least-once checkpointing (set per statement by the
  editor), so streaming SELECT rows appear as they come instead of every 30 s.

## The shipment lookup API

The online-serving side of Lab 2, modelled on the inference service of
[realtime-feature-store](https://github.com/campossalex/realtime-feature-store). pyfluss is
async and Flask is not, so the service runs one asyncio loop in a background thread and makes
**every** pyfluss call on it — not just the `await`: pyfluss creates its futures on the
calling thread's running loop, and a call made from a Flask thread fails with
`no running event loop`. The connection, the table handle and a pool of `Lookuper`s are opened
on demand and reused; on an error they are re-opened once, so the service starts before Lab 2
and survives a table being dropped and re-created.

**Lookup latency with pyfluss 1.0** (measured with the latency test below, on a laptop):

- A lookup takes **~100 ms** (p50 ~103 ms, p99 ~107–113 ms). The steady value suggests a fixed
  client-side wait rather than server work; pyfluss 1.0 exposes no lookup setting to change it
  (the Java client's `client.lookup.*` options are rejected as unknown properties).
- **One `Lookuper` handles one lookup at a time.** Eight concurrent lookups through a single
  `Lookuper` each took ~820 ms (~10 lookups/s in total). Separate `Lookuper`s run in
  parallel: 8 → ~78/s, 32 → ~314/s, with per-lookup latency unchanged. So the API
  round-robins over a pool (`LOOKUPER_POOL`, default 8), and each latency-test worker gets its
  own.

### Latency test endpoints

| Endpoint | What it does |
|---|---|
| `POST /bench` `{"shipment_id"?, "duration_s": 60, "concurrency": 1}` | Starts a test (5–600 s, 1–64 in flight). No `shipment_id`: a live one is picked. `409` while another test runs, `503` before Lab 2. |
| `GET /bench` | The current or last test: `state`, progress, `count`, `throughput_per_s`, `errors`, `latency_ms` (`min`, `mean`, `p50`, `p90`, `p99`, `max`), a per-second `timeline` (p50, p99) and a 20-bucket `histogram`. |
| `POST /bench/stop` | Ends the running test early; its results stay readable. |

Latency is measured inside the service with `time.perf_counter()` around
`await lookuper.lookup(...)`; percentiles are exact (nearest rank over every sample). One test
runs at a time, in the service's Fluss loop alongside normal API traffic.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `scan-gen` logs `[WAIT] Fluss table not yet available` | Expected until Lab 1, Step 2 creates `fluss.app.scan_events_log`. |
| The lookup API answers `503 … does not exist yet` | Expected until Lab 2, Step 4 creates `shipment_status`; it opens the table on the next request after that. |
| The lookup API returns `404` for every ID | `shipment_status` is empty: deploy `shipment-status` (Lab 2, Step 5). |
| `scans_enriched` stays empty | `route_reference` is empty: deploy `routes-cdc` (Step 8). A lookup join drops scans with no matching carrier. |
| `routes-cdc` fails with `replication slot "flink_cdc_routes_slot" is active` | Only one reader per slot: another `routes-cdc` is already running. Cancel the extra one in the Flink Web UI. |
| `Object 'route_reference_cdc' not found` | A new session without the CDC table: run `ddl/03_sources.sql` (Examples → Step 8 · the postgres-cdc source table). |
| A new route gets no scans | scan-gen re-reads carriers every 30 s; check `docker compose logs scan-gen` for a `[CARRIERS]` line. |
| PostgreSQL tables stay empty | A window is written only when it closes: up to 5 minutes plus 5 s after the jobs start. |
| `Could not find any factory for identifier 'jdbc'` | `lib/` is missing the JDBC JARs: `bash scripts/download-jars.sh`, then `docker compose up -d --force-recreate`. |
| `Multiple factories for identifier 'jdbc'` | The all-in-one `flink-connector-jdbc` JAR is in `lib/`; delete it (the download script does). |
| Jobs stay in `CREATED` | No free task slots: cancel streaming SELECTs you no longer need in the Flink Web UI. |
| Ports already allocated | Another stack uses 80/3000/5432/8081/8083/8084/9123: stop it first. |

## Teardown

```bash
docker compose down -v
```

`./start.sh --reset` is the clean restart: it removes every volume (Fluss data, PostgreSQL,
the catalog store, checkpoints) before starting again.
