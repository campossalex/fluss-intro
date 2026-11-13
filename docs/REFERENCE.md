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
| `scan-gen` | `python:3.11-slim` | `datagen/scan_events.py`: appends scans to `fluss.app.scan_events_log` with `pyfluss`; waits until the table exists. |
| `postgres` | `postgres:15` | Database `shipments`, the three Lab 4 sink tables from `postgres/pg_shipments_ddl.sql`. |
| `grafana` | `grafana/grafana:11.3.0` | Provisioned datasource `shipments_dwh` and the dashboard `grafana/dashboards/shipment_tracking.json`. |

The connector JARs in `lib/` (`fluss-flink-1.20-1.0.0`, `flink-connector-jdbc-core` and
`-postgres` 3.3.0-1.20, the PostgreSQL driver) are mounted into every Flink container and
copied onto the classpath at start. `scripts/download-jars.sh` fetches them again.

## Running the steps by hand

```bash
# Every job of the labs, in order (what ./start.sh does without --services-only)
docker compose exec sql-client /opt/submit.sh

# Some of them, by name: status seed enrich scan-window delivery-summary transit-time
docker compose exec sql-client /opt/submit.sh status

# An interactive SQL client: the fluss and shipments_pg catalogs are already registered
docker compose exec sql-client /opt/flink/bin/sql-client.sh

# PostgreSQL
docker compose exec postgres psql -U root -d shipments
```

### Files

| Path | What it holds |
|---|---|
| `flink_sql/ddl/01_fluss.sql` | The `fluss` catalog and the four Fluss tables, cut into `-- --- Step N:` sections |
| `flink_sql/ddl/02_postgres.sql` | The `shipments_pg` JDBC catalog |
| `flink_sql/seed/route_reference.sql` | The five carriers (a bounded INSERT) |
| `flink_sql/jobs/*.sql` | One streaming job per file, each with `SET 'pipeline.name'` |
| `flink_sql/explore.sql` | The interactive queries of every lab, one section each |

The SQL editor's Examples menu and every Pipeline Builder action are cut from these files at
request time, so editing a file changes the editor without a restart.

## What survives a session

- **Fluss tables** live in Fluss: they are there for every session and every job.
- **Catalog registrations** (`fluss`, `shipments_pg`) are kept by Flink's file-based
  CatalogStore (`catalog-store` volume, shared by `sql-gateway` and `sql-client`).
- Nothing in this lab lives in Flink's in-memory catalog, so a new session needs no DDL.

## The Pipeline Builder

`/api/pipeline/status` reports, per box, whether its table exists (`SHOW TABLES`) and
whether it returns a row (a background `SELECT 1 … LIMIT 1` probe), and per arrow which
running Flink job writes its table — read from each job's plan, so an edited
`pipeline.name` does not matter. `scan_events_log` and `route_reference` are probed whenever
they exist, because the generator and the bounded seed are not running Flink jobs. The page
registers `shipments_pg` on load, since its three PostgreSQL tables already exist.

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

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `scan-gen` logs `[WAIT] Fluss table not yet available` | Expected until Lab 1, Step 2 creates `fluss.app.scan_events_log`. |
| `scans_enriched` stays empty | `route_reference` is empty: run the Step 8 seed. A lookup join drops scans with no matching carrier. |
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
