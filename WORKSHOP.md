# Real-time Shipment Tracking with Apache Fluss — Open-Source Edition

In this lab you will learn **Apache Fluss** by building a real-time shipment tracking
pipeline from the ground up, entirely on open source: **Apache Flink 1.20**, **Apache
Fluss 1.0**, PostgreSQL and Grafana.

A data generator is already running. It writes synthetic scan events **directly to
Fluss** with the Fluss Python client — no Kafka, no ingestion job. Each event is a courier
scanning a package at a stage of delivery: `PICKED_UP`, `IN_TRANSIT`, `OUT_FOR_DELIVERY`
or `DELIVERED`. You will create the Log table (which starts the data flowing), keep each
shipment's latest state in a Primary Key table, enrich every scan with carrier metadata
through a temporal lookup join, and compute analytics that Grafana shows live.

This is the open-source edition of the Ververica Platform `fluss_intro` lab. The labs and
step numbers are the same; what changes is where you type the SQL and how a job gets
deployed.

## Your tools

| Tool | URL | What you use it for |
|------|-----|---------------------|
| **Lab homepage** | http://localhost | The architecture you are about to build, links to every tool, connection details |
| **Flink SQL editor** | http://localhost:8084 | Write and run every Flink SQL statement |
| **Pipeline Builder** | http://localhost:8084/pipeline | The same labs, clicked through the architecture diagram |
| **Flink Web UI** | http://localhost:8081 | See, inspect and cancel the running jobs |
| **Grafana** | http://localhost:3000 | The shipment analytics dashboard (Lab 5) |

PostgreSQL (database `shipments`) is at `localhost:5432` from your machine, user `root`,
password `admin1`. Inside the lab network — in the SQL below — it is `postgres:5432`.

Your instructor started the environment with `./start.sh --services-only`: everything is
running, and nothing has been created yet.

### Using the Flink SQL editor

- Type or paste SQL into the editor and press **Run** — or <kbd>⌘</kbd>/<kbd>Ctrl</kbd>+<kbd>Enter</kbd>.
  With text selected, only the selection runs; otherwise the whole editor runs, one
  statement after another, each with its own result panel.
- A streaming `SELECT` keeps its panel live until you press **Cancel** on that panel.
  Cancel it before moving on — each one is a real Flink job holding a slot.
- An `INSERT INTO` becomes a **long-running Flink job**. Its panel links to the job in the
  Flink Web UI, and it keeps running when you cancel, close the tab or start a new query.
  The `INSERT` *is* the deployment.
- Every statement in this guide is also in the **Examples** menu, in lab order.
- The **Catalog** button opens a browser of catalogs, databases, tables and columns.
- Your session survives a page reload, and the Pipeline Builder shares it.

### Or: the Pipeline Builder

http://localhost:8084/pipeline draws the pipeline as a diagram: every box is a table,
every arrow a Flink job, and each has buttons — **Create**, **Deploy**, **View data** — that
open the exact SQL of the step below, ready to run. Choose **Guide me** for a callout that
points at the next button. Boxes turn green when their table has rows.

---

## Lab 1: Fluss Catalog & Log Table

In this lab you will set up the Fluss catalog, create the Log table the data generator
writes to, and confirm that scan events are landing in real time.

### Step 1: Create the Fluss catalog

Fluss is a streaming storage for real-time analytics. Unlike Kafka, Fluss tables keep data
with no retention limit and come in two kinds: append-only **Log tables** and upsert-capable
**Primary Key (PK) tables**. Creating the catalog registers Fluss in Flink, so you can create
and query Fluss tables from SQL.

Open the **Flink SQL editor** and run:

```sql
CREATE CATALOG IF NOT EXISTS fluss WITH (
  'type'              = 'fluss',
  'bootstrap.servers' = 'coordinator-server:9123'
);
```

> [!NOTE]
> Open the **Catalog** panel: `fluss` is there. All Fluss tables in this lab live under
> `fluss.app.*`. The catalog registration is persisted, so it is still there after a new
> session or a restart of the SQL Gateway.

### Step 2: Create the Fluss database and Log table

A Fluss **Log table** is an ordered, append-only store — like a Kafka topic, but with no
retention limit and native Flink integration. The `WATERMARK` lets it drive event-time
windows later.

As soon as this table exists, the data generator starts writing scan events to it.

```sql
CREATE DATABASE IF NOT EXISTS fluss.app;

CREATE TABLE IF NOT EXISTS fluss.app.scan_events_log (
  `scan_id`      STRING,
  `shipment_id`  STRING,
  `carrier_code` STRING,
  `status`       STRING,
  `location`     STRING,
  `scan_time`    TIMESTAMP(3),
  WATERMARK FOR `scan_time` AS `scan_time` - INTERVAL '5' SECOND
);
```

> [!NOTE]
> There is no `'table.type'` option: open-source Fluss makes a table a **Log table** when it
> has no primary key, and a **PK table** when it has one.

### Step 3: Query the Log table

Run a streaming SELECT to confirm scan events are arriving:

```sql
SELECT * FROM fluss.app.scan_events_log;
```

Rows appear in real time — one per scan, with `status` moving through `PICKED_UP` →
`IN_TRANSIT` → `OUT_FOR_DELIVERY` → `DELIVERED`. Press **Cancel** on the result panel to stop.

> [!NOTE]
> The generator checks for the table every 5 seconds, so the first rows can take a few
> seconds. `docker compose logs -f scan-gen` shows it writing.

---

## Lab 2: PK Table — Real-time Shipment State

A **Primary Key table** stores exactly one row per key. Every write with the same key is an
upsert: newer values overwrite older ones. In this lab you build a live view of where every
shipment is right now.

### Step 4: Create the shipment_status PK table

```sql
CREATE TABLE IF NOT EXISTS fluss.app.shipment_status (
  `shipment_id`  STRING,
  `carrier_code` STRING,
  `status`       STRING,
  `location`     STRING,
  `last_scan`    TIMESTAMP(3),
  PRIMARY KEY (`shipment_id`) NOT ENFORCED
) WITH (
  'bucket.num' = '3'
);
```

### Step 5: Start the state aggregation job

This job reads the Log table and writes to the PK table. Because `shipment_id` is the
primary key, only each shipment's latest scan survives.

```sql
SET 'pipeline.name' = 'shipment-status';

INSERT INTO fluss.app.shipment_status
SELECT
  `shipment_id`,
  `carrier_code`,
  `status`,
  `location`,
  `scan_time` AS `last_scan`
FROM fluss.app.scan_events_log;

RESET 'pipeline.name';
```

The result panel links to the new job in the Flink Web UI. It runs until you cancel it.

### Step 6: Point lookup

Find a real `shipment_id`, then look it up in the PK table:

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT DISTINCT `shipment_id` FROM fluss.app.scan_events_log LIMIT 5;
```

Copy a value, substitute it below, and run:

```sql
SELECT * FROM fluss.app.shipment_status
WHERE `shipment_id` = 'SHP-XXXXX';
```

One row comes back — always the shipment's most recent scan. Run it again after a few seconds
and the status may have moved on. A key lookup is served by Fluss's key index directly.

### Step 7: Compare the Log and PK tables

```sql
SELECT COUNT(*) AS total_scans      FROM fluss.app.scan_events_log;
SELECT COUNT(*) AS active_shipments FROM fluss.app.shipment_status;
SET 'execution.runtime-mode' = 'streaming';
```

> [!NOTE]
> The Log table grows with every scan — the complete history. The PK table holds one row per
> shipment — the current state. Both come from the same stream and answer different
> questions. The last `SET` puts the session back in streaming mode for the next labs.

---

## Lab 3: Temporal Lookup Join

A **temporal lookup join** enriches a stream with a PK table's data at the moment each event
is processed — no separate enrichment service, no cache: Fluss serves each lookup as a point
read.

### Step 8: Create and seed the route_reference PK table

Static carrier metadata, one row per carrier: full name, hub city and SLA (average delivery
days).

```sql
CREATE TABLE IF NOT EXISTS fluss.app.route_reference (
  `carrier_code`      STRING,
  `carrier_name`      STRING,
  `hub_city`          STRING,
  `avg_delivery_days` INT,
  PRIMARY KEY (`carrier_code`) NOT ENFORCED
) WITH (
  'bucket.num' = '1'
);

INSERT INTO fluss.app.route_reference VALUES
  ('DHL', 'DHL Express',          'Frankfurt',  2),
  ('UPS', 'UPS Worldwide',        'Cologne',    3),
  ('FDX', 'FedEx International',  'Amsterdam',  2),
  ('GLS', 'GLS Group',            'Bologna',    4),
  ('TNT', 'TNT Express',          'Liège',      3);
```

The `INSERT … VALUES` is a bounded Flink job: it writes five rows and finishes.

### Step 9: Inspect the reference table

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT * FROM fluss.app.route_reference;
SET 'execution.runtime-mode' = 'streaming';
```

Five rows, one per carrier.

### Step 10: Create the scans_enriched Log table

Every scan, enriched with carrier metadata: the single source for the Lab 4 analytics.

```sql
CREATE TABLE IF NOT EXISTS fluss.app.scans_enriched (
  `scan_id`           STRING,
  `shipment_id`       STRING,
  `carrier_code`      STRING,
  `carrier_name`      STRING,
  `status`            STRING,
  `location`          STRING,
  `hub_city`          STRING,
  `avg_delivery_days` INT,
  `scan_time`         TIMESTAMP(3),
  WATERMARK FOR `scan_time` AS `scan_time` - INTERVAL '5' SECOND
);
```

### Step 11: Start the enrichment job

`FOR SYSTEM_TIME AS OF t.proc_time` reads `route_reference` at the processing time of each
scan, so a live update to `route_reference` is picked up by later scans without a restart.
`PROCTIME()` is added in a subquery; it is only the lookup's clock and is not written out.

```sql
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

RESET 'pipeline.name';
```

Check the enriched events:

```sql
SELECT * FROM fluss.app.scans_enriched;
```

Every row now carries `carrier_name`, `hub_city` and `avg_delivery_days`. Cancel the query.

---

## Lab 4: Analytics Jobs → PostgreSQL

`scans_enriched` has a watermark and the carrier metadata, so it can drive three 5-minute
tumbling-window jobs that write to PostgreSQL, where Grafana reads them.

| Table | Source | What it measures |
|---|---|---|
| `scan_window_5m` | scans_enriched | Scan volume per carrier + location per window |
| `delivery_summary` | scans_enriched | Shipments per status + carrier per window |
| `transit_time_5m` | scans_enriched (DELIVERED only) | Delivery throughput + SLA per carrier per window |

### Step 12: Create the PostgreSQL JDBC catalog

A JDBC catalog lets Flink see and write PostgreSQL tables without declaring their schema.

```sql
CREATE CATALOG IF NOT EXISTS shipments_pg WITH (
  'type'             = 'jdbc',
  'base-url'         = 'jdbc:postgresql://postgres:5432',
  'default-database' = 'shipments',
  'username'         = 'root',
  'password'         = 'admin1'
);
```

Expand `shipments_pg` in the Catalog panel: the three tables were created when PostgreSQL
started.

> [!NOTE]
> If you opened the Pipeline Builder, `shipments_pg` already exists — the page registers it so
> its PostgreSQL boxes can show their state. The statement is `IF NOT EXISTS`, so it is a
> no-op then.

### Step 13: Verify the PostgreSQL tables

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT COUNT(*) AS scan_window_rows FROM shipments_pg.shipments.scan_window_5m;
SET 'execution.runtime-mode' = 'streaming';
```

Zero rows: the jobs below fill it.

### Step 14: Job A — Scan volume per window

```sql
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

RESET 'pipeline.name';
```

### Step 15: Job B — Shipment status breakdown per window

`COUNT(DISTINCT shipment_id)` avoids double-counting a shipment scanned more than once in the
same window.

```sql
SET 'pipeline.name' = 'delivery-summary';

INSERT INTO shipments_pg.shipments.delivery_summary
SELECT
  window_start,
  window_end,
  `carrier_code`,
  `carrier_name`,
  `status`,
  COUNT(DISTINCT `shipment_id`) AS shipment_count
FROM TABLE(
  TUMBLE(TABLE fluss.app.scans_enriched, DESCRIPTOR(`scan_time`), INTERVAL '5' MINUTE)
)
GROUP BY window_start, window_end, `carrier_code`, `carrier_name`, `status`;

RESET 'pipeline.name';
```

### Step 16: Job C — Delivery throughput per window

`DELIVERED` events only, per carrier and window, with the SLA captured by the Lab 3 lookup
join — so Grafana can compare throughput against each carrier's promise.

```sql
SET 'pipeline.name' = 'transit-time-5m';

INSERT INTO shipments_pg.shipments.transit_time_5m
SELECT
  window_start,
  window_end,
  `carrier_code`,
  `carrier_name`,
  `avg_delivery_days` AS sla_days,
  COUNT(*) AS delivered_count
FROM TABLE(
  TUMBLE(TABLE fluss.app.scans_enriched, DESCRIPTOR(`scan_time`), INTERVAL '5' MINUTE)
)
WHERE `status` = 'DELIVERED'
GROUP BY window_start, window_end, `carrier_code`, `carrier_name`, `avg_delivery_days`;

RESET 'pipeline.name';
```

A window is written when it closes: at the next :00, :05, :10 … plus the 5-second watermark
delay. Then check what landed:

```sql
SET 'execution.runtime-mode' = 'batch';
SELECT carrier_name, SUM(scan_count) AS total_scans
FROM shipments_pg.shipments.scan_window_5m
GROUP BY carrier_name
ORDER BY total_scans DESC;
SET 'execution.runtime-mode' = 'streaming';
```

One row per carrier. Empty? Wait for the first window to close and run it again.

---

## Lab 5: Grafana Dashboard

### Steps 17–18: The datasource and the dashboard

Nothing to set up: Grafana's PostgreSQL datasource (`shipments_dwh`) and the dashboard are
provisioned when the stack starts. Open **http://localhost:3000** — it opens on the
*Fluss Intro — Shipment Tracking Analytics* dashboard.

### Step 19: Explore the dashboard

- **KPI row**: Total Scans, Active Shipments, Delivered, Carriers Tracked.
- **Scan Volume by Carrier**: which carrier generates the most scans per window.
- **Deliveries Over Time**: which carrier completes the most deliveries per window.
- **Top Locations**: the busiest scan cities across all carriers.
- **Shipments by Status / Status Breakdown**: shipments per lifecycle status.
- **Carrier SLA vs Deliveries**: `sla_days` (captured by the Lab 3 join) next to `delivered_count`.

The dashboard shows the last 30 minutes and refreshes every 30 seconds.

### Step 20: Update a carrier SLA and watch it propagate

`route_reference` supports live upserts. Change DHL's SLA from 2 days to 1:

```sql
INSERT INTO fluss.app.route_reference VALUES
  ('DHL', 'DHL Express', 'Frankfurt', 1);
```

`carrier_code = 'DHL'` already exists, so Fluss overwrites the row in place. Scans enriched
after this point carry `avg_delivery_days = 1` for DHL, and the next closed window in
`transit_time_5m` shows `sla_days = 1` for DHL in Grafana's Carrier SLA table — without
restarting any Flink job.

This is the core of Fluss PK tables: reference data that updates in place and is picked up
immediately by every running lookup join.

Congrats, you made it! You built a complete real-time Fluss pipeline — from a Python generator
writing directly to Fluss, through Log and PK tables, to enrichment via a temporal lookup join,
and live analytics in Grafana.
