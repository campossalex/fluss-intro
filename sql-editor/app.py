"""Web SQL editor and Pipeline Builder for the Fluss Intro lab.

A thin proxy in front of the Flink SQL Gateway: the browser cannot call the gateway
directly (it sends no CORS headers). The Fluss and PostgreSQL tables, and the `fluss` and
`shipments_pg` catalog registrations (kept by the shared CatalogStore), are there for every
session. The one exception is the postgres-cdc source table in flink_sql/ddl/03_sources.sql:
it lives in the session's in-memory catalog, so a new session gets it re-created while the
routes-cdc job that reads it is running.

  - optionally (PRELOAD_DDL=true) runs flink_sql/ddl/*.sql into each new session first,
    like `sql-client.sh -i`. Off by default: in the lab attendees issue the DDL
    themselves. Every statement in those files is IF NOT EXISTS.
  - builds the examples menu from flink_sql/: the DDL files, the explore.sql sections,
    and the job files.

This lab has no secrets, so there is no placeholder substitution and nothing to redact:
statements reach the gateway verbatim.
"""
import glob
import json
import os
import re
import threading
import time

import requests
from flask import Flask, Response, jsonify, request, send_from_directory

GATEWAY = os.environ.get("GATEWAY_URL", "http://sql-gateway:8083").rstrip("/")
SQL_DIR = os.environ.get("SQL_DIR", "/opt/sql")
PRELOAD_DDL = os.environ.get("PRELOAD_DDL", "false").lower() in ("1", "true", "yes")
FLINK = os.environ.get("FLINK_URL", "http://jobmanager:8081").rstrip("/")
# Flink's built-in, in-memory catalog: table.builtin-catalog-name in docker-compose.yml.
BUILTIN_CATALOG = os.environ.get("BUILTIN_CATALOG", "source_catalog")

app = Flask(__name__, static_folder="static")


# ── SQL text handling ─────────────────────────────────────────────────────────

def split_statements(sql):
    """Split a script into statements on `;`, dropping comments.

    A character scanner rather than str.split: string literals can hold `;` or `--`,
    and explore.sql puts a `--` comment inside a WITH (...) clause.
    """
    out, buf, i, n = [], [], 0, len(sql)
    while i < n:
        c = sql[i]
        if c in ("'", "`", '"'):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # doubled quote is an escape
                        j += 2
                        continue
                    break
                j += 1
            buf.append(sql[i:j + 1])
            i = j + 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif sql.startswith("/*+", i):
            # A hint, not a comment: `/*+ OPTIONS('snapshot-id' = '...') */` is part of
            # the statement, and dropping it would silently run a different query.
            j = sql.find("*/", i + 3)
            j = n if j < 0 else j + 2
            buf.append(sql[i:j])
            i = j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
        elif c == ";":
            out.append("".join(buf))
            buf = []
            i += 1
        else:
            buf.append(c)
            i += 1
    out.append("".join(buf))
    return [s.strip() for s in out if s.strip()]


def ddl_files():
    return sorted(glob.glob(os.path.join(SQL_DIR, "ddl", "*.sql")))


def render_ddl():
    statements = []
    for path in ddl_files():
        with open(path) as f:
            statements += split_statements(f.read())
    return statements


DDL = render_ddl()

BANNER = re.compile(r"^-- -{3,} (.+?)\s*$")


PIPELINE_NAME = re.compile(r"SET\s+'pipeline\.name'\s*=\s*'([^']+)'", re.I)

# Appended to each job example. The job file's SET 'pipeline.name' is a session option,
# so without this every SELECT run afterwards would show up in the Flink UI under the
# job's name.
RESET_NAME = """
-- Added by the editor: stop later statements in this session inheriting the job name.
RESET 'pipeline.name';
"""


# The examples menu, in WORKSHOP.md's order — the same order as the pipeline page: each
# lab's tables, sources, jobs and queries together, labelled with their step. Entries
# point into the SQL files by section title, so the SQL itself lives in one place.
#   ("ddl", file, section prefix)  a section of a ddl/ file
#   ("file", path)                 a whole file
#   ("job", file)                  a jobs/ file, plus a RESET of its pipeline.name
#   ("explore", section prefix)    a section of explore.sql
#   ("note", sql)                  guidance with nothing to run
MENU = [
    ("Lab 1 · Fluss catalog & Log table", [
        ("Step 1 · the Fluss catalog", ("ddl", "01_fluss.sql", "Step 1:")),
        ("Step 2 · database + scan_events_log", ("ddl", "01_fluss.sql", "Step 2:")),
        ("Step 3 · scans landing in Fluss", ("explore", "Lab 1, Step 3:")),
    ]),
    ("Lab 2 · PK table: shipment state", [
        ("Step 4 · shipment_status (PK table)", ("ddl", "01_fluss.sql", "Step 4:")),
        ("Step 5 · job: shipment-status", ("job", "20_shipment_status.sql")),
        ("Step 6 · pick a shipment_id", ("explore", "Lab 2, Step 6: pick")),
        ("Step 6 · point lookup", ("explore", "Lab 2, Step 6: point")),
        ("Step 7 · Log vs PK row counts", ("explore", "Lab 2, Step 7:")),
    ]),
    ("Lab 3 · Temporal lookup join", [
        ("Step 8 · route_reference (PK table)", ("ddl", "01_fluss.sql", "Step 8:")),
        ("Step 8 · the postgres-cdc source table", ("ddl", "03_sources.sql", "Step 8:")),
        ("Step 8 · job: routes-cdc", ("job", "25_routes_cdc.sql")),
        ("Step 9 · the reference table in Fluss", ("explore", "Lab 3, Step 9: the reference")),
        ("Step 9 · the routes in PostgreSQL", ("explore", "Lab 3, Step 9: the routes")),
        ("Step 9 · the CDC changelog", ("explore", "Lab 3, Step 9: the CDC")),
        ("Step 10 · scans_enriched (Log table)", ("ddl", "01_fluss.sql", "Step 10:")),
        ("Step 11 · job: scan-enrichment", ("job", "30_enrichment.sql")),
        ("Step 11 · enriched scans", ("explore", "Lab 3, Step 11:")),
    ]),
    ("Lab 4 · Analytics → PostgreSQL", [
        ("Step 12 · the shipments_pg catalog", ("file", "ddl/02_postgres.sql")),
        ("Step 13 · the PostgreSQL tables", ("explore", "Lab 4, Step 13:")),
        ("Step 14 · job A: scan-window-5m", ("job", "40_scan_window_5m.sql")),
        ("Step 15 · job B: delivery-summary", ("job", "41_delivery_summary.sql")),
        ("Step 16 · job C: transit-time-5m", ("job", "42_transit_time_5m.sql")),
        ("Step 16 · scans per carrier", ("explore", "Lab 4, Step 16:")),
    ]),
    ("Lab 5 · Grafana", [
        ("Step 20 · update a carrier SLA", ("explore", "Lab 5, Step 20:")),
        ("Step 21 · add a route", ("explore", "Lab 5, Step 21:")),
    ]),
]


def job_example(name):
    sql = read_sql(os.path.join("jobs", name))
    return sql.rstrip() + "\n" + (RESET_NAME if PIPELINE_NAME.search(sql) else "")


def load_examples():
    """The examples menu: MENU in lab order, then the complete DDL files. A job or an
    explore.sql section MENU does not list lands in "Other", so none goes missing."""
    explore = load_sections(os.path.join(SQL_DIR, "explore.sql"))
    used_explore, used_jobs, examples = set(), set(), []
    for group, items in MENU:
        for title, ref in items:
            kind = ref[0]
            if kind == "ddl":
                body = next(b for t, b in load_sections(os.path.join(SQL_DIR, "ddl", ref[1])) if t.startswith(ref[2]))
            elif kind == "file":
                body = read_sql(ref[1])
                if PIPELINE_NAME.search(body):
                    body = body.rstrip() + "\n" + RESET_NAME
            elif kind == "job":
                body = job_example(ref[1]); used_jobs.add(ref[1])
            elif kind == "explore":
                t, body = next((t, b) for t, b in explore if t.startswith(ref[1])); used_explore.add(t)
            else:
                body = ref[1]
            examples.append({"group": group, "title": title, "sql": body})
    for t, b in explore:
        if t not in used_explore:
            examples.append({"group": "Other", "title": t, "sql": b})
    for path in sorted(glob.glob(os.path.join(SQL_DIR, "jobs", "*.sql"))):
        if os.path.basename(path) not in used_jobs:
            examples.append({"group": "Other", "title": os.path.basename(path), "sql": job_example(os.path.basename(path))})
    for path in ddl_files():   # all of a file at once, for loading everything in one run
        with open(path) as f:
            examples.append({"group": "Complete DDL files", "title": os.path.basename(path), "sql": f.read()})
    return examples


def load_sections(path):
    """A SQL file cut at its `-- ------ title` banners: [(title, body), ...]. Whatever
    comes before the first banner (the file's header) is dropped."""
    if not os.path.isfile(path):
        return []
    sections, title, body = [], None, []
    with open(path) as f:
        for line in f:
            m = BANNER.match(line)
            if m:
                if title:
                    sections.append((title, "".join(body).strip() + "\n"))
                title, body = m.group(1), []
            elif title:
                body.append(line)
    if title:
        sections.append((title, "".join(body).strip() + "\n"))
    return sections


def load_explore():
    return [{"group": "Queries", "title": t, "sql": b}
            for t, b in load_sections(os.path.join(SQL_DIR, "explore.sql"))]


# ── Gateway access ────────────────────────────────────────────────────────────

class GatewayError(Exception):
    pass


def gw(method, path, body=None):
    r = requests.request(method, GATEWAY + path, json=body, timeout=60)
    text = r.text
    if r.status_code >= 400:
        raise GatewayError(error_text(text))
    return json.loads(text) if text else {}


def error_text(text):
    """The gateway returns {"errors": ["Internal server error.", "<stack trace>"]}."""
    try:
        errors = json.loads(text).get("errors") or [text]
    except ValueError:
        errors = [text]
    return "\n".join(e for e in errors if e != "Internal server error.") or text


def result_path(session, op, token=0):
    return f"/v2/sessions/{session}/operations/{op}/result/{token}?rowFormat=JSON"


# A query's rows reach the client through Flink's collect sink. With exactly-once
# checkpointing (the session's 30 s interval) it holds them until a checkpoint completes,
# so a streaming SELECT shows nothing for up to 30 s, then a batch every 30 s.
# At-least-once lets them through as they come; at worst a failover repeats some rows.
# Queries only, per statement: INSERT jobs keep exactly-once.
QUERY = re.compile(r"\s*(\(\s*)*(SELECT|WITH|VALUES|TABLE)\b", re.I)
QUERY_CONFIG = {"execution.checkpointing.mode": "AT_LEAST_ONCE"}


def submit(session, sql):
    body = {"statement": sql}
    if QUERY.match(sql):
        body["executionConfig"] = QUERY_CONFIG
    return gw("POST", f"/v1/sessions/{session}/statements", body)["operationHandle"]


def execute_and_wait(session, sql, timeout=60):
    op = submit(session, sql)
    path, deadline = result_path(session, op), time.time() + timeout
    while path:
        res = gw("GET", path)
        if res.get("resultType") == "EOS":
            break
        if time.time() > deadline:
            raise GatewayError(f"timed out after {timeout}s: {sql[:80]}")
        if res.get("resultType") == "NOT_READY":
            time.sleep(0.2)
        path = res.get("nextResultUri")
    gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")


def json_error(e, status=502):
    return jsonify({"error": str(e)}), status


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/pipeline")
def pipeline_page():
    return send_from_directory(app.static_folder, "pipeline.html")


@app.get("/api/config")
def config():
    return jsonify({"preloadDdl": PRELOAD_DDL})


ACTIVE = {"CREATED", "INITIALIZING", "RUNNING", "RESTARTING", "FAILING", "CANCELLING", "RECONCILING"}


def flink(method, path, **kw):
    r = requests.request(method, FLINK + path, timeout=kw.pop("timeout", 30), **kw)
    if r.status_code >= 400:
        raise GatewayError(error_text(r.text))
    return r.json() if r.text else {}


@app.post("/api/session")
def open_session():
    load_ddl = (request.get_json(silent=True) or {}).get("ddl", PRELOAD_DDL)
    try:
        session = gw("POST", "/v1/sessions", {"sessionName": "sql-editor"})["sessionHandle"]
        if load_ddl:
            for stmt in DDL:
                execute_and_wait(session, stmt)
    except (GatewayError, requests.RequestException) as e:
        return json_error(f"could not open a session with the DDL loaded:\n{e}")
    # A pipeline already running (full ./start.sh): its source table, so it can be queried.
    restored = []
    if not load_ddl:
        try:
            restored = restore_sources(session)
        except (GatewayError, requests.RequestException):
            pass
    return jsonify({"session": session, "ddl": len(DDL) if load_ddl else 0, "sources": restored})


@app.post("/api/split")
def split():
    return jsonify({"statements": split_statements(request.get_json()["sql"])})


@app.post("/api/run/<session>")
def run(session):
    try:
        op = submit(session, request.get_json()["sql"])
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 400)
    return jsonify({"op": op})


@app.get("/api/result/<session>/<op>/<int:token>")
def result(session, op, token):
    try:
        res = gw("GET", result_path(session, op, token))
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 400)
    nxt = res.get("nextResultUri")
    return jsonify({
        "type": res.get("resultType"),
        "jobId": res.get("jobID"),
        "isQuery": res.get("isQueryResult"),
        "columns": [c["name"] for c in (res.get("results") or {}).get("columns", [])],
        "rows": (res.get("results") or {}).get("data", []),
        "next": int(nxt.split("/result/")[1].split("?")[0]) if nxt else None,
    })


# Close, not cancel: a streaming SELECT's operation is FINISHED as soon as its job is
# submitted, so /cancel refuses it ("Failed to convert the Operation Status from
# FINISHED to CANCELED"). Closing releases the result fetcher, which cancels the job.
@app.post("/api/cancel/<session>/<op>")
def cancel(session, op):
    try:
        gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({})


@app.post("/api/heartbeat/<session>")
def heartbeat(session):
    try:
        gw("POST", f"/v1/sessions/{session}/heartbeat")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e, 410)
    return jsonify({})


@app.post("/api/close/<session>")
def close(session):
    try:
        gw("DELETE", f"/v1/sessions/{session}")
    except (GatewayError, requests.RequestException):
        pass  # already gone (idle timeout) — nothing to do
    return Response(status=204)


# ── Catalog browser ───────────────────────────────────────────────────────────
#
# Runs SHOW / DESCRIBE in the browser's own session, so the tree also shows that
# session's in-memory tables, which no other session can see.

def query_rows(session, sql, timeout=60):
    """Run one statement and return every row's fields. For bounded statements only."""
    op = submit(session, sql)
    path, rows, columns, deadline = result_path(session, op), [], [], time.time() + timeout
    try:
        while path:
            res = gw("GET", path)
            if res.get("resultType") == "EOS":
                break
            if time.time() > deadline:
                raise GatewayError(f"timed out after {timeout}s: {sql}")
            results = res.get("results") or {}
            columns = [c["name"] for c in results.get("columns", [])] or columns
            rows += [r["fields"] for r in results.get("data", [])]
            if res.get("resultType") == "NOT_READY":
                time.sleep(0.2)
            path = res.get("nextResultUri")
    finally:
        gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
    return columns, rows


def ident(*parts):
    return ".".join("`" + p.replace("`", "``") + "`" for p in parts)


@app.get("/api/catalog/<session>/current")
def catalog_current(session):
    """The session's current catalog and database: what an unqualified name in a DDL
    statement resolves against, so the browser knows which tree level it changed."""
    try:
        _, c = query_rows(session, "SHOW CURRENT CATALOG")
        _, d = query_rows(session, "SHOW CURRENT DATABASE")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({"catalog": c[0][0], "database": d[0][0]})


@app.get("/api/catalog/<session>/structure")
def catalog_structure(session):
    """One table's columns (DESCRIBE) and its DDL (SHOW CREATE), for the structure
    dialog. SHOW CREATE TABLE refuses a view, so a view falls back to SHOW CREATE VIEW."""
    c, d, t = (request.args.get(k) for k in ("catalog", "database", "table"))
    if not (c and d and t):
        return json_error("catalog, database and table are required", 400)
    try:
        names, rows = query_rows(session, f"DESCRIBE {ident(c, d, t)}")
        kind, ddl = "table", None
        try:
            _, out = query_rows(session, f"SHOW CREATE TABLE {ident(c, d, t)}")
        except GatewayError:
            kind = "view"
            _, out = query_rows(session, f"SHOW CREATE VIEW {ident(c, d, t)}")
        ddl = out[0][0] if out else None
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({"kind": kind, "columns": [dict(zip(names, r)) for r in rows], "ddl": ddl})


@app.get("/api/catalog/<session>")
def catalog(session):
    """No arguments: the catalogs. ?catalog=: its databases. + &database=: its tables and
    views. + &table=: that table's columns, as DESCRIBE reports them."""
    c, d, t = (request.args.get(k) for k in ("catalog", "database", "table"))
    try:
        if not c:
            # The built-in catalog first (the session's own tables), then the rest A–Z.
            _, rows = query_rows(session, "SHOW CATALOGS")
            ordered = sorted((r[0] for r in rows), key=lambda n: (n != BUILTIN_CATALOG, n))
            return jsonify({"items": [{"name": n} for n in ordered]})
        if not d:
            _, rows = query_rows(session, f"SHOW DATABASES IN {ident(c)}")
            return jsonify({"items": [{"name": r[0]} for r in rows]})
        if not t:
            # Views included: SHOW TABLES lists both. (SHOW VIEWS has no IN clause in
            # Flink 1.20, and USE would change the session's current database.)
            _, rows = query_rows(session, f"SHOW TABLES IN {ident(c, d)}")
            return jsonify({"items": sorted(({"name": r[0]} for r in rows), key=lambda i: i["name"])})
        columns, rows = query_rows(session, f"DESCRIBE {ident(c, d, t)}")
        return jsonify({"items": [dict(zip(columns, r)) for r in rows]})
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)


# ── Pipeline page (static/pipeline.html) ─────────────────────────────────────
#
# The architecture diagram as a control surface: each box is a table (a source or a
# sink), each arrow the Flink job that writes it. The page asks /api/pipeline/status what
# exists and what runs, and runs every action's SQL in the browser's own session, like
# the editor — the two share one session.

# Box -> its table, and the arrow (job) that feeds it. `listed` is the name SHOW TABLES
# prints when it differs from the table's own (the JDBC catalog qualifies the schema).
# `probe`: confirm rows whenever the table exists, not only while its feeding job runs —
# scan_events_log is written by the scan-gen container, not by a Flink job.
PG = ("shipments_pg", "shipments")
NODES = {
    "scan_events_log":  {"table": ("fluss", "app", "scan_events_log"), "probe": True},
    "route_pg":         {"table": (BUILTIN_CATALOG, "default_database", "route_reference_cdc"), "source": True},
    "route_reference":  {"table": ("fluss", "app", "route_reference"), "feed": "routes"},
    "shipment_status":  {"table": ("fluss", "app", "shipment_status"), "feed": "status"},
    "scans_enriched":   {"table": ("fluss", "app", "scans_enriched"), "feed": "enrichment"},
    "scan_window_5m":   {"table": PG + ("scan_window_5m",), "listed": "public.scan_window_5m",
                         "feed": "scan_window", "precreated": True},
    "delivery_summary": {"table": PG + ("delivery_summary",), "listed": "public.delivery_summary",
                         "feed": "delivery", "precreated": True},
    "transit_time_5m":  {"table": PG + ("transit_time_5m",), "listed": "public.transit_time_5m",
                         "feed": "transit", "precreated": True},
}
EDGES = {
    "status":           {"from": ["scan_events_log"], "to": "shipment_status", "file": "jobs/20_shipment_status.sql"},
    "enrichment":       {"from": ["scan_events_log", "route_reference"], "to": "scans_enriched", "file": "jobs/30_enrichment.sql"},
    "scan_window":      {"from": ["scans_enriched"], "to": "scan_window_5m", "file": "jobs/40_scan_window_5m.sql"},
    "delivery":         {"from": ["scans_enriched"], "to": "delivery_summary", "file": "jobs/41_delivery_summary.sql"},
    "transit":          {"from": ["scans_enriched"], "to": "transit_time_5m", "file": "jobs/42_transit_time_5m.sql"},
    "routes":           {"from": ["route_pg"], "to": "route_reference", "file": "jobs/25_routes_cdc.sql"},
}
# Pre-registered on page load: the three PostgreSQL tables already exist (pg_shipments_ddl.sql),
# so Flink only needs the shipments_pg catalog to see them.
BOOTSTRAP_FILES = ["ddl/02_postgres.sql"]

# ...and the CDC source table, but only while the job reading it runs. A full ./start.sh
# submits routes-cdc through submit.sh, whose SQL Client sessions are the only ones that
# ever had route_reference_cdc: it lives in the in-memory catalog. So a browser session
# gets it too — the same IF NOT EXISTS DDL — once routes-cdc is running.
SOURCES = {   # box -> (its DDL section, the arrow whose job reads it)
    "route_pg": (("ddl/03_sources.sql", "Step 8:"), "routes"),
}


def read_sql(rel):
    with open(os.path.join(SQL_DIR, rel)) as f:
        return f.read()


def section(rel, prefix):
    for title, body in load_sections(os.path.join(SQL_DIR, rel)):
        if title.startswith(prefix):
            return f"-- {title}\n{body}"
    raise KeyError(f"no '{prefix}' section in {rel}")


def without_header(sql):
    """A job file minus its leading `-- ===` comment block: the pop-up says what it is."""
    lines = sql.splitlines()
    i = 0
    while i < len(lines) and (lines[i].startswith("--") or not lines[i].strip()):
        i += 1
    return "\n".join(lines[i:]).strip() + "\n"


@app.get("/api/pipeline/sql")
def pipeline_sql():
    """Every action's default SQL, cut from flink_sql/ so the files stay the one source."""
    catalog = section("ddl/01_fluss.sql", "Step 1:")
    database = "CREATE DATABASE IF NOT EXISTS fluss.app;\n"
    view = lambda t, note: f"-- {note}\nSELECT * FROM {t};\n"
    explore = dict(load_sections(os.path.join(SQL_DIR, "explore.sql")))
    ex = lambda prefix: next(b for t, b in explore.items() if t.startswith(prefix))
    pg_view = lambda t, order, note: (f"-- {note}\n"
                                      "SET 'execution.runtime-mode' = 'batch';\n"
                                      f"SELECT * FROM shipments_pg.shipments.{t} ORDER BY {order};\n"
                                      "SET 'execution.runtime-mode' = 'streaming';\n")
    return jsonify({
        "nodes": {
            "scan_events_log": {"create": catalog + "\n" + section("ddl/01_fluss.sql", "Step 2:"),
                                "view": "-- Lab 1, Step 3: every scan, as scan-gen appends it. A streaming read: it keeps going until you cancel or close.\n" + ex("Lab 1, Step 3:")},
            "shipment_status": {"create": catalog + "\n" + database + "\n" + section("ddl/01_fluss.sql", "Step 4:"),
                                "view": view("fluss.app.shipment_status", "One row per shipment: a streaming read of a PK table is a changelog, so rows are updated in place as shipments move on.")},
            "route_reference": {"create": catalog + "\n" + database + "\n" + section("ddl/01_fluss.sql", "Step 8:"),
                                "view": "-- Lab 3, Step 9: one row per carrier, replicated from PostgreSQL by routes-cdc.\n" + ex("Lab 3, Step 9: the reference")},
            "route_pg": {"create": section("ddl/03_sources.sql", "Step 8:"),
                         "view": "-- Lab 3, Step 9: route_reference read through CDC: the snapshot, then a row for every change. A streaming read: it keeps going until you cancel or close.\n" + ex("Lab 3, Step 9: the CDC"),
                         "pg": "-- Lab 3, Step 9\n" + ex("Lab 3, Step 9: the routes"),
                         "update": "-- Lab 5, Step 20\n" + ex("Lab 5, Step 20:"),
                         "add": "-- Lab 5, Step 21\n" + ex("Lab 5, Step 21:")},
            "scans_enriched": {"create": catalog + "\n" + database + "\n" + section("ddl/01_fluss.sql", "Step 10:"),
                               "view": view("fluss.app.scans_enriched", "Lab 3, Step 11: every scan, with carrier_name, hub_city and avg_delivery_days from route_reference.")},
            "lookup": {"view": "-- Lab 2, Steps 6-7: a point lookup on the PK table, then Log vs PK row counts.\n"
                               "-- The placeholder shipment_id is replaced with a live one once shipment_status has rows.\n"
                               + ex("Lab 2, Step 6: point") + "\n" + ex("Lab 2, Step 7:")},
            "scan_window_5m": {"view": pg_view("scan_window_5m", "window_start DESC, scan_count DESC",
                                               "scan_window_5m in PostgreSQL, through the shipments_pg catalog: a bounded read, newest window first.")},
            "delivery_summary": {"view": pg_view("delivery_summary", "window_start DESC, carrier_code, status",
                                                 "delivery_summary in PostgreSQL: shipments per status and carrier, newest window first.")},
            "transit_time_5m": {"view": pg_view("transit_time_5m", "window_start DESC, carrier_code",
                                                "transit_time_5m in PostgreSQL: deliveries per carrier with its SLA, newest window first.")},
        },
        "edges": {e: {"sql": without_header(read_sql(spec["file"])) + RESET_NAME.lstrip("\n")} for e, spec in EDGES.items()},
    })


@app.post("/api/pipeline/bootstrap/<session>")
def pipeline_bootstrap(session):
    """Register what the pre-created boxes need (the shipments_pg catalog), and the source
    a running pipeline already reads. IF NOT EXISTS throughout."""
    try:
        for rel in BOOTSTRAP_FILES:
            for stmt in split_statements(read_sql(rel)):
                execute_and_wait(session, stmt)
        restored = restore_sources(session)
    except (GatewayError, requests.RequestException, OSError) as e:
        return json_error(e)
    return jsonify({"ok": True, "sources": restored})


# Which table a job writes, from its plan. Matching jobs to arrows by what they write, not
# by name, keeps an arrow lit when an attendee edits pipeline.name in the pop-up. Fluss
# sinks print "Sink(app.scans_enriched)", JDBC ones "Sink(table=[shipments_pg.shipments.scan_window_5m]".
SINK_FULL = re.compile(r"Sink\(table=\[([^\]]+)\]")
SINK_SHORT = re.compile(r"Sink\(([\w$.`]+)\)")
plan_sinks = {}


def job_sinks(jid):
    if jid not in plan_sinks:
        nodes = flink("GET", f"/jobs/{jid}/plan").get("plan", {}).get("nodes", [])
        text = " ".join(n.get("description", "") for n in nodes)
        plan_sinks[jid] = [s.replace("`", "") for s in SINK_FULL.findall(text) + SINK_SHORT.findall(text)]
    return plan_sinks[jid]


def writes(sinks, table):
    full = ".".join(table)
    return any(s == full or full.endswith("." + s) for s in sinks)


def active_jobs():
    """Flink jobs that are up, each with the tables it writes (from its plan)."""
    active = [j for j in flink("GET", "/jobs/overview").get("jobs", [])
              if j["state"] in ACTIVE and j["state"] != "CANCELLING"]
    for j in active:
        j["sinks"] = job_sinks(j["jid"])
    return active


def edge_jobs(active, edge):
    return [j for j in active if writes(j["sinks"], NODES[EDGES[edge]["to"]]["table"])]


def restore_sources(session):
    """Create in this session the source tables whose reading job runs (see SOURCES).
    Returns the boxes it created. Qualified, so a USE in the session does not misplace them."""
    running = {e for e in EDGES if any(j["state"] == "RUNNING" for j in edge_jobs(active_jobs(), e))}
    listed = names(session, f"SHOW TABLES IN {ident(BUILTIN_CATALOG, 'default_database')}")
    restored = []
    for node, ((rel, prefix), edge) in SOURCES.items():
        if edge in running and NODES[node]["table"][2] not in listed:
            for stmt in split_statements(section(rel, prefix)):
                execute_and_wait(session, stmt.replace(
                    "CREATE TABLE IF NOT EXISTS ", f"CREATE TABLE IF NOT EXISTS {ident(BUILTIN_CATALOG, 'default_database')}.", 1))
            restored.append(node)
    return restored


def names(session, sql):
    try:
        return {r[0] for r in query_rows(session, sql)[1]}
    except GatewayError:
        return set()   # e.g. the catalog or database does not exist yet


# ── The data probe: does a table return a row? ──
# The one status check that costs a Flink job, so it runs in the background, one at a
# time, in a session of its own, and only for a box whose feeding job runs and whose data
# is not confirmed yet. Confirmed boxes are not probed again.
probe_lock = threading.Lock()
probe_wake = threading.Event()
probe_ok, probe_next, probe_wanted = set(), {}, {}
PROBE_TIMEOUT, PROBE_RETRY = 90, 30


def want_probe(node):
    with probe_lock:
        if node in probe_ok or node in probe_wanted or time.time() < probe_next.get(node, 0):
            return
        probe_wanted[node] = f"SELECT 1 FROM {ident(*NODES[node]['table'])} LIMIT 1"
    probe_wake.set()


def first_row(session, sql):
    op = submit(session, sql)
    path, deadline = result_path(session, op), time.time() + PROBE_TIMEOUT
    try:
        while path and time.time() < deadline:
            res = gw("GET", path)
            if (res.get("results") or {}).get("data"):
                return True
            if res.get("resultType") == "EOS":
                return False
            time.sleep(0.5)
            path = res.get("nextResultUri")
        return False
    finally:
        try:
            gw("DELETE", f"/v1/sessions/{session}/operations/{op}/close")
        except (GatewayError, requests.RequestException):
            pass


def probe_worker():
    session = None
    while True:
        probe_wake.wait()
        probe_wake.clear()
        while True:
            with probe_lock:
                if not probe_wanted:
                    break
                node, sql = next(iter(probe_wanted.items()))
            ok = False
            try:
                if session is None:
                    session = gw("POST", "/v1/sessions", {"sessionName": "pipeline-probe"})["sessionHandle"]
                ok = first_row(session, sql)
            except (GatewayError, requests.RequestException):
                session = None   # gone or broken: a fresh one next time
            with probe_lock:
                probe_wanted.pop(node, None)
                if ok:
                    probe_ok.add(node)
                else:
                    probe_next[node] = time.time() + PROBE_RETRY


threading.Thread(target=probe_worker, daemon=True, name="pipeline-probe").start()

# A live shipment_id for the point lookup's placeholder, read once from shipment_status.
sample_shipment = None


def sample_id(session):
    global sample_shipment
    if sample_shipment is None:
        try:
            _, rows = query_rows(session, "SELECT shipment_id FROM fluss.app.shipment_status LIMIT 1", timeout=30)
            sample_shipment = rows[0][0] if rows else None
        except (GatewayError, requests.RequestException, IndexError):
            pass
    return sample_shipment


@app.get("/api/pipeline/status/<session>")
def pipeline_status(session):
    try:
        catalogs = names(session, "SHOW CATALOGS")
        listed = {
            BUILTIN_CATALOG: names(session, f"SHOW TABLES IN {ident(BUILTIN_CATALOG, 'default_database')}"),
            "fluss": names(session, "SHOW TABLES IN `fluss`.`app`") if "fluss" in catalogs else set(),
            "shipments_pg": names(session, "SHOW TABLES IN `shipments_pg`.`shipments`") if "shipments_pg" in catalogs else set(),
        }
    except requests.RequestException as e:
        return json_error(e)
    exists = {n: (spec.get("listed") or spec["table"][2]) in listed.get(spec["table"][0], set())
              for n, spec in NODES.items()}

    # Jobs, by the table they write.
    try:
        active = active_jobs()
    except (GatewayError, requests.RequestException):
        active = []
    edges = {}
    for e, spec in EDGES.items():
        jobs = [{"jid": j["jid"], "name": j["name"], "state": j["state"]} for j in edge_jobs(active, e)]
        states = {j["state"] for j in jobs}
        edges[e] = {"state": "running" if "RUNNING" in states else
                             "failing" if states & {"RESTARTING", "FAILING"} else
                             "starting" if jobs else "idle",
                    "jobs": jobs, "ready": all(exists[n] for n in spec["from"] + [spec["to"]])}

    nodes = {}
    for n, spec in NODES.items():
        feed = edges.get(spec.get("feed"), {}).get("state")
        if not exists[n]:
            with probe_lock:
                probe_ok.discard(n)   # dropped: whatever was confirmed is gone with it
            state = "inactive"
        elif feed == "running" or spec.get("probe"):
            want_probe(n)
            state = "live" if n in probe_ok else "starting" if feed == "running" or n == "scan_events_log" else "ready"
        elif spec.get("source"):
            state = "live"           # PostgreSQL's route_reference: always there to read
        else:
            state = "ready"
        nodes[n] = {"state": state, "exists": exists[n]}

    # scan-gen: not a Flink job, the Fluss Python client appending to scan_events_log.
    edges["gen"] = {"state": "running" if nodes["scan_events_log"]["state"] == "live" else "idle", "jobs": []}
    nodes["scan_gen"] = {"state": "live"}
    # The point lookup, a consumer of shipment_status.
    nodes["lookup"] = {"exists": exists["shipment_status"],
                       "state": "inactive" if not exists["shipment_status"] else
                                "live" if nodes["shipment_status"]["state"] == "live" else "ready",
                       "sample": sample_id(session) if nodes["shipment_status"]["state"] == "live" else None}
    pg_live = [nodes[n]["state"] == "live" for n in ("scan_window_5m", "delivery_summary", "transit_time_5m")]
    nodes["grafana"] = {"state": "live" if any(pg_live) else "ready"}
    missing = [n for n, spec in NODES.items() if spec.get("precreated") and not exists[n]]
    missing += [n for n, (_, e) in SOURCES.items() if not exists[n] and edges[e]["state"] == "running"]
    return jsonify({"nodes": nodes, "edges": edges, "bootstrap": bool(missing)})


# The data probe's jobs (want_probe): SELECT 1 FROM <table> LIMIT 1, as Flink names them.
PROBE_JOB = re.compile(r"\s*SELECT 1\s+FROM\b.*FETCH NEXT 1 ROWS ONLY\s*$", re.S | re.I)


@app.get("/api/jobs")
def jobs_list():
    """Every Flink job, newest first, for the catalog panel's Job Catalog: what kind of job
    it is (pipeline job or interactive query) and, for an active pipeline
    job, the table it writes — read from its plan, as the pipeline status does."""
    try:
        jobs = flink("GET", "/jobs/overview").get("jobs", [])
    except (GatewayError, requests.RequestException) as e:
        return json_error(f"could not reach the Flink JobManager: {e}")
    out = []
    for j in sorted(jobs, key=lambda j: j.get("start-time", 0), reverse=True):
        name, state = j["name"], j["state"]
        if PROBE_JOB.match(name):
            continue   # the pipeline page's own "does this table return a row?" checks
        kind = "query" if name.lstrip().upper().startswith("SELECT") else "job"
        sinks = []
        if kind == "job" and state in ACTIVE:
            try:   # Fluss plans name the sink db.table only: show it with its catalog
                sinks = [x if x.count(".") >= 2 else "fluss." + x for x in job_sinks(j["jid"])]
            except (GatewayError, requests.RequestException):
                pass
        out.append({"jid": j["jid"], "name": " ".join(name.split()), "state": state, "kind": kind,
                    "sinks": sinks, "start": j.get("start-time"), "duration": j.get("duration"),
                    "active": state in ACTIVE})
    return jsonify({"jobs": out})


@app.post("/api/jobs/<jid>/cancel")
def job_cancel(jid):
    try:
        flink("PATCH", f"/jobs/{jid}?mode=cancel")
    except (GatewayError, requests.RequestException) as e:
        return json_error(e)
    return jsonify({})


@app.get("/api/examples")
def examples():
    return jsonify(load_examples())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8084, threaded=True)
