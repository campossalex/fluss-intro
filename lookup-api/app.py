"""Shipment lookup API: point lookups on the Fluss PK table fluss.app.shipment_status.

The request-response serving pattern: an application asks "where is shipment X right now?"
and gets the answer straight from Fluss by primary key — no Flink job, no cache, no copy
in another database. The same table the shipment-status job keeps up to date (Lab 2) is
the online store.

  GET /shipments/<shipment_id>   the shipment's current state, or 404
  GET /shipments/sample?n=6      a few live shipment IDs (for the UI's quick picks)
  GET /health                    {"status", "fluss_ready", "table_ready"}
  POST /bench                    start a latency test: {shipment_id?, duration_s=60, concurrency=1}
  GET /bench                     its progress and p50 / p90 / p99, per-second timeline, histogram
  POST /bench/stop               end it early
  GET /                          a one-page UI

  curl http://localhost:9051/shipments/SHP-12345

Modelled on the inference service of github.com/campossalex/realtime-feature-store, with
one difference: the Fluss connection and the table handle are (re)opened on demand, so the
service starts before Fluss is ready or before Lab 2 creates the table, and recovers when
the table is dropped and re-created by a reset.
"""
import asyncio
import inspect
import itertools
import os
import threading
import time
from datetime import datetime, timezone

import fluss
from flask import Flask, jsonify, request, send_from_directory

FLUSS_BOOTSTRAP = os.environ.get("FLUSS_BOOTSTRAP_SERVERS", "coordinator-server:9123")
TABLE = fluss.TablePath("app", "shipment_status")
TABLE_NAME = "fluss.app.shipment_status"
TIMEOUT_S = 5.0
SAMPLE_SCAN = 1000   # rows read per bucket to choose the UI's quick picks
# A pyfluss Lookuper serves one lookup at a time (each takes ~100 ms), so concurrent
# requests sharing one would queue behind each other. Separate Lookupers run in parallel:
# the API round-robins over a small pool, and each latency-test worker gets its own.
LOOKUPER_POOL = int(os.environ.get("LOOKUPER_POOL", "8"))

app = Flask(__name__, static_folder="static")

# ── Fluss access ──────────────────────────────────────────────────────────────
# pyfluss is async; Flask is synchronous. One asyncio loop runs in a daemon thread and
# every Fluss call is submitted to it with run_coroutine_threadsafe(). The connection,
# the table and its Lookuper are created once and reused across requests.

_loop = asyncio.new_event_loop()
threading.Thread(target=_loop.run_forever, daemon=True, name="fluss-loop").start()

_lock = threading.Lock()
_conn = None       # fluss.FlussConnection
_table = None      # fluss.FlussTable for shipment_status
_lookupers = []   # a pool of fluss.Lookuper on it
_next = itertools.count()


class TableMissing(Exception):
    pass


def _run(fn):
    """Call fn() ON the Fluss loop thread and wait for its result. pyfluss creates its
    futures on the calling thread's running loop, so a call made from a Flask thread fails
    with "no running event loop": the call itself, not just the await, belongs on _loop."""
    async def call():
        res = fn()
        return await res if inspect.isawaitable(res) else res
    return asyncio.run_coroutine_threadsafe(call(), _loop).result(timeout=TIMEOUT_S)


def _reset(table_only=False):
    global _conn, _table, _lookupers
    _table, _lookupers = None, []
    if not table_only:
        _conn = None


def _ensure():
    """Open the connection and the table if needed. Raises TableMissing before Lab 2."""
    global _conn, _table, _lookupers
    with _lock:
        if _conn is None:
            _conn = _run(lambda: fluss.FlussConnection.create(fluss.Config({"bootstrap.servers": FLUSS_BOOTSTRAP})))
            print(f"[lookup-api] Fluss connection established → {FLUSS_BOOTSTRAP}", flush=True)
        if _table is None:
            try:
                _table = _run(lambda: _conn.get_table(TABLE))
            except fluss.FlussError as e:
                if "Failed to get tables" in str(e) or "does not exist" in str(e):
                    raise TableMissing() from e
                raise
            _lookupers = [_run(lambda: _table.new_lookup().create_lookuper()) for _ in range(LOOKUPER_POOL)]
            print(f"[lookup-api] {TABLE_NAME} opened", flush=True)


def _with_retry(fn):
    """Run fn() against the cached handles; on an error, re-open them once and retry
    (a dropped and re-created table keeps its name but not its id)."""
    try:
        _ensure()
        return fn()
    except TableMissing:
        raise
    except Exception:
        _reset()
        _ensure()
        return fn()


def lookup(shipment_id):
    def one():
        lk = _lookupers[next(_next) % len(_lookupers)]
        return _run(lambda: lk.lookup({"shipment_id": shipment_id}))
    return _with_retry(one)


def sample(n):
    """The n most recently scanned shipments still on their way, from a bounded scan of
    every bucket (SAMPLE_SCAN rows each) — so the UI's quick picks are shipments that move."""
    def scan():
        info = _table.get_table_info()
        rows = []
        for b in range(info.num_buckets):
            bucket = fluss.TableBucket(info.table_id, b)
            batch = _run(lambda: _table.new_scan().limit(SAMPLE_SCAN).create_bucket_batch_scanner(bucket).to_arrow())
            rows += batch.to_pylist()
        moving = [r for r in rows if r.get("status") != "DELIVERED"] or rows
        moving.sort(key=lambda r: r.get("last_scan") or datetime.min, reverse=True)
        return [r["shipment_id"] for r in moving[:n]]
    return _with_retry(scan)


def serialize(row):
    out = {k: v for k, v in row.items() if k != "shipment_id"}
    ts = row.get("last_scan")
    if isinstance(ts, datetime):
        # A naive UTC timestamp: scan-gen runs with TZ=UTC.
        out["last_scan"] = ts.isoformat(timespec="milliseconds")
        out["age_s"] = round((datetime.now(timezone.utc) - ts.replace(tzinfo=timezone.utc)).total_seconds(), 1)
    return out


MISSING = {"error": f"{TABLE_NAME} does not exist yet: create it in Lab 2, Step 4 "
                    "and deploy shipment-status (Step 5)."}


# ── Latency test ──────────────────────────────────────────────────────────────
# Repeated point lookups of one shipment for a fixed time, measured INSIDE the service
# around `await lookuper.lookup(...)`: the Fluss client's lookup latency, without the
# browser or HTTP in the number. `concurrency` workers keep that many lookups in flight,
# each a coroutine on the Fluss loop with its own Lookuper. One test at a time; the last
# result stays readable.

BENCH_MAX_S, BENCH_MAX_CONCURRENCY, HIST_BUCKETS = 600, 64, 20
_bench_lock = threading.Lock()
_bench = None   # the current or last test (see bench_start)


def _pct(sorted_ms, p):
    """Nearest-rank percentile of an ascending list."""
    if not sorted_ms:
        return None
    k = max(0, min(len(sorted_ms) - 1, -(-p * len(sorted_ms) // 100) - 1))
    return round(sorted_ms[int(k)], 2)


async def _bench_run(b, table):
    t0, key = b["t0"], {"shipment_id": b["shipment_id"]}
    deadline = t0 + b["duration_s"]

    async def worker():
        lookuper = table.new_lookup().create_lookuper()   # its own: see LOOKUPER_POOL
        while not b["stop"] and time.perf_counter() < deadline:
            t = time.perf_counter()
            try:
                row = await lookuper.lookup(key)
            except Exception as e:
                b["errors"] += 1
                b["last_error"] = str(e)[:300]
                await asyncio.sleep(0.05)
                continue
            ms = (time.perf_counter() - t) * 1000
            b["samples"].append(ms)
            b["seconds"].setdefault(int(t - t0), []).append(ms)
            if row is None:
                b["not_found"] += 1

    try:
        await asyncio.gather(*(worker() for _ in range(b["concurrency"])))
        b["state"] = "stopped" if b["stop"] else "done"
    except Exception as e:
        b["state"], b["last_error"] = "failed", str(e)[:300]
    finally:
        b["t_end"] = time.perf_counter()


def bench_start(shipment_id, duration_s, concurrency):
    global _bench
    with _bench_lock:
        if _bench and _bench["state"] == "running":
            return None
        _ensure()   # raises TableMissing before Lab 2
        _bench = {
            "id": f"{int(time.time())}", "shipment_id": shipment_id, "duration_s": duration_s,
            "concurrency": concurrency, "state": "running", "stop": False,
            "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "t0": time.perf_counter(), "t_end": None, "samples": [], "seconds": {},
            "errors": 0, "not_found": 0, "last_error": None,
        }
        asyncio.run_coroutine_threadsafe(_bench_run(_bench, _table), _loop)
        return _bench


def bench_report(b):
    samples = sorted(b["samples"])       # a copy: the workers keep appending
    now = b["t_end"] or time.perf_counter()
    elapsed = min(now - b["t0"], b["duration_s"])
    n = len(samples)
    hist = []
    if n:
        lo, hi = samples[0], samples[-1]
        width = (hi - lo) / HIST_BUCKETS or 1
        counts = [0] * HIST_BUCKETS
        for ms in samples:
            counts[min(HIST_BUCKETS - 1, int((ms - lo) / width))] += 1
        hist = [{"from_ms": round(lo + i * width, 2), "to_ms": round(lo + (i + 1) * width, 2), "count": c}
                for i, c in enumerate(counts)]
    timeline = []
    for sec in sorted(b["seconds"]):
        s = sorted(b["seconds"][sec])
        timeline.append({"t": sec, "count": len(s), "p50": _pct(s, 50), "p99": _pct(s, 99)})
    return {
        "id": b["id"], "state": b["state"], "shipment_id": b["shipment_id"],
        "duration_s": b["duration_s"], "concurrency": b["concurrency"], "started_at": b["started_at"],
        "elapsed_s": round(elapsed, 1), "progress": round(min(1.0, elapsed / b["duration_s"]), 3),
        "count": n, "throughput_per_s": round(n / elapsed, 1) if elapsed > 0 else 0,
        "errors": b["errors"], "not_found": b["not_found"], "last_error": b["last_error"],
        "latency_ms": {
            "min": round(samples[0], 2) if n else None,
            "mean": round(sum(samples) / n, 2) if n else None,
            "p50": _pct(samples, 50), "p90": _pct(samples, 90), "p99": _pct(samples, 99),
            "max": round(samples[-1], 2) if n else None,
        },
        "timeline": timeline, "histogram": hist,
    }

# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/shipments/sample")
def shipments_sample():
    n = max(1, min(int(request.args.get("n", 6)), 50))
    try:
        return jsonify({"shipment_ids": sample(n)})
    except TableMissing:
        return jsonify({**MISSING, "shipment_ids": []}), 503
    except Exception as e:
        return jsonify({"error": f"Fluss unavailable: {e}", "shipment_ids": []}), 503


@app.get("/shipments/<shipment_id>")
def shipment(shipment_id):
    t0 = time.monotonic()
    try:
        row = lookup(shipment_id)
    except TableMissing:
        return jsonify(MISSING), 503
    except Exception as e:
        return jsonify({"error": f"Fluss unavailable: {e}"}), 503
    body = {
        "shipment_id": shipment_id,
        "found": row is not None,
        "shipment": serialize(row) if row else None,
        "source": TABLE_NAME,
        "response_time_ms": round((time.monotonic() - t0) * 1000, 2),
    }
    return jsonify(body), 200 if row else 404


@app.post("/bench")
def bench_post():
    body = request.get_json(silent=True) or {}
    try:
        duration = int(body.get("duration_s", 60))
        concurrency = int(body.get("concurrency", 1))
    except (TypeError, ValueError):
        return jsonify({"error": "duration_s and concurrency must be integers"}), 400
    if not 5 <= duration <= BENCH_MAX_S:
        return jsonify({"error": f"duration_s must be between 5 and {BENCH_MAX_S}"}), 400
    if not 1 <= concurrency <= BENCH_MAX_CONCURRENCY:
        return jsonify({"error": f"concurrency must be between 1 and {BENCH_MAX_CONCURRENCY}"}), 400
    try:
        shipment_id = (body.get("shipment_id") or "").strip() or next(iter(sample(1)), None)
        if not shipment_id:
            return jsonify({"error": "shipment_status has no rows yet: deploy shipment-status (Lab 2, Step 5)."}), 503
        b = bench_start(shipment_id, duration, concurrency)
    except TableMissing:
        return jsonify(MISSING), 503
    except Exception as e:
        return jsonify({"error": f"Fluss unavailable: {e}"}), 503
    if b is None:
        return jsonify({"error": "a latency test is already running", **bench_report(_bench)}), 409
    return jsonify(bench_report(b)), 202


@app.get("/bench")
def bench_get():
    if _bench is None:
        return jsonify({"state": "none"})
    return jsonify(bench_report(_bench))


@app.post("/bench/stop")
def bench_stop():
    if _bench is None:
        return jsonify({"state": "none"})
    _bench["stop"] = True
    return jsonify(bench_report(_bench))


@app.get("/health")
def health():
    table_ready = False
    try:
        _ensure()
        table_ready = True
    except TableMissing:
        pass
    except Exception:
        _reset()
    return jsonify({"status": "ok", "fluss_ready": _conn is not None, "table_ready": table_ready})


@app.get("/")
def ui():
    return send_from_directory(app.static_folder, "index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9051, threaded=True)
