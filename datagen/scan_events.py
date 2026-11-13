# Purpose: Generates synthetic shipment scan events directly to Fluss for the fluss_intro lab.
#
# Simulates a fleet of couriers scanning packages at each stage of delivery:
#   PICKED_UP → IN_TRANSIT (1-3 hops) → OUT_FOR_DELIVERY → DELIVERED
#
# Events are written directly to the fluss.app.scan_events_log Log table.
# The script retries until the table is available — create it in Lab 1, Step 2.
#
# The carriers come from PostgreSQL's route_reference, re-read every 30 s: a route added
# there (Lab 5, Step 21) starts getting new shipments. Shipments already in flight keep
# their carrier. If PostgreSQL cannot be read, the last known list stays in use.

import asyncio
import configparser
import os
import random
import time
import uuid
from datetime import datetime

import fluss
import psycopg

# ── Configuration ─────────────────────────────────────────────────────────────
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "configuration/configuration.ini"))

bootstrap_servers = os.environ.get("FLUSS_BOOTSTRAP_SERVERS", config["FLUSS"]["bootstrap_servers"])
table_database    = config["FLUSS"]["table_database"]
table_name        = config["FLUSS"]["table_name"]

min_freq_ms       = int(config["GENERATOR"]["min_freq_ms"])
max_freq_ms       = int(config["GENERATOR"]["max_freq_ms"])
num_shipments     = int(config["GENERATOR"]["num_shipments"])
max_transit_hops  = int(config["GENERATOR"]["max_transit_hops"])

PG_DSN = os.environ.get("PG_DSN", "postgresql://root:admin1@postgres:5432/shipments")
CARRIER_REFRESH_S = 30

# ── Constants ─────────────────────────────────────────────────────────────────
# Until the first read from PostgreSQL succeeds: the five carriers it is seeded with.
CARRIERS = ["DHL", "UPS", "FDX", "GLS", "TNT"]

LOCATIONS = [
    "Frankfurt", "Berlin", "Munich", "Hamburg", "Cologne",
    "Vienna",    "Amsterdam", "Brussels", "Paris",  "Zurich",
    "Milan",     "Madrid",   "Barcelona", "Warsaw", "Prague",
    "Budapest",  "Lisbon",   "Stockholm", "Oslo",   "Copenhagen",
]

STATUS_FLOW = ["PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED"]


# ── Helpers ───────────────────────────────────────────────────────────────────
def new_shipment_id() -> str:
    return "SHP-" + str(random.randint(10000, 99999))


def new_scan_id() -> str:
    return "SCN-" + uuid.uuid4().hex[:8].upper()


def make_scan(shipment_id: str, carrier_code: str, status: str, location: str) -> dict:
    return {
        "scan_id":      new_scan_id(),
        "shipment_id":  shipment_id,
        "carrier_code": carrier_code,
        "status":       status,
        "location":     location,
        "scan_time":    datetime.now(),
    }


# ── Carriers from PostgreSQL ──────────────────────────────────────────────────
_carriers_read_at = 0.0


def refresh_carriers() -> None:
    """Re-read the carrier codes from route_reference, at most every CARRIER_REFRESH_S."""
    global CARRIERS, _carriers_read_at
    if time.monotonic() - _carriers_read_at < CARRIER_REFRESH_S:
        return
    _carriers_read_at = time.monotonic()
    try:
        with psycopg.connect(PG_DSN, connect_timeout=5) as pg:
            codes = [r[0] for r in pg.execute("SELECT carrier_code FROM route_reference ORDER BY carrier_code")]
    except Exception as e:
        print(f"[CARRIERS] could not read route_reference ({e}); keeping {CARRIERS}", flush=True)
        return
    if codes and set(codes) != set(CARRIERS):
        print(f"[CARRIERS] {sorted(CARRIERS)} -> {codes}", flush=True)
    if codes:
        CARRIERS = codes


# ── Fluss helpers ─────────────────────────────────────────────────────────────
async def get_writer(conn, table_path):
    """Wait until the Fluss table is available, then return an append writer."""
    while True:
        try:
            table = await conn.get_table(table_path)
            return table.new_append().create_writer()
        except Exception:
            print("[WAIT] Fluss table not yet available, retrying in 5 s ...", flush=True)
            print("       Create the table in Lab 1, Step 2 to start the data flow.", flush=True)
            await asyncio.sleep(5)


# ── Main ──────────────────────────────────────────────────────────────────────
async def main() -> None:
    fluss_config = fluss.Config({"bootstrap.servers": bootstrap_servers})
    conn = await fluss.FlussConnection.create(fluss_config)
    table_path = fluss.TablePath(table_database, table_name)

    print(f"[scan_events] Connecting to Fluss at {bootstrap_servers} ...")
    writer = await get_writer(conn, table_path)

    def emit(scan: dict) -> None:
        writer.append(scan)
        print(
            f"[SCAN] {scan['scan_id']} | {scan['shipment_id']} "
            f"| {scan['carrier_code']} | {scan['status']:20s} | {scan['location']}"
        )

    # active_shipments: shipment_id → {carrier_code, status_idx, location, transit_hops}
    active: dict = {}

    refresh_carriers()

    # Seed an initial pool of shipments spread across lifecycle stages
    for _ in range(num_shipments // 2):
        sid      = new_shipment_id()
        carrier  = random.choice(CARRIERS)
        location = random.choice(LOCATIONS)
        idx      = random.randint(0, 1)   # start at PICKED_UP or IN_TRANSIT
        active[sid] = {
            "carrier_code": carrier,
            "status_idx":   idx,
            "location":     location,
            "transit_hops": 0,
        }
        emit(make_scan(sid, carrier, STATUS_FLOW[idx], location))

    await writer.flush()

    print(
        f"\n[scan_events] Generator started: {len(active)} initial shipments in flight\n"
        f"  Table : fluss.{table_database}.{table_name}\n"
        f"  Delay : {min_freq_ms}–{max_freq_ms} ms per event\n"
    )

    while True:
        refresh_carriers()

        # ── Maybe birth a new shipment ──────────────────────────────────────
        if len(active) < num_shipments and random.random() < 0.20:
            sid      = new_shipment_id()
            carrier  = random.choice(CARRIERS)
            location = random.choice(LOCATIONS)
            active[sid] = {
                "carrier_code": carrier,
                "status_idx":   0,
                "location":     location,
                "transit_hops": 0,
            }
            emit(make_scan(sid, carrier, "PICKED_UP", location))

        # ── Advance a random active shipment ───────────────────────────────
        if active:
            sid  = random.choice(list(active.keys()))
            ship = active[sid]
            idx  = ship["status_idx"]

            if STATUS_FLOW[idx] == "IN_TRANSIT" and ship["transit_hops"] < random.randint(1, max_transit_hops):
                ship["location"]     = random.choice(LOCATIONS)
                ship["transit_hops"] += 1
                emit(make_scan(sid, ship["carrier_code"], "IN_TRANSIT", ship["location"]))
            elif idx < len(STATUS_FLOW) - 1:
                ship["status_idx"] += 1
                next_status         = STATUS_FLOW[ship["status_idx"]]
                ship["location"]    = random.choice(LOCATIONS)
                ship["transit_hops"] = 0
                emit(make_scan(sid, ship["carrier_code"], next_status, ship["location"]))

                if next_status == "DELIVERED":
                    del active[sid]

        await writer.flush()
        await asyncio.sleep(random.randint(min_freq_ms, max_freq_ms) / 1000)


if __name__ == "__main__":
    asyncio.run(main())
