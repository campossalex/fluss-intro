# Fluss Intro — Real-time Shipment Tracking

An introduction to **Apache Fluss** on open source only — **Apache Flink 1.20** and **Apache
Fluss 1.0**, with PostgreSQL and Grafana — all in one Docker Compose stack. The open-source
edition of the Ververica Platform `fluss_intro` lab, with the same browser tooling as
[Streamhouse](https://github.com/campossalex/streaming_lakehouse): a lab homepage, a Flink SQL
editor and a click-through Pipeline Builder.

```
scan-gen (pyfluss) ─► fluss.app.scan_events_log ─► shipment-status ─► fluss.app.shipment_status (PK)
                                 │
                                 └─► scan-enrichment ◄── fluss.app.route_reference (PK, lookup join)
                                           │
                                fluss.app.scans_enriched
                                           │  3 jobs, TUMBLE 5 min
                                           ▼
                            PostgreSQL `shipments` ─► Grafana
```

This page is how to deploy it. Once it is up:

- [WORKSHOP.md](WORKSHOP.md) — the attendee walkthrough, Lab 1 to Lab 5.
- [docs/REFERENCE.md](docs/REFERENCE.md) — how each piece is wired, running steps from a
  terminal, troubleshooting.

---

## Requirements

| | Minimum | Notes |
|---|---|---|
| **Memory for Docker** | **6 GB** | About 4–5 GB in use with every lab job running. On Docker Desktop: *Settings → Resources*. |
| **CPU** | 4 cores | |
| **Disk** | 10 GB free | Images (Flink, Fluss, PostgreSQL, Grafana, Python) and data. |
| **Docker** | Docker Engine with **Compose v2 ≥ 2.20** | `docker compose version` to check. |
| **Tools** | `bash`, `curl`, `jq` | `start.sh` uses `curl` and `jq`. |
| **Internet access** | On the first run | Images from Docker Hub, Python packages from PyPI (`pyfluss`). The connector JARs are committed in `lib/`. |
| **Free ports** | 80, 3000, 5432, 8081, 8083, 8084, 9123 | Stop any other Docker Compose stack using them first. |

Images exist for both x86_64 and arm64 (Apple Silicon, AWS Graviton).

---

## Run locally

```bash
git clone https://github.com/campossalex/fluss-intro.git
cd fluss-intro

./start.sh --services-only
```

Then open **http://localhost** — the lab homepage, with a link to every tool.

`start.sh` starts the stack, waits until every service is healthy, then prints the URLs and a
runbook of manual commands. The modes:

| Command | Result | Use it for |
|---|---|---|
| `./start.sh --services-only` | Everything running, nothing created; the generator waits for its table | **A workshop** — attendees start from Lab 1 |
| `./start.sh` | Also creates every table and submits every lab job | **A demo** — the finished state of all labs |
| add `--reset` to either | Wipes every volume first: Fluss, PostgreSQL, the Flink catalog store | **Handing a clean environment** to the next group |

To stop:

```bash
docker compose down -v      # stop everything and delete its data
```

---

## Run on a remote host (e.g. AWS EC2)

The same stack runs on a single VM; attendees use their browser, nothing is installed on
their machines. An `m6i.large` (2 vCPU, 8 GB) is the minimum, `m6i.xlarge` (4 vCPU, 16 GB)
recommended. Install Docker with the Compose plugin, clone the repository and run
`./start.sh --services-only`. The homepage and the editors build every link from the address
they were opened with, so share `http://<public-ip-or-dns>`.

**Nothing has authentication**: Grafana runs as anonymous admin, the Flink Web UI accepts JAR
uploads, the SQL Gateway executes any SQL. Open ports 80, 8084, 8081 and 3000 only to the
attendees' IP range; never open 5432, 8083 or 9123.

---

## Services and endpoints

| Service | URL | Credentials | Used in |
|---|---|---|---|
| **Lab homepage** | http://localhost | — | Start here |
| **Flink SQL editor** | http://localhost:8084 | — | Labs 1–5 |
| **Pipeline Builder** | http://localhost:8084/pipeline | — | Labs 1–5 |
| Flink Web UI | http://localhost:8081 | — | Labs 2–4 |
| Grafana — *Shipment Tracking* | http://localhost:3000 | anonymous admin | Lab 5 |

| Endpoint | From the host | Inside the Docker network (what the lab SQL uses) | Credentials |
|---|---|---|---|
| Fluss coordinator | `localhost:9123` | `coordinator-server:9123` | — |
| PostgreSQL `shipments` | `localhost:5432` | `postgres:5432` | `root` / `admin1` |
| Flink SQL Gateway (REST) | `http://localhost:8083` | `http://sql-gateway:8083` | — |

---

## Differences from the Ververica Platform version

| | Ververica Platform (`scenarios/fluss_intro`) | This repository |
|---|---|---|
| Where SQL runs | VVP SQL editor, deployments | Flink SQL editor / Pipeline Builder over the Flink SQL Gateway; an `INSERT` is the deployment |
| Fluss | On Kubernetes, `coordinator-server-0…:9124` | Docker, `coordinator-server:9123` |
| Table type | `'table.type' = 'log' / 'primary-key'` | Inferred: a PRIMARY KEY makes a PK table (OSS Fluss rejects `table.type`) |
| PostgreSQL | `host.minikube.internal:5432` | `postgres:5432` |
| Generator | `screen` session on the VM | the `scan-gen` container |
| Grafana | datasource and dashboard added by hand (Steps 17–18) | both provisioned |
