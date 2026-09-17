# Tilley Station — Real-Time Crowd Management on Confluent Cloud

A live, continuously-running demo of **event-driven architecture** built on
Confluent Cloud. It models crowd safety at a fictional London Underground
station, **Tilley**, on one line between two neighbours — **Overton** (west) and
**Jones** (east).

Passengers pour into Tilley from the street and from arriving trains. The
station runs its own **deterministic operational controls** — block signalling
(a train holds the platform, the next one queues at a red) and an occupancy-based
street gateline. On top of that operational event stream, **Confluent Cloud
provides the intelligence**: real-time crowd metrics, ML anomaly detection, and a
**GenAI advisor** that turns anomalies into plain-English guidance for the duty
supervisor — all within seconds, end to end.

That separation is the point: control that *should* be deterministic stays
deterministic; the streaming platform adds observability, ML, and AI-assisted
decision support. The tube theme is just the vehicle. What it demonstrates:

- **Apache Kafka** as the backbone every event flows through.
- **Apache Flink (SQL)** for real-time windowed metrics and stream processing.
- **Flink built-in ML** (`ML_DETECT_ANOMALIES`) spotting abnormal crowd spikes —
  the filter that decides which events are worth escalating.
- **Flink AI inference** (`ML_PREDICT` on **AWS Bedrock**, Claude Sonnet) turning
  those anomalies into plain-English operational advice for the ops/safety team.
- **Deterministic operational control in the emulator** (block signalling +
  gateline) — a clean separation from the analytics/AI, published to Kafka so it
  is part of the same event-driven stream.

## Architecture

```mermaid
flowchart LR
  subgraph local["Runs on your machine (Docker)"]
    EM["Python emulator<br/>owns occupancy · runs trains<br/>deterministic signals + gateline"]
    BE["Flask + Socket.IO<br/>backend"]
    UI["Browser dashboard"]
  end

  subgraph cloud["Confluent Cloud"]
    RAW["Emulator topics<br/>passengers_flow · train_in_transit · train_in_station<br/>station_occupancy · signal_state · gateline_state"]
    FLINK["Flink SQL (analytics only)<br/>windowed metrics · ML anomaly filter · AI advisor"]
    DER["Derived topics<br/>station_metrics · station_anomalies<br/>station_ai_suggestions"]
  end

  BR["AWS Bedrock<br/>(GenAI)"]

  EM -->|Avro events| RAW --> FLINK --> DER
  FLINK <-->|ML_PREDICT| BR
  RAW --> BE
  DER --> BE -->|WebSocket| UI
  UI -->|Rush Hour toggle| BE -->|demo_control| EM
```

**The flow:** the emulator is the single source of truth for occupancy
(guaranteed never negative) and for the operational controls — it runs block
signalling and the gateline **deterministically**, and publishes everything
(raw events + `signal_state` + `gateline_state`) to Kafka. Flink is **analytics
only**: it aggregates the stream into windowed metrics, uses `ML_DETECT_ANOMALIES`
to filter for genuinely abnormal crowd events, and calls Bedrock (`ML_PREDICT`)
to turn those into advice for staff — it does **not** control the station. The
browser sees everything live over WebSockets, and the presenter can trigger a
crowd surge on demand (`demo_control`).

## What you need

- A **Confluent Cloud** account and a **Cloud API key** (resource-management
  scope): `confluent api-key create --resource cloud`.
- **Terraform** ≥ 1.5 and the **Confluent CLI** — to provision the cloud side.
- **Docker** (Desktop or Engine) — to run the app layer. *No Python needed.*
- An **AWS account with Bedrock access** and an IAM access key/secret — the
  GenAI advisor is part of the pipeline. Enable the model's **inference profile**
  under **Bedrock → Model access** (default: Claude Sonnet 4.5). The model id is
  combined with the region-geo prefix into a cross-region inference profile
  (`us-east-1` → `us.anthropic.claude-sonnet-4-5-…`), which is how newer Claude
  models must be invoked — the bare model id returns 404.

Everything (Confluent Cloud and Bedrock) runs in **AWS `us-east-1`** by default,
so the Flink AI inference calls Bedrock in-region. Change `cc_cloud_region` /
`bedrock_region` together to relocate it.

## Quickstart

### 1. Clone and configure

```bash
git clone <this-repo> tube-station && cd tube-station
cp .env_example .env
```

Edit `.env` and fill in your **Confluent Cloud** API key/secret and your **AWS**
keys (for the Bedrock advisor). Keep it as plain `KEY=VALUE` lines. Terraform
fills in the Kafka / Schema Registry values for you in the next step.

### 2. Provision Confluent Cloud

```bash
set -a; source .env; set +a          # load Confluent + AWS creds into your shell
cd terraform
terraform init
terraform apply
```

This creates the environment, a Standard Kafka cluster, Schema Registry, all
topics + Avro schemas, the Flink compute pool, every Flink SQL statement, and the
Bedrock connection + model + AI advisor. It also writes the Kafka and Schema
Registry connection details back into your root `.env` automatically.

### 3. Run the demo (Docker)

```bash
cd ..
docker compose up --build
```

This starts the **emulator** (generates traffic) and the **backend** (serves the
dashboard). Open **http://localhost:8080**.

The dashboard shows a platform-safety panel (evacuation time), an animated
station strip (trains approaching, queueing behind a red signal, dwelling at the
platform and departing; the Tilley box fills with the crowd; signals and the
street gateline change colour), an occupancy chart with anomaly markers, a
flow-in/out chart, a live event feed, and the AI advisor's advice.

Flip the **Rush Hour** toggle (top-right, red when on) to trigger a surge and
watch the system react — as occupancy climbs the gateline **restricts** then
**closes** to cap street inflow, `ML_DETECT_ANOMALIES` flags the spike, and the
**AI advisor** posts concrete guidance for the ops/safety team. Trains keep
cycling through the platform (block signalling), draining the crowd, until the
gateline reopens. Flip it back to reset.

Stop with `Ctrl-C`, or `docker compose down`. (Re-run with `--build` whenever you
change the frontend, since it's baked into the image.)

### Preview the UI without the cloud

The dashboard has a self-contained demo mode that synthesises the whole story in
the browser — handy for a quick look with no pipeline running:

**http://localhost:8080/?demo=1**

### 4. Tear down

```bash
docker compose down
cd terraform && terraform destroy
```

Confluent CFUs and the cluster cost money — always destroy after you're done.

## Running without the emulator (optional)

`tools/mockfeed.py` publishes synthetic Avro to every topic (including a scripted
surge and simplified control loops), so you can drive the dashboard without the
emulator + Flink:

```bash
docker compose --profile mock up --build    # backend + mock feed
```

## Configuration

Each setting lives in exactly one place:

| File | Holds |
|---|---|
| **`demo.config.json`** | shared domain constants — station capacity, evacuation flow, aggregation window, and the crowd thresholds. Read by both Terraform and the Python apps. |
| **`.env`** (git-ignored) | secrets (Confluent/AWS keys) and app runtime knobs — passenger/train/surge rates, and `INITIAL_OCCUPANCY_FRACTION` (starting/RESET occupancy as a fraction of capacity, default `0.45`). Kafka + Schema Registry values are written here automatically by `terraform apply`. Plain `KEY=VALUE` lines. |
| **`terraform/vars.tf`** (+ `terraform.tfvars`) | cloud infrastructure — region, cluster, compute units, retention, Bedrock model. |

Change a threshold or the window once in `demo.config.json` and both the Flink
SQL and the Python apps pick it up.

> **Security note:** when the AI advisor is enabled, the Bedrock connection's AWS
> keys are stored in Terraform state. `terraform.tfstate` is git-ignored — keep
> it secure, or use an encrypted remote backend.

## How the pieces fit

```
tube-station/
├── demo.config.json       # single source of truth for shared domain constants
├── Dockerfile             # one image: emulator, backend, or mock feed
├── docker-compose.yml     # runs the app layer
├── terraform/             # Confluent Cloud footprint + Flink SQL (terraform/sql/)
├── emulator/              # Python passenger/train simulator (owns occupancy)
├── backend/               # Flask + Socket.IO: streams topics to the browser
├── frontend/              # browser dashboard (served by the backend)
└── tools/mockfeed.py      # synthetic data generator (run the UI without the cloud pipeline)
```

- **Emulator** — generates foot traffic and a per-train lifecycle (approach →
  dwell → depart), owns occupancy, and runs the two **deterministic** operational
  controls itself: **block signalling** (one train per platform per direction;
  the signal is RED while a train dwells and following trains queue behind it) and
  the **gateline** (throttles street inflow by occupancy band). It publishes
  `signal_state` and `gateline_state` to Kafka. Self-stabilising boarding keeps
  occupancy in a healthy mid-band; only a surge pushes it critical, and it
  recovers. The only control it consumes is the presenter's `demo_control`.
- **Flink SQL** (`terraform/sql/`) — **analytics only**: windowed crowd metrics
  (`01`), `ML_DETECT_ANOMALIES` on foot/alighting spikes as the filter (`02`), and
  the Bedrock-backed advisory AI (`05`/`06`, `ML_PREDICT`) triggered by those
  anomalies/thresholds. It derives insight from the stream; it does not control
  the station.
- **Backend** — one Socket.IO channel per stream (`occupancy`, `metrics`,
  `anomaly`, `signal`, `gateline`, `ai_suggestion`, plus raw events) and the
  `POST /demo/surge` / `POST /demo/reset` control endpoints.
- **Frontend** — a single React page served at `/` (loaded from CDNs, no build
  step or `npm`): platform-safety panel, animated SVG station strip (with the
  occupancy readout inside the Tilley box, and trains that queue behind red
  signals), Chart.js occupancy/flow charts, live event + AI-advice feeds, and the
  Rush Hour toggle. Append `?demo=1` for a cloud-free preview.

## Developing without Docker

You can run the apps natively with Python 3.12:

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r emulator/requirements.txt -r backend/requirements.txt
python -m emulator.run     # terminal 1
python -m backend.run      # terminal 2 → http://localhost:8080
```

Run the test suite (no cloud needed — it uses in-memory producers and a fast
virtual-clock simulator):

```bash
python -m pytest
```
