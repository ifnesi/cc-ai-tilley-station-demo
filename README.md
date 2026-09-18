# Tilley Station: Real-Time Crowd Management on Confluent Cloud

A live, continuously-running demo of **event-driven architecture** built on
Confluent Cloud. It models crowd safety at a fictional London Underground
station, **Tilley**, on one line between two neighbours: **Overton** (west) and
**Jones** (east).

Passengers pour into Tilley from the street and from arriving trains. The
station runs its own **deterministic operational controls**, block signalling
(a train holds the platform, the next one queues at a red) and an occupancy-based
street gateline. On top of that operational event stream, **Confluent Cloud
provides the intelligence**: real-time crowd metrics, ML anomaly detection, and a
**GenAI advisor** that turns anomalies into plain-English guidance for the duty
supervisor, all within seconds, end to end.

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
  gateline), a clean separation from the analytics/AI, published to Kafka so it
  is part of the same event-driven stream.
- **Ask-AI**, the supervisor can type an ad-hoc question; the backend snapshots
  the live metrics + anomalies into it, it goes onto Kafka, Flink asks Bedrock
  with that context, and the answer streams back to the browser.
- **Real-Time Context Engine (RTCE) + managed MCP**, every topic is exposed to
  Confluent Cloud's managed MCP server, so **Claude Code** can connect and ask
  questions about the live station data (read-only).

## Architecture

![Architecture Diagram](docs/architecture-diagram.png)

**The flow:** the emulator is the single source of truth for occupancy
(guaranteed never negative) and for the operational controls, it runs block
signalling and the gateline **deterministically**, and publishes everything
(raw events + `signal_state` + `gateline_state`) to Kafka. Flink is **analytics
only**: it aggregates the stream into windowed metrics, uses `ML_DETECT_ANOMALIES`
to filter for genuinely abnormal crowd events, and calls Bedrock (`ML_PREDICT`)
to turn those into advice for staff, it does **not** control the station. The
browser sees everything live over WebSockets, and the presenter can trigger a
crowd surge on demand (`demo_control`).

## The Flink pipeline, where the power is

This is the heart of the demo: a chain of Flink SQL jobs, each reading Kafka
topics and writing Kafka topics, exactly what Confluent Cloud's
[Stream Lineage](https://docs.confluent.io/cloud/current/stream-governance/stream-lineage.html)
shows you live. Rounded boxes are **Kafka topics**; rectangles are **Flink SQL
jobs** (`terraform/sql/`):

```mermaid
flowchart LR
  classDef topic fill:#0d2233,stroke:#4aa3c7,color:#dbeafe;
  classDef job fill:#2a1436,stroke:#b06fd6,color:#f3e8ff;

  PF(["passengers_flow"]):::topic
  TT(["train_in_transit"]):::topic
  TS(["train_in_station"]):::topic
  SO(["station_occupancy"]):::topic

  OQ(["operator_questions"]):::topic

  J1["<b>01 · metrics</b><br/>TUMBLE window<br/>per-window crowd stats"]:::job
  J2["<b>02 · anomalies</b><br/>ML_DETECT_ANOMALIES<br/>(the filter)"]:::job
  J6["<b>06 · AI advisor</b><br/>ML_PREDICT → Bedrock"]:::job
  J7["<b>07 · operator Q&A</b><br/>question + context → ML_PREDICT"]:::job

  SM(["station_metrics"]):::topic
  SA(["station_anomalies"]):::topic
  AI(["station_ai_suggestions"]):::topic
  OA(["operator_answers"]):::topic
  BR["AWS Bedrock<br/>Claude Sonnet"]

  PF --> J1
  TT --> J1
  TS --> J1
  SO --> J1
  J1 --> SM
  SM --> J2 --> SA
  SA --> J6
  SM --> J6
  J6 --> AI
  J6 <-->|ML_PREDICT| BR
  OQ --> J7
  J7 --> OA
  J7 <-->|ML_PREDICT| BR
```

Each statement lives in `terraform/sql/` and is deployed by `terraform apply`:

| Statement | What it's for | Inputs | What it does | Output |
|---|---|---|---|---|
| **[`01_metrics.sql`](terraform/sql/01_metrics.sql)** | Windowed crowd metrics | `passengers_flow`, `train_in_station`, `train_in_transit`, `station_occupancy` | `UNION ALL`s the four raw streams into one normalised stream, then a single `TUMBLE` aggregation (`agg_window_seconds`, 5s) — foot-in, alighting/boarding totals (and per-direction), net change, latest occupancy + `occupancy_pct`. | `station_metrics` |
| **[`02_anomalies.sql`](terraform/sql/02_anomalies.sql)** | ML anomaly detection (the filter) | `station_metrics` | Runs `ML_DETECT_ANOMALIES` over `foot_in` and `alight_total` in an unbounded `OVER` window, partitioned by station; keeps only rows flagged `is_anomaly`, carrying occupancy context through. | `station_anomalies` |
| **[`05_ai_model.sql`](terraform/sql/05_ai_model.sql)** | Register the alert advisor model | none — defines a model, reads no topic | `CREATE MODEL` for a Bedrock text-generation model (`ops_advisor`) with the four-line `SEVERITY/ASSESSMENT/ACTIONS/WATCH` system prompt and a ~20-action LU toolkit. | Flink model `ops_advisor` |
| **[`05b_qa_model.sql`](terraform/sql/05b_qa_model.sql)** | Register the Ask-AI model | none — defines a model, reads no topic | `CREATE MODEL` for a second, conversational Bedrock model (`ops_qa`) that answers a free-text supervisor question from live context (Markdown, ~120 words). | Flink model `ops_qa` |
| **[`06_ai_suggestions.sql`](terraform/sql/06_ai_suggestions.sql)** | GenAI advisor | `station_anomalies`, `station_metrics` | `UNION ALL`s two triggers — any anomaly, and occupancy band step-ups/HIGH-CRITICAL re-fires — builds a prompt with live context, and calls `ML_PREDICT` on `ops_advisor`; parses out `severity`. | `station_ai_suggestions` |
| **[`07_operator_qa.sql`](terraform/sql/07_operator_qa.sql)** | Ask-AI (on demand) | `operator_questions` (with a backend-snapshotted context) | Builds a prompt from the (length-capped) question + context and calls `ML_PREDICT` on `ops_qa`, correlated by `question_id`. Append-only, no join. | `operator_answers` |

Reading the pipeline end to end, it tells the whole Flink story:

1. **Stream processing**, `01` fans four raw event streams into one and runs a
   single tumbling-window aggregation (`TUMBLE`) into `station_metrics`: crowd
   counts per window (the `agg_window_seconds` tumble, 5s by default),
   continuously.
2. **Built-in ML**, `02` runs `ML_DETECT_ANOMALIES` over that windowed stream
   (the single place the anomaly ML runs). Only genuinely abnormal crowd spikes
   flow into `station_anomalies`, which feeds **both** the dashboard (the red
   markers on the occupancy chart) **and** the AI advisor.
3. **GenAI advisor**, `06` calls `ML_PREDICT` against a Bedrock model (`05`
   registers it, with a ~20-action London-Underground toolkit) to write
   `station_ai_suggestions`, concrete guidance for the ops team. It fires from
   two sources: **any anomaly** on `station_anomalies`, **and** occupancy bands on
   `station_metrics`. The band cadence is deliberate: a step *up* into BUSY (70%,
   an early warning) fires once and is then debounced on a plateau, while HIGH
   (85%) and CRITICAL (95%) re-fire **every window** as a running reminder while
   the station is in trouble. It never fires per raw event, so the expensive AI
   stays cheap, and the advice scales from a one-off early warning to continuous
   crisis guidance during a surge.
4. **GenAI on demand**, `07` powers the **Ask-AI** feature: a supervisor
   question lands on `operator_questions` with a live-context snapshot the backend
   took from its metrics/anomaly consumers; `07` sends it to a second Bedrock
   model (`05b`) via `ML_PREDICT` and writes the answer to `operator_answers`.

Signals and the gateline are **not** here: they are deterministic and owned by
the emulator. Flink does what Flink is uniquely good at, windowed aggregation,
streaming ML, and AI inference, on the operational event stream.

The same pipeline as Confluent Cloud sees it — the live Stream Lineage:

![Stream Lineage](docs/cluster-lkc-mvpko0q-lineage.png)

...and the Flink SQL statements that drive it, deployed by `terraform apply`:

![Flink SQL statements](docs/flink-statements.png)

## Topics

The Kafka topics that power the demo:

![Kafka topics](docs/kafka-topics.png)

| Topic | Purpose |
|-------|---------|
| **passengers_flow** | Raw street-entry passenger events (already throttled by the gateline). |
| **train_in_transit** | Trains approaching (with passenger count + ETA) or departing (carrying `passengers_boarding`). |
| **train_in_station** | Trains dwelling at the platform, with the alighting splits; boarding is recorded on the departing `train_in_transit` event. |
| **station_occupancy** | Authoritative occupancy heartbeat emitted by the emulator (guaranteed never negative). |
| **signal_state** | Block signalling state (RED when train dwells, GREEN when clear). |
| **gateline_state** | Street gateline control state (OPEN, RESTRICTED, or CLOSED based on occupancy). |
| **demo_control** | Control events triggered by the presenter (e.g., surge toggles). |
| **station_metrics** | Aggregated tumbling windows (`agg_window_seconds`, 5s by default): foot counts, alighting, boarding, occupancy bands. |
| **station_anomalies** | Detected anomalies flagged by `ML_DETECT_ANOMALIES` (crowd spikes). |
| **station_ai_suggestions** | AI advisor guidance triggered by anomalies or occupancy thresholds. |
| **operator_questions** | Free-form questions from operators asking about live station state. |
| **operator_answers** | AI-generated answers to operator questions, enriched with live context. |

## What you need

- A **Confluent Cloud** account and a **Cloud API key** (resource-management
  scope): `confluent api-key create --resource cloud`.
- **Terraform** ≥ 1.5 and the **Confluent CLI**, to provision the cloud side.
- **Docker** (Desktop or Engine), to run the app layer. *No Python needed.*
- An **AWS account with Bedrock access** and an IAM access key/secret, the
  GenAI advisor is part of the pipeline. Enable the model's **inference profile**
  under **Bedrock → Model access** (default: Claude Sonnet 4.5). The model id is
  combined with the region-geo prefix into a cross-region inference profile
  (`us-east-1` → `us.anthropic.claude-sonnet-4-5-…`), which is how newer Claude
  models must be invoked, the bare model id returns 404.

Everything (Confluent Cloud and Bedrock) runs in **AWS `us-east-1`** by default,
so the Flink AI inference calls Bedrock in-region. Change `cc_cloud_region` /
`bedrock_region` together to relocate it.

## Quickstart

### 1. Clone and configure

```bash
git clone git@github.com:ifnesi/cc-ai-tilley-station-demo.git && cd cc-ai-tilley-station-demo
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
topics + Avro schemas, the Flink compute pool, every Flink SQL statement, the
Bedrock connection + two models (the alert advisor and the Ask-AI model), and it
enables **RTCE on every topic** plus a read-only **`mcp-reader`** principal for
the managed MCP server. It also writes the Kafka / Schema Registry connection
details, the domain constants, **and** the managed-MCP URL (`CC_MCP_URL`) back
into your root `.env` automatically. (The MCP auth token `CC_MCP_AUTH` is the one
value you add by hand, see [Ask the live data from Claude Code](#ask-the-live-data-from-claude-code-rtce--managed-mcp).)

### 3. Run the demo (Docker)

```bash
cd ..
docker compose up --build
```

This starts the **emulator** (generates traffic) and the **backend** (serves the
dashboard). Open **http://localhost:8080**.

The dashboard shows the **AI supervisor advice** feed at the top, an animated
station strip (trains approaching, queueing behind a red signal, dwelling at the
platform and departing; the Tilley box fills with the crowd and shows live
occupancy; signals and the street gateline change colour), an occupancy chart
with anomaly markers, a flow-in/out chart, and a live event feed.

![Dashboard](docs/frontend-dashboard.png)

Flip the **Rush Hour** toggle (top-right, red when on) to trigger a surge and
watch the system react, as occupancy climbs the gateline **restricts** then
**closes** to cap street inflow, `ML_DETECT_ANOMALIES` flags the spike, and the
**AI advisor** posts concrete guidance for the ops/safety team (early warning at
BUSY, escalating through HIGH and CRITICAL). Trains keep cycling through the
platform (block signalling), draining the crowd, until the gateline reopens. Flip
it back to reset.

Switch to the **Ask AI** tab (top of the page) to ask ad-hoc questions: it keeps
the full question/answer history with timestamps and a text box at the bottom.
Each question is enriched with the live station context and answered by Bedrock,
while the **Dashboard** tab keeps running in the background.

![Ask AI tab](docs/frontend-ask-ai.png)

Stop with `Ctrl-C`, or `docker compose down`. (Re-run with `--build` whenever you
change the frontend, since it's baked into the image.)

### Preview the UI without the cloud

The dashboard has a self-contained demo mode that synthesises the whole story in
the browser, handy for a quick look with no pipeline running:

**http://localhost:8080/?demo=1**

### 4. Tear down

```bash
docker compose down
cd terraform && terraform destroy
```

Confluent CFUs and the cluster cost money, always destroy after you're done.

## Ask the live data from Claude Code (RTCE + managed MCP)

`terraform apply` also enables Confluent's **Real-Time Context Engine (RTCE)** on
**every topic** in this demo (`confluent_rtce_topic`, one per topic), so the data
is queryable through Confluent Cloud's **managed MCP server**, and Claude Code
can connect to it and ask questions about the live station data.

The repo-root **`.mcp.json`** declares the server (`cc-managed-mcp`, an HTTP MCP)
and resolves from two env vars:

```jsonc
"cc-managed-mcp": {
  "type": "http",
  "url": "${CC_MCP_URL}",                         // regional, cluster-scoped endpoint
  "headers": { "Authorization": "Basic ${CC_MCP_AUTH}" }
}
```

- **`CC_MCP_URL`**, Terraform builds this and writes it into `.env`. It's the
  **regional, cluster-scoped** endpoint:
  `https://mcp.<region>.<cloud>.confluent.cloud/mcp/v1/context-engine/organizations/<org>/environments/<env>/kafka-clusters/<lkc>`
- **`CC_MCP_AUTH`**, **you create this yourself** (Terraform does not generate
  it). The regional managed-MCP server only accepts a **Global API key**; it
  rejects a plain Cloud API key with `404 resource_not_found`, and a Global key
  is the one credential the Terraform provider **cannot** mint, so it's a
  one-time manual step (see below).

### Create the Global API key (one time)

1. In the Confluent Cloud Console, go to **Cloud API keys → Add key → Granular
   access → *Global***.
2. Set the **owner** to the **`mcp-reader`** service account Terraform created,
   so the key inherits its read-only RBAC:
   ```bash
   cd terraform && terraform output mcp_reader_service_account   # the SA id to pick as owner
   ```
3. Copy the key + secret, base64-encode `key:secret` (use `printf`, **not**
   `echo`, a trailing newline causes a `401`), and set it as `CC_MCP_AUTH` in
   your root `.env` (it's a secret; `.env` is git-ignored):
   ```bash
   printf '%s:%s' <GLOBAL_KEY> <GLOBAL_SECRET> | base64      # paste result as CC_MCP_AUTH=… in .env
   ```

`terraform apply` never overwrites `CC_MCP_AUTH`, so it persists across future
applies.

### Launch Claude Code

From the repo root, with both values now in `.env`:

```bash
set -a; source .env; set +a          # exports CC_MCP_URL + CC_MCP_AUTH
claude
```

Approve `cc-managed-mcp` on first run, then ask, e.g.:
> *"Using cc-managed-mcp, list the topics and summarise the latest station_metrics and station_anomalies."*

![Claude Code querying the live data over the Confluent managed MCP](docs/claude-mcp-confluent.png)

## Running without the emulator (optional)

`tools/mockfeed.py` publishes synthetic Avro to every topic (a scripted surge
plus rough stand-ins for the metrics/anomaly/AI outputs), so you can drive the
dashboard without the emulator + Flink:

```bash
docker compose --profile mock up --build    # backend + mock feed
```

## Configuration

There is no separate config data file, everything is either an **env var** or a
**Terraform variable**:

| Where | Holds |
|---|---|
| **`.env`** (git-ignored; template in **`.env_example`**) | Everything the Python apps read: secrets (Confluent/AWS keys), the domain constants (`STATION_CAPACITY`, `AGG_WINDOW_SECONDS`, `PCT_LOW/HIGH/CRITICAL`, `STATION_NAME`), the demo knobs (foot/train/surge rates, `INITIAL_OCCUPANCY_FRACTION`), and the managed-MCP `CC_MCP_URL` (written by `terraform apply`; `CC_MCP_AUTH` is a Global API key you add by hand). Plain `KEY=VALUE` lines. |
| **`terraform/vars.tf`** | Terraform's copy: cloud infra (region, cluster, CFUs, retention, Bedrock model) **and** the domain constants used to template the Flink SQL. |

The domain constants exist in both places by design, kept in step automatically:
`terraform apply` resolves them from `vars.tf` and **writes them into `.env`**
(along with the Kafka / Schema Registry connection details) via
`terraform/write_env.sh`, so Flink and the Python apps always use the same
numbers. **Nothing is hard-coded in the Python source**, the apps read every
value from the environment and **fail loudly** if `.env` is missing a key (no
silent defaults). `cp .env_example .env` gives you a complete starting point; the
test suite loads `.env_example` itself so it needs no real `.env`.

> **Security note:** the Bedrock connection's AWS keys are stored in Terraform
> state. `terraform.tfstate` is git-ignored, keep it secure, or use an encrypted
> remote backend.

## How the pieces fit

```
cc-ai-tilley-station-demo/
├── .env_example           # template for .env, every runtime var lives here
├── .mcp.json              # Claude Code MCP config (managed MCP / RTCE endpoint)
├── Dockerfile             # one image: emulator, backend, or mock feed
├── docker-compose.yml     # runs the app layer
├── terraform/             # Confluent Cloud footprint + Flink SQL (terraform/sql/)
├── emulator/              # Python passenger/train simulator (owns occupancy + controls)
├── backend/               # Flask + Socket.IO: streams topics to the browser
├── frontend/              # browser dashboard (served by the backend)
└── tools/mockfeed.py      # synthetic data generator (run the UI without the cloud pipeline)
```

- **Emulator**, generates foot traffic and a per-train lifecycle (approach →
  dwell → depart), owns occupancy, and runs the two **deterministic** operational
  controls itself: **block signalling** (one train per platform per direction;
  the signal is RED while a train dwells and following trains queue behind it) and
  the **gateline** (throttles street inflow by occupancy band). It publishes
  `signal_state` and `gateline_state` to Kafka. Self-stabilising boarding keeps
  occupancy in a healthy mid-band; only a surge pushes it critical, and it
  recovers. The only control it consumes is the presenter's `demo_control`.
- **Flink SQL** (`terraform/sql/`), **analytics only**: windowed crowd metrics
  (`01`), `ML_DETECT_ANOMALIES` as the filter (`02`), the Bedrock-backed advisory
  AI (`05`/`06`, `ML_PREDICT`) triggered by anomalies + occupancy bands, and the
  on-demand **Ask-AI** job (`05b`/`07`) that answers a supervisor question, with
  a live-context snapshot the backend attaches, via a second Bedrock model. It
  derives insight from the stream; it does not control the station.
- **Backend**, one Socket.IO channel per stream (`occupancy`, `metrics`,
  `anomaly`, `signal`, `gateline`, `ai_suggestion`, `operator_answer`, plus raw
  events); the `POST /demo/surge` / `POST /demo/reset` control endpoints; and
  `POST /ask`, which publishes the question to `operator_questions`.
- **Frontend**, a single React page served at `/` (loaded from CDNs, no build
  step or `npm`) with two tabs: a **Dashboard** (AI advice feed, animated SVG
  station strip with the occupancy readout in the Tilley box and trains that queue
  behind red signals, Chart.js occupancy/flow charts, live event feed, Rush Hour
  toggle) and an **Ask AI** tab (full Q&A history with timestamps + a text box).
  Append `?demo=1` for a cloud-free preview.

## Developing without Docker

You can run the apps natively with Python 3.12:

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r emulator/requirements.txt -r backend/requirements.txt
python -m emulator.run     # terminal 1
python -m backend.run      # terminal 2 → http://localhost:8080
```

Run the test suite (no cloud needed, it uses in-memory producers and a fast
virtual-clock simulator):

```bash
python -m pytest
```
