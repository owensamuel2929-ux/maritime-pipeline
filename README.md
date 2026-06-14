# Maritime Supply Chain Intelligence Pipeline

A production-style data engineering portfolio project that monitors live vessel traffic at the Port of Rotterdam (NLRTM) — one of the world's busiest ports.

---

## Architecture

```
VesselAPI (live data)
       │
       ▼
  Apache Airflow          ← orchestrates every 6 hours
       │  incremental fetch — only events newer than last run
       ▼
  PostgreSQL (raw)        ← raw.port_events (deduped), raw.vessel_positions (snapshot)
       │
       ▼
  dbt staging             ← filters nulls, future timestamps, out-of-bounds positions
       │
       ▼
  dbt marts               ← business-ready tables
       │
       ▼
  dbt test (28 tests)     ← error severity on critical fields, warn on nullable fields
       │
       ▼
  HTML Dashboard          ← Leaflet vessel map + Chart.js + KPI cards
```

---

## Stack

| Tool | Version | Purpose |
|---|---|---|
| Apache Airflow | 2.9.0 | Pipeline orchestration |
| PostgreSQL | 15 | Data storage (raw + transformed) |
| dbt | 1.7.17 | SQL transformations + data validation |
| VesselAPI Python SDK | 1.3.0 | Live Rotterdam port data |
| Python | 3.11 | Extract, load, dashboard |
| Docker Compose | — | Full stack containerisation |

---

## Data Flow

### Extract (Incremental)
Each run uses the last stored event timestamp as `time_from` — only new events are fetched from the API, not the full history every time.

- **Port events** — arrivals + departures at Rotterdam (NLRTM), paginated
- **Vessel positions** — live GPS per MMSI (replaced each run — latest snapshot only)
- **Vessel emissions** — CO₂ / fuel data when available

### Transform (dbt)

| Model | Schema | Rows | Description |
|---|---|---|---|
| `stg_port_events` | dbt_staging | view | Cleaned events — nulls + future timestamps filtered out |
| `stg_vessel_positions` | dbt_staging | view | Positions filtered to Rotterdam bounding box |
| `mart_vessels_in_port` | dbt_marts | 60 | Vessel name + live GPS — map-ready |
| `mart_port_congestion` | dbt_marts | — | Daily arrivals, departures, congestion level |
| `mart_hourly_arrivals` | dbt_marts | — | Traffic pattern by hour of day |
| `mart_carrier_performance` | dbt_marts | — | Per-vessel event activity summary |

### Validate (28 dbt tests)

Two-layer approach:
1. **Staging filters** — bad rows (null MMSI, future timestamps, out-of-bounds GPS) never reach marts
2. **Schema tests with severity** — `error` stops the DAG; `warn` logs and continues

| Severity | Fields |
|---|---|
| `error` | mmsi, event_type, event_timestamp, lat/lon, congestion_level |
| `warn` | imo, speed, heading (legitimately null for anchored vessels) |

---

## Cost Controls

| Problem | Solution |
|---|---|
| API re-fetches data already in DB | Incremental fetch using last stored timestamp as `time_from` |
| Duplicate rows accumulating in `port_events` | Dedup on `(mmsi, timestamp, event_type)` before insert |
| `vessel_positions` growing unbounded | `if_exists="replace"` — latest snapshot only |
| API quota exceeded breaks entire DAG | 429 errors caught — dbt still runs on existing data |

---

## Getting Started

### 1. Clone the repo
```bash
git clone https://github.com/owensamuel2929-ux/maritime-pipeline.git
cd maritime-pipeline
```

### 2. Set up environment variables
```bash
cp .env.example .env
# Edit .env and add your VesselAPI key
```

### 3. Start the stack
```bash
docker compose up -d
```

> If you have PostgreSQL running locally on port 5432, Docker postgres is mapped to **5433**.

### 4. Access services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5433 | see .env |

### 5. Trigger the pipeline
1. Open Airflow → enable `maritime_pipeline` DAG
2. Click **▶** to trigger a manual run
3. All 6 tasks should go green: extract × 3 → dbt_staging → dbt_marts → dbt_test

### 6. Run dbt manually
```bash
docker exec <airflow-scheduler-id> bash -c \
  "dbt run --project-dir /opt/dbt/maritime --profiles-dir /opt/dbt/maritime"
```

### 7. Run dbt tests manually
```bash
docker exec <airflow-scheduler-id> bash -c \
  "dbt test --project-dir /opt/dbt/maritime --profiles-dir /opt/dbt/maritime"
```

### 8. Generate the dashboard
```bash
docker exec <airflow-scheduler-id> python3 src/dashboard.py
# opens dashboard.html in your project folder
```

---

## Project Structure

```
├── dags/
│   └── maritime_dag.py              # Airflow DAG — 6 tasks, XCom, quota handling
├── src/
│   ├── extract.py                   # Incremental VesselAPI fetch with pagination
│   ├── load.py                      # Dedup insert + snapshot replace
│   ├── dashboard.py                 # Standalone HTML dashboard generator
│   └── notify.py                    # Telegram alerts (optional)
├── dbt/
│   └── maritime/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── schema.yml       # Staging tests (error + warn severity)
│       │   │   ├── stg_port_events.sql
│       │   │   └── stg_vessel_positions.sql
│       │   └── marts/
│       │       ├── schema.yml       # Mart tests (error + warn severity)
│       │       ├── mart_vessels_in_port.sql
│       │       ├── mart_port_congestion.sql
│       │       ├── mart_hourly_arrivals.sql
│       │       └── mart_carrier_performance.sql
│       └── profiles.yml
├── init/
│   └── 01_init.sql                  # Creates raw schema on first postgres start
├── Dockerfile                       # Airflow + dbt + VesselAPI image
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Dashboard

`dashboard.html` is generated from live mart data — no extra Docker image needed.

- **KPI cards** — vessels in port, arrivals, departures, congestion level (color-coded)
- **Interactive map** — all vessels on Rotterdam with click popups (Leaflet.js)
- **Hourly traffic chart** — when the port is busiest (Chart.js)
- **Vessel activity table** — per-vessel event log

---

## API Quota Note

VesselAPI free plans have a monthly call limit. When quota is exceeded the extract tasks log a warning and return empty — dbt still runs and rebuilds marts from existing data. The pipeline never fully breaks.
