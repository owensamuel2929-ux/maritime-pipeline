# Maritime Supply Chain Intelligence Pipeline

A production-style data engineering portfolio project that monitors live vessel traffic at the Port of Rotterdam (NLRTM) — one of the world's busiest ports.

---

## Architecture

```
VesselAPI (live data)
       │
       ▼
  Apache Airflow          ← orchestrates the pipeline every 6 hours
       │
       ▼
  PostgreSQL (raw)        ← raw.port_events, raw.vessel_positions
       │
       ▼
     dbt                  ← staging + mart transformations
       │
       ▼
  PostgreSQL (marts)      ← mart_vessels_in_port, mart_port_congestion, etc.
       │
       ▼
  HTML Dashboard          ← Leaflet map + Chart.js visualizations
```

## Stack

| Tool | Purpose |
|---|---|
| Apache Airflow 2.9 | Pipeline orchestration (every 6 hours) |
| PostgreSQL 15 | Data storage (raw + transformed) |
| dbt 1.7 | SQL transformations (staging → marts) |
| VesselAPI SDK | Live Rotterdam port data |
| Docker Compose | Full stack containerisation |
| Python 3.11 | Extract + load scripts |

---

## Data Flow

### Extract
- **Port events** — arrivals and departures at Rotterdam (NLRTM), paginated with `time_from` / `time_to`
- **Vessel positions** — live GPS coordinates, speed, heading per MMSI
- **Vessel emissions** — CO₂ and fuel data (when available)

### Transform (dbt)
| Model | Schema | Description |
|---|---|---|
| `stg_port_events` | dbt_staging | Cleaned port events with typed timestamps |
| `stg_vessel_positions` | dbt_staging | Filtered positions (lat/lon not null) |
| `mart_vessels_in_port` | dbt_marts | Vessels + GPS positions joined — map-ready |
| `mart_port_congestion` | dbt_marts | Daily arrivals, departures, congestion level |
| `mart_hourly_arrivals` | dbt_marts | Traffic pattern by hour of day |
| `mart_carrier_performance` | dbt_marts | Per-vessel event activity summary |

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

> **Note:** If you already have PostgreSQL running locally on port 5432, the Docker postgres is mapped to **5433** to avoid conflicts.

### 4. Access the services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5433 | see .env |

### 5. Trigger the pipeline
1. Open Airflow at http://localhost:8080
2. Enable the `maritime_pipeline` DAG (toggle on)
3. Click the **▶** play button to trigger a manual run

### 6. Generate the dashboard
```bash
docker exec <airflow-scheduler-id> python3 src/dashboard.py
```
Then open `dashboard.html` in your browser.

---

## Project Structure

```
├── dags/
│   └── maritime_dag.py          # Airflow DAG definition
├── src/
│   ├── extract.py               # VesselAPI data extraction
│   ├── load.py                  # PostgreSQL loader
│   ├── dashboard.py             # HTML dashboard generator
│   └── notify.py                # Telegram alerts (optional)
├── dbt/
│   └── maritime/
│       ├── models/
│       │   ├── staging/         # stg_port_events, stg_vessel_positions
│       │   └── marts/           # mart_* business-ready tables
│       └── profiles.yml
├── init/
│   └── 01_init.sql              # Creates raw schema + airflow/metabase DBs
├── Dockerfile                   # Airflow + dbt + VesselAPI image
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Dashboard Preview

The generated `dashboard.html` includes:
- **KPI cards** — vessels in port, arrivals, departures, congestion level
- **Interactive map** — all vessels plotted on Rotterdam with click popups
- **Hourly traffic chart** — when the port is busiest
- **Vessel activity table** — per-vessel event log

---

## API Quota Note

VesselAPI free plans have a monthly call limit. If the pipeline hits the quota, the extract tasks log a warning and skip gracefully — dbt still runs on existing data, keeping the marts up to date.
