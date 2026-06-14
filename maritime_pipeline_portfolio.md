# Maritime Supply Chain Intelligence Pipeline

## Project Overview

```
Name:    Maritime Supply Chain Intelligence
Domain:  Port Logistics / Shipping Analytics
Source:  VesselAPI (free tier, no credit card)
Stack:   Python + Airflow + PostgreSQL + dbt + Docker + Metabase
Target:  Port of Rotterdam, Maersk, DSV, Coolblue
```

---

## Elevator Pitch

> "Built a maritime supply chain intelligence platform monitoring Rotterdam port traffic,
> carrier reliability, and EU carbon compliance — using live VesselAPI data
> orchestrated by Airflow, transformed with dbt, and visualized in Metabase."

---

## Architecture

```
VesselAPI (free)
        ↓
Airflow DAG (every 6 hours)
  → fetch port events at NLRTM (Rotterdam)
  → fetch vessel positions near NL coast
  → fetch incoming ETAs
  → fetch vessel emissions
        ↓
PostgreSQL raw tables
        ↓
dbt models
  → staging layer (clean raw data)
  → mart layer (business insights)
        ↓
Metabase dashboards
  → Port Congestion Monitor
  → Carrier Performance Scorecard
  → Carbon Footprint Tracker
        ↓
Telegram alerts
  → "5 container ships arriving Rotterdam tomorrow"
  → "Carrier X delayed 3 vessels this week"
```

---

## Data Available from VesselAPI

| Category | Fields | Update Frequency |
|---|---|---|
| Vessel tracking | lat, lon, speed, heading, ETA | Sub-minute |
| Port events | arrival/departure, timestamp, vessel | 90K+ daily |
| Vessel emissions | CO2, fuel consumption (EU MRV) | Per voyage |
| Vessel enrichment | owner, type, capacity, flag | Static |
| Safety & compliance | inspections, detentions | Per event |
| Ports database | 120K+ ports, UN/LOCODE | Static |

---

## Project Structure

```
maritime-pipeline/
├── dags/
│   └── maritime_dag.py
├── src/
│   ├── extract.py           ← VesselAPI calls
│   ├── load.py              ← load to PostgreSQL
│   └── notify.py            ← Telegram alerts
├── dbt/
│   └── maritime/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── stg_port_events.sql
│       │   │   ├── stg_vessel_positions.sql
│       │   │   └── stg_vessel_emissions.sql
│       │   └── marts/
│       │       ├── mart_port_congestion.sql
│       │       ├── mart_carrier_performance.sql
│       │       └── mart_carbon_footprint.sql
│       ├── tests/
│       │   └── schema.yml
│       └── dbt_project.yml
├── docker-compose.yml
├── Dockerfile
├── .env
└── README.md
```

---

## Step 1 — Extract from VesselAPI

```python
# src/extract.py
from vessel_api_python import VesselClient
import os

client = VesselClient(api_key=os.getenv("VESSEL_API_KEY"))

def fetch_port_events(port_code="NLRTM"):
    """Fetch all arrivals and departures at Rotterdam"""
    events = client.port_events.by_port(
        port_code,
        filter_event_type="arrival"
    ).port_events

    return [{
        "vessel_name":   e.vessel.name,
        "mmsi":          e.vessel.mmsi,
        "imo":           e.vessel.imo,
        "vessel_type":   e.vessel.vessel_type,
        "event_type":    e.event,
        "port_name":     e.port.name,
        "port_unlocode": e.port.unlo_code,
        "timestamp":     e.timestamp,
        "ingested_at":   pd.Timestamp.now()
    } for e in events]


def fetch_vessel_positions(port_code="NLRTM"):
    """Fetch current positions of vessels near Rotterdam"""
    # Rotterdam bounding box
    positions = client.vessels.by_area(
        lat_min=51.8, lat_max=52.1,
        lon_min=3.9,  lon_max=4.6
    ).vessel_positions

    return [{
        "mmsi":      p.mmsi,
        "vessel_name": p.vessel_name,
        "latitude":  p.latitude,
        "longitude": p.longitude,
        "speed":     p.speed,
        "heading":   p.heading,
        "destination": p.destination,
        "eta":       p.eta,
        "ingested_at": pd.Timestamp.now()
    } for p in positions]


def fetch_emissions(mmsi_list):
    """Fetch EU MRV emissions for vessels"""
    records = []
    for mmsi in mmsi_list:
        try:
            emis = client.vessels.emissions(
                mmsi, filter_id_type="mmsi"
            ).emissions
            for e in emis:
                records.append({
                    "mmsi":              mmsi,
                    "reporting_period":  e.reporting_period,
                    "co2_total":         e.co2_emissions_total,
                    "fuel_consumed":     e.fuel_consumed_total,
                    "distance_traveled": e.distance_travelled,
                    "ingested_at":       pd.Timestamp.now()
                })
        except Exception:
            continue
    return records
```

---

## Step 2 — Load to PostgreSQL

```python
# src/load.py
import psycopg2
import pandas as pd
import os

def load_to_postgres(data: list, table: str):
    """Load list of dicts to PostgreSQL raw table"""
    df = pd.DataFrame(data)
    engine = create_engine(os.getenv("DATABASE_URL"))
    df.to_sql(
        table,
        engine,
        schema="raw",
        if_exists="append",
        index=False
    )
    print(f"Loaded {len(df)} rows to raw.{table}")
```

---

## Step 3 — Airflow DAG

```python
# dags/maritime_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from src.extract import fetch_port_events, fetch_vessel_positions
from src.load import load_to_postgres
from src.notify import send_alert

with DAG(
    "maritime_pipeline",
    schedule="0 */6 * * *",    # every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["maritime", "rotterdam", "portfolio"]
) as dag:

    extract_events = PythonOperator(
        task_id="extract_port_events",
        python_callable=lambda: load_to_postgres(
            fetch_port_events("NLRTM"), "port_events"
        )
    )

    extract_positions = PythonOperator(
        task_id="extract_vessel_positions",
        python_callable=lambda: load_to_postgres(
            fetch_vessel_positions(), "vessel_positions"
        )
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="dbt run --select staging --project-dir /opt/dbt/maritime"
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command="dbt run --select marts --project-dir /opt/dbt/maritime"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /opt/dbt/maritime"
    )

    alert = PythonOperator(
        task_id="send_alerts",
        python_callable=send_alert
    )

    [extract_events, extract_positions] >> dbt_staging >> dbt_marts >> dbt_test >> alert
```

---

## Step 4 — dbt Staging Models

### `stg_port_events.sql`

```sql
SELECT
    vessel_name,
    mmsi,
    imo,
    vessel_type,
    event_type,
    port_name,
    port_unlocode,
    timestamp::timestamptz           AS event_timestamp,
    DATE(timestamp)                  AS event_date,
    EXTRACT(HOUR FROM timestamp)     AS event_hour,
    ingested_at
FROM raw.port_events
WHERE port_unlocode = 'NLRTM'
  AND timestamp IS NOT NULL
```

### `stg_vessel_positions.sql`

```sql
SELECT
    mmsi,
    vessel_name,
    latitude,
    longitude,
    speed,
    heading,
    destination,
    eta::timestamptz    AS eta,
    ingested_at
FROM raw.vessel_positions
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
```

### `stg_vessel_emissions.sql`

```sql
SELECT
    mmsi,
    reporting_period,
    co2_total,
    fuel_consumed,
    distance_traveled,
    ROUND(co2_total / NULLIF(distance_traveled, 0), 4)
        AS co2_per_km,
    ingested_at
FROM raw.vessel_emissions
WHERE co2_total IS NOT NULL
```

---

## Step 5 — dbt Mart Models (The 3 Key Insights)

### Insight 1 — `mart_port_congestion.sql`

```sql
-- How busy is Rotterdam port right now?

WITH events AS (
    SELECT * FROM {{ ref('stg_port_events') }}
),

daily_traffic AS (
    SELECT
        event_date,
        vessel_type,
        COUNT(*) FILTER (WHERE event_type = 'arrival')
            AS arrivals,
        COUNT(*) FILTER (WHERE event_type = 'departure')
            AS departures,
        COUNT(*) FILTER (WHERE event_type = 'arrival')
        - COUNT(*) FILTER (WHERE event_type = 'departure')
            AS vessels_in_port,

        -- Congestion score
        CASE
            WHEN COUNT(*) > 50 THEN 'HIGH'
            WHEN COUNT(*) > 25 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS congestion_level

    FROM events
    GROUP BY event_date, vessel_type
)

SELECT * FROM daily_traffic
ORDER BY event_date DESC
```

### Insight 2 — `mart_carrier_performance.sql`

```sql
-- Which carriers are reliable vs always late?

WITH positions AS (
    SELECT * FROM {{ ref('stg_vessel_positions') }}
    WHERE eta IS NOT NULL
),

performance AS (
    SELECT
        vessel_name,
        mmsi,
        destination,
        eta,
        ingested_at,

        -- Delay calculation (ETA vs when we first saw it)
        EXTRACT(EPOCH FROM (eta - ingested_at)) / 3600
            AS hours_until_arrival,

        -- On-time flag (arriving within 2 hours of ETA)
        CASE
            WHEN ABS(EXTRACT(EPOCH FROM
                (eta - ingested_at)) / 3600) <= 2
            THEN 'ON_TIME'
            WHEN eta < ingested_at THEN 'LATE'
            ELSE 'EARLY'
        END AS arrival_status

    FROM positions
)

SELECT
    vessel_name,
    mmsi,
    COUNT(*)                                    AS total_voyages,
    COUNT(*) FILTER (WHERE arrival_status = 'ON_TIME')
                                                AS on_time_count,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE arrival_status = 'ON_TIME')
        / NULLIF(COUNT(*), 0), 1
    )                                           AS on_time_rate_pct,
    AVG(hours_until_arrival)                    AS avg_transit_hours,
    MIN(eta)                                    AS first_seen,
    MAX(eta)                                    AS last_seen

FROM performance
GROUP BY vessel_name, mmsi
ORDER BY on_time_rate_pct DESC
```

### Insight 3 — `mart_carbon_footprint.sql`

```sql
-- EU carbon compliance per vessel (Green Deal reporting)

WITH emissions AS (
    SELECT * FROM {{ ref('stg_vessel_emissions') }}
),

carbon_summary AS (
    SELECT
        mmsi,
        reporting_period,
        co2_total,
        fuel_consumed,
        distance_traveled,
        co2_per_km,

        -- EU MRV compliance tier
        CASE
            WHEN co2_per_km < 0.05 THEN 'EXCELLENT'
            WHEN co2_per_km < 0.10 THEN 'GOOD'
            WHEN co2_per_km < 0.20 THEN 'AVERAGE'
            ELSE 'HIGH_EMITTER'
        END AS emissions_grade,

        -- Rank within reporting period
        RANK() OVER (
            PARTITION BY reporting_period
            ORDER BY co2_per_km ASC
        ) AS efficiency_rank

    FROM emissions
)

SELECT * FROM carbon_summary
ORDER BY reporting_period DESC, co2_per_km ASC
```

---

## Step 6 — dbt Tests

```yaml
# tests/schema.yml
models:
  - name: mart_port_congestion
    columns:
      - name: event_date
        tests:
          - not_null
      - name: congestion_level
        tests:
          - accepted_values:
              values: ['HIGH', 'MEDIUM', 'LOW']

  - name: mart_carrier_performance
    columns:
      - name: on_time_rate_pct
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "between 0 and 100"

  - name: mart_carbon_footprint
    columns:
      - name: emissions_grade
        tests:
          - accepted_values:
              values: ['EXCELLENT', 'GOOD', 'AVERAGE', 'HIGH_EMITTER']
      - name: co2_total
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: "> 0"
```

---

## Step 7 — Telegram Alert

```python
# src/notify.py
import psycopg2
import requests
import os

def send_alert():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cursor = conn.cursor()

    # Vessels arriving in next 24 hours
    cursor.execute("""
        SELECT vessel_name, destination, eta
        FROM stg_vessel_positions
        WHERE eta BETWEEN NOW() AND NOW() + INTERVAL '24 hours'
        ORDER BY eta ASC
        LIMIT 5
    """)
    incoming = cursor.fetchall()

    # Congestion level today
    cursor.execute("""
        SELECT congestion_level, arrivals, vessels_in_port
        FROM mart_port_congestion
        WHERE event_date = CURRENT_DATE
        LIMIT 1
    """)
    congestion = cursor.fetchone()

    message = "🚢 *Rotterdam Port Update*\n\n"

    if congestion:
        level = congestion[0]
        emoji = "🔴" if level == "HIGH" else "🟡" if level == "MEDIUM" else "🟢"
        message += f"{emoji} Congestion: *{level}*\n"
        message += f"Arrivals today: {congestion[1]}\n"
        message += f"Vessels in port: {congestion[2]}\n\n"

    if incoming:
        message += "📦 *Arriving next 24h:*\n"
        for name, dest, eta in incoming:
            message += f"• {name} → {dest} @ {eta}\n"

    requests.post(
        f"https://api.telegram.org/bot{os.getenv('TELEGRAM_TOKEN')}/sendMessage",
        json={
            "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
            "text": message,
            "parse_mode": "Markdown"
        }
    )
```

---

## Dashboarding — Metabase

### Why Metabase (not Metaplot)

| Tool | Pros | Free? |
|---|---|---|
| **Metabase** | Easy setup, connects to PostgreSQL, no-code charts | ✅ Open source |
| Grafana | Better for time-series/monitoring | ✅ Open source |
| Apache Superset | More powerful, steeper learning curve | ✅ Open source |
| Power BI | Industry standard in NL enterprises | ❌ Paid |
| Tableau | Most popular in corporates | ❌ Paid |

**For portfolio → Metabase.** Runs in Docker, connects directly to your PostgreSQL, zero cost.

---

### Add Metabase to docker-compose.yml

```yaml
services:
  metabase:
    image: metabase/metabase:latest
    ports:
      - "3000:3000"
    environment:
      MB_DB_TYPE: postgres
      MB_DB_HOST: postgres
      MB_DB_PORT: 5432
      MB_DB_USER: ${POSTGRES_USER}
      MB_DB_PASS: ${POSTGRES_PASSWORD}
      MB_DB_DBNAME: ${POSTGRES_DB}
    depends_on:
      - postgres
```

Open `http://localhost:3000` → connect to your PostgreSQL → build dashboards in minutes.

---

### 3 Dashboards to Build

**Dashboard 1 — Port Congestion Monitor**
```
Charts:
→ Line chart: daily arrivals over time
→ Bar chart: arrivals by vessel type
→ KPI card: today's congestion level (RED/AMBER/GREEN)
→ KPI card: vessels currently in port
```

**Dashboard 2 — Carrier Performance Scorecard**
```
Charts:
→ Table: vessel name + on-time rate % (sortable)
→ Bar chart: top 10 most reliable carriers
→ KPI card: average on-time rate across all vessels
→ Scatter plot: transit time vs on-time rate
```

**Dashboard 3 — Carbon Footprint Tracker**
```
Charts:
→ Bar chart: CO2 per km by vessel (ranked)
→ Pie chart: emissions grade distribution
→ Line chart: CO2 trend over reporting periods
→ KPI card: % of fleet rated EXCELLENT or GOOD
```

---

## What Makes This Portfolio-Worthy

| Feature | Why It Matters |
|---|---|
| Real live data (VesselAPI) | Not just simulated — actual Rotterdam vessels |
| EU carbon compliance | Every NL company needs this post-Green Deal |
| Port of Rotterdam focus | Your target employer's exact domain |
| 3 business insights | Covers operations, procurement, sustainability |
| dbt with tests | Production-level data quality |
| Airflow orchestration | Industry standard scheduling |
| Metabase dashboard | Stakeholder-facing output |
| Telegram alerts | Real-time notification system |
| Docker containerized | Runs anywhere reproducibly |

---

## Target Employers & Talking Points

| Company | Angle |
|---|---|
| Port of Rotterdam | "I monitored your port's live traffic and congestion" |
| Maersk / MSC | "I tracked carrier performance and on-time rates" |
| DSV | "I built EU carbon compliance reporting for maritime" |
| Coolblue | "I tracked inbound shipment ETAs for inventory planning" |

---

## Checklist

- [ ] Sign up for VesselAPI free tier (no credit card)
- [ ] Set up docker-compose (Airflow + PostgreSQL + Metabase)
- [ ] Write `extract.py` — VesselAPI calls
- [ ] Write `load.py` — load to PostgreSQL
- [ ] Set up dbt project structure
- [ ] Write staging models (3 files)
- [ ] Write mart models (3 insights)
- [ ] Add dbt tests
- [ ] Build Metabase dashboards (3 dashboards)
- [ ] Connect Telegram alert
- [ ] Push to GitHub with clean README + architecture diagram
- [ ] Deploy to Azure free tier VM
- [ ] Add CI/CD with GitHub Actions
