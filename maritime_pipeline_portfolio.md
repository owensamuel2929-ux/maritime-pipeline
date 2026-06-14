# Maritime Supply Chain Intelligence Pipeline

## Project Overview

```
Name:    Maritime Supply Chain Intelligence
Domain:  Port Logistics / Shipping Analytics
Source:  VesselAPI (live data)
Stack:   Python + Airflow + PostgreSQL + dbt + Docker + HTML Dashboard
Target:  Port of Rotterdam, Maersk, DSV, Coolblue
GitHub:  https://github.com/owensamuel2929-ux/maritime-pipeline
```

---

## Elevator Pitch

> "Built a maritime supply chain intelligence platform monitoring live vessel traffic
> at the Port of Rotterdam — using real VesselAPI data, orchestrated by Airflow,
> transformed and validated with dbt, with cost controls for incremental loading
> and a live vessel map dashboard."

---

## Architecture

```
VesselAPI (live data)
        │
        ▼
Airflow DAG (every 6 hours)
  → incremental fetch: only new events since last run
  → port events (arrivals + departures at NLRTM)
  → vessel positions (live GPS per MMSI)
  → vessel emissions (when available)
        │
        ▼
PostgreSQL — raw schema
  → raw.port_events        (deduplicated on insert)
  → raw.vessel_positions   (latest snapshot, replaced each run)
  → raw.vessel_emissions
        │
        ▼
dbt — staging layer
  → filters null mmsi / event_type / timestamp
  → filters future timestamps
  → filters positions outside Rotterdam bounding box (51.8–52.0°N, 4.0–4.4°E)
        │
        ▼
dbt — mart layer (business-ready tables)
  → mart_vessels_in_port       (vessel name + live GPS — map-ready)
  → mart_port_congestion       (daily arrivals, departures, congestion level)
  → mart_hourly_arrivals       (traffic pattern by hour)
  → mart_carrier_performance   (per-vessel activity summary)
        │
        ▼
dbt test (28 tests)
  → error severity: mmsi, lat/lon, event_type, timestamp
  → warn severity:  imo, speed, heading (legitimately nullable)
        │
        ▼
HTML Dashboard (dashboard.html)
  → KPI cards, Leaflet vessel map, hourly chart, vessel table
```

---

## Stack

| Tool | Version | Purpose |
|---|---|---|
| Apache Airflow | 2.9.0 | Pipeline orchestration (every 6 hours) |
| PostgreSQL | 15 | Data storage — raw + transformed layers |
| dbt | 1.7.17 | SQL transformations + data validation |
| VesselAPI Python SDK | 1.3.0 | Live Rotterdam port data |
| Python | 3.11 | Extract, load, dashboard generation |
| Docker Compose | — | Full stack containerisation |
| SQLAlchemy | — | Database connection layer |
| Pandas | — | DataFrame manipulation |
| Leaflet.js + Chart.js | CDN | Interactive map + charts in dashboard |

---

## Project Structure

```
maritime-pipeline/
├── dags/
│   └── maritime_dag.py              ← Airflow DAG (6-task pipeline)
├── src/
│   ├── extract.py                   ← VesselAPI calls (incremental fetch)
│   ├── load.py                      ← PostgreSQL loader (dedup + upsert)
│   ├── dashboard.py                 ← HTML dashboard generator
│   └── notify.py                    ← Telegram alerts (optional)
├── dbt/
│   └── maritime/
│       ├── models/
│       │   ├── staging/
│       │   │   ├── schema.yml       ← staging tests (error + warn severity)
│       │   │   ├── stg_port_events.sql
│       │   │   ├── stg_vessel_positions.sql
│       │   │   └── stg_vessel_emissions.sql
│       │   └── marts/
│       │       ├── schema.yml       ← mart tests (error + warn severity)
│       │       ├── mart_vessels_in_port.sql
│       │       ├── mart_port_congestion.sql
│       │       ├── mart_hourly_arrivals.sql
│       │       └── mart_carrier_performance.sql
│       ├── tests/
│       │   └── schema.yml
│       ├── profiles.yml
│       └── dbt_project.yml
├── init/
│   └── 01_init.sql                  ← Creates schemas + databases on first run
├── Dockerfile                       ← Airflow + dbt + VesselAPI image
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Step 1 — Extract (Incremental)

```python
# src/extract.py
def fetch_port_events(port_code="NLRTM", days_back=30):
    """
    Incremental fetch: uses last stored timestamp as time_from
    so each run only pulls events newer than what's already in the DB.
    Falls back to days_back on first run.
    """
    last_ts = get_last_event_timestamp()
    if last_ts:
        time_from = last_ts          # only fetch new events
    else:
        time_from = (now - timedelta(days=days_back)).strftime(...)

    for event_type in ["arrival", "departure"]:
        next_token = None
        while True:
            response = client.port_events.list(
                filter_unlocode=port_code,
                filter_event_type=event_type,
                time_from=time_from,
                time_to=time_to,
                pagination_limit=50,
                pagination_next_token=next_token
            )
            # ... process events ...
            next_token = response.next_token
            if not next_token:
                break
```

---

## Step 2 — Load (Deduplicated)

```python
# src/load.py
def load_to_postgres(data: list, table: str):
    if table == "port_events":
        df = _dedup_port_events(df, engine)   # skip rows already in DB
    elif table == "vessel_positions":
        df.to_sql(..., if_exists="replace")    # latest snapshot only
        return

    df.to_sql(..., if_exists="append")

def _dedup_port_events(df, engine):
    """Remove rows where (mmsi, timestamp, event_type) already exist."""
    existing = pd.read_sql(
        "SELECT mmsi::text, timestamp, event_type FROM raw.port_events", engine
    )
    existing_keys = set(zip(existing["mmsi"], existing["timestamp"], existing["event_type"]))
    return df[~df.apply(
        lambda r: (str(r["mmsi"]), str(r["timestamp"]), r["event_type"]) in existing_keys,
        axis=1
    )]
```

---

## Step 3 — Airflow DAG

```python
# dags/maritime_dag.py
# Task flow:
# extract_port_events
#   ├── extract_vessel_positions
#   └── extract_vessel_emissions
#         └── dbt_staging → dbt_marts → dbt_test

# API quota handling: if VesselRateLimitError, logs warning and
# returns empty list — dbt still runs on existing data
def extract_and_load_events():
    try:
        events = fetch_port_events("NLRTM")
        load_to_postgres(events, "port_events")
        return [e["mmsi"] for e in events if e.get("mmsi")]
    except Exception as e:
        if "quota exceeded" in str(e).lower() or "429" in str(e):
            log.warning("API quota exceeded — skipping, dbt runs on existing data")
            return []
        raise
```

---

## Step 4 — dbt Staging (Data Filtering)

### `stg_port_events.sql`
```sql
SELECT vessel_name, mmsi, imo, event_type,
       port_name, port_unlocode, port_country,
       "timestamp"::timestamptz                    AS event_timestamp,
       DATE("timestamp"::timestamptz)              AS event_date,
       EXTRACT(HOUR FROM "timestamp"::timestamptz) AS event_hour,
       ingested_at
FROM raw.port_events
WHERE port_unlocode = 'NLRTM'
  AND mmsi IS NOT NULL          -- hard filter: no MMSI = useless
  AND event_type IS NOT NULL
  AND timestamp IS NOT NULL
  AND "timestamp"::timestamptz <= NOW()  -- reject future timestamps
```

### `stg_vessel_positions.sql`
```sql
SELECT mmsi, latitude, longitude, speed, heading,
       destination, eta::timestamptz AS eta, ingested_at
FROM raw.vessel_positions
WHERE mmsi IS NOT NULL
  AND latitude  IS NOT NULL
  AND longitude IS NOT NULL
  AND latitude  BETWEEN 51.8 AND 52.0   -- Rotterdam bounding box
  AND longitude BETWEEN 4.0  AND 4.4
```

---

## Step 5 — dbt Mart Models

### `mart_vessels_in_port.sql`
```sql
-- Latest event per vessel joined with live GPS position
WITH latest_events AS (
    SELECT DISTINCT ON (mmsi)
        vessel_name, mmsi, imo, event_type, event_timestamp, event_date
    FROM {{ ref('stg_port_events') }}
    ORDER BY mmsi, event_timestamp DESC
)
SELECT e.vessel_name, e.mmsi, e.imo, e.event_type,
       e.event_timestamp AS last_seen,
       p.latitude, p.longitude, p.heading, p.speed
FROM latest_events e
LEFT JOIN {{ ref('stg_vessel_positions') }} p ON e.mmsi::text = p.mmsi::text
WHERE p.latitude IS NOT NULL
```

### `mart_port_congestion.sql`
```sql
-- Daily arrivals, departures, vessels in port, congestion level
SELECT event_date,
       COUNT(*) FILTER (WHERE event_type = 'Arrival')   AS arrivals,
       COUNT(*) FILTER (WHERE event_type = 'Departure') AS departures,
       COUNT(*) FILTER (WHERE event_type = 'Arrival')
       - COUNT(*) FILTER (WHERE event_type = 'Departure') AS vessels_in_port,
       COUNT(DISTINCT mmsi) AS unique_vessels,
       CASE WHEN COUNT(*) > 50 THEN 'HIGH'
            WHEN COUNT(*) > 25 THEN 'MEDIUM'
            ELSE 'LOW' END AS congestion_level
FROM {{ ref('stg_port_events') }}
GROUP BY event_date
```

### `mart_hourly_arrivals.sql`
```sql
-- Traffic pattern by hour of day
SELECT event_hour,
       COUNT(*) AS vessel_count,
       COUNT(*) FILTER (WHERE event_type = 'Arrival')   AS arrivals,
       COUNT(*) FILTER (WHERE event_type = 'Departure') AS departures,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_day
FROM {{ ref('stg_port_events') }}
GROUP BY event_hour
```

---

## Step 6 — Data Validation (28 dbt Tests)

Two-layer validation approach:

**Layer 1 — Filter in staging** (bad rows never reach marts)
- Null `mmsi`, `event_type`, `timestamp` → excluded
- Future timestamps → excluded
- Positions outside Rotterdam bbox → excluded

**Layer 2 — dbt schema tests with severity levels**

```yaml
# models/staging/schema.yml
- name: stg_port_events
  columns:
    - name: mmsi
      tests:
        - not_null:
            severity: error    # hard fail — stops DAG
    - name: event_type
      tests:
        - accepted_values:
            values: ['Arrival', 'Departure']
            severity: error
    - name: imo
      tests:
        - not_null:
            severity: warn     # warn only — many vessels lack IMO
    - name: speed
      tests:
        - not_null:
            severity: warn     # warn only — null for anchored vessels
```

**Test results: 24 PASS / 4 WARN / 0 ERROR**
- Warnings are expected nulls (imo, speed, heading) — pipeline continues
- Errors would stop the DAG before bad data reaches marts

---

## Step 7 — Cost Controls

| Risk | Fix Applied |
|---|---|
| API quota exhausted by re-fetching known data | Incremental fetch: `time_from` = last stored timestamp |
| `raw.port_events` growing with duplicates | Dedup on `(mmsi, timestamp, event_type)` before insert |
| `vessel_positions` accumulating stale snapshots | `if_exists="replace"` — always keeps latest snapshot only |
| DAG fails when API quota exceeded | Try/except catches 429 — dbt still runs on existing data |

---

## Step 8 — Dashboard

Generated by `src/dashboard.py` — runs inside the Airflow container, produces a standalone `dashboard.html`.

```bash
docker exec <airflow-scheduler-id> python3 src/dashboard.py
# → open dashboard.html in browser
```

**Dashboard contents:**
- **4 KPI cards** — vessels in port, arrivals, departures, congestion level (color-coded)
- **Interactive Leaflet map** — all 60 vessels plotted on Rotterdam, click for MMSI/name/heading
- **Hourly traffic bar chart** — when the port is busiest (Chart.js)
- **Vessel activity table** — per-vessel event summary with timestamps

---

## What Makes This Portfolio-Worthy

| Feature | Why It Matters |
|---|---|
| Real live data (VesselAPI) | Not simulated — actual Rotterdam vessels |
| Incremental loading | Production cost control pattern |
| Storage deduplication | Prevents unbounded DB growth |
| Two-layer data validation | Staging filters + dbt tests with severity |
| Graceful quota handling | Pipeline never fully breaks |
| dbt marts for business insights | Stakeholder-ready output |
| Interactive vessel map dashboard | Visual proof the pipeline works |
| Airflow with XCom | MMSI list passed between tasks |
| Fully containerised | Reproducible anywhere with Docker |
| Clean GitHub repo | Portfolio-ready with README |

---

## Target Employers & Talking Points

| Company | Angle |
|---|---|
| Port of Rotterdam | "I monitored live Rotterdam traffic with congestion scoring" |
| Maersk / MSC | "I tracked vessel positions and carrier activity patterns" |
| DSV | "I built an incremental pipeline with deduplication and validation" |
| Coolblue | "I tracked inbound vessel ETAs for supply chain visibility" |

---

## Checklist

- [x] Sign up for VesselAPI
- [x] Docker Compose stack (Airflow + PostgreSQL)
- [x] Write `extract.py` — incremental VesselAPI fetch with pagination
- [x] Write `load.py` — deduplication + snapshot replace
- [x] Airflow DAG with XCom + quota error handling
- [x] dbt staging models with data filtering
- [x] dbt mart models (4 business-ready tables)
- [x] dbt tests — 28 tests, error + warn severity
- [x] HTML dashboard — map, charts, KPIs
- [x] Cost controls — incremental fetch + deduplication
- [x] Push to GitHub with README
- [ ] Telegram alerts
- [ ] Deploy to cloud VM
- [ ] CI/CD with GitHub Actions
