"""
Generate a standalone HTML dashboard from the maritime mart tables.
Run inside the airflow container or locally with psycopg2 installed.
"""
import os
import json
import psycopg2
import psycopg2.extras
from datetime import datetime

DB_URL = os.getenv("DATABASE_URL", "postgresql://maritime:maritime_pass@localhost:5433/maritime")

# ── helpers ──────────────────────────────────────────────────────────────────

def connect():
    import re
    m = re.match(r"postgresql://(.+):(.+)@(.+):?(\d*)/(.+)", DB_URL)
    user, pwd, host, port, db = m.groups()
    return psycopg2.connect(host=host, port=port or 5432,
                            dbname=db, user=user, password=pwd)

def query(sql):
    with connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            return [dict(r) for r in cur.fetchall()]

# ── fetch data ────────────────────────────────────────────────────────────────

congestion   = query("SELECT * FROM dbt_marts.mart_port_congestion ORDER BY event_date DESC LIMIT 1")
hourly       = query("SELECT event_hour, vessel_count FROM dbt_marts.mart_hourly_arrivals ORDER BY event_hour")
vessels      = query("SELECT vessel_name, mmsi, imo, last_event, last_seen, latitude, longitude, heading FROM dbt_marts.mart_vessels_in_port ORDER BY last_seen DESC")
performance  = query("SELECT vessel_name, mmsi, total_events, arrival_count, departure_count, first_seen, last_seen FROM dbt_marts.mart_carrier_performance ORDER BY total_events DESC LIMIT 20")

cong = congestion[0] if congestion else {}
total_vessels   = cong.get("unique_vessels", 0)
arrivals_today  = cong.get("arrivals", 0)
departures_today = cong.get("departures", 0)
cong_level      = cong.get("congestion_level", "N/A")
cong_color      = {"HIGH": "#ef4444", "MEDIUM": "#f59e0b", "LOW": "#22c55e"}.get(cong_level, "#6b7280")

hours_labels = [str(r["event_hour"]) + ":00" for r in hourly]
hours_data   = [r["vessel_count"] for r in hourly]

map_vessels = [r for r in vessels if r["latitude"] and r["longitude"]]
map_json    = json.dumps([{
    "name": r["vessel_name"] or "Unknown",
    "mmsi": r["mmsi"],
    "lat":  float(r["latitude"]),
    "lon":  float(r["longitude"]),
    "heading": r["heading"] or 0,
    "event": r["last_event"],
} for r in map_vessels])

perf_rows = "".join(f"""
<tr>
  <td>{r['vessel_name'] or '—'}</td>
  <td><code>{r['mmsi']}</code></td>
  <td>{r['total_events']}</td>
  <td>{r['arrival_count']}</td>
  <td>{r['departure_count']}</td>
  <td>{str(r['last_seen'])[:16] if r['last_seen'] else '—'}</td>
</tr>""" for r in performance)

generated = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

# ── HTML ─────────────────────────────────────────────────────────────────────

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Rotterdam Port Intelligence Dashboard</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family:'Segoe UI',system-ui,sans-serif; background:#0f172a; color:#e2e8f0; }}
  header {{ background:#1e293b; border-bottom:1px solid #334155; padding:1.25rem 2rem; display:flex; justify-content:space-between; align-items:center; }}
  header h1 {{ font-size:1.4rem; font-weight:700; color:#38bdf8; letter-spacing:.5px; }}
  header span {{ font-size:.75rem; color:#64748b; }}
  .grid {{ display:grid; gap:1.25rem; padding:1.5rem 2rem; }}
  .kpis {{ display:grid; grid-template-columns:repeat(4,1fr); gap:1rem; }}
  .kpi {{ background:#1e293b; border:1px solid #334155; border-radius:.75rem; padding:1.25rem 1.5rem; }}
  .kpi .label {{ font-size:.7rem; text-transform:uppercase; letter-spacing:.08em; color:#64748b; margin-bottom:.5rem; }}
  .kpi .value {{ font-size:2.2rem; font-weight:700; line-height:1; }}
  .kpi .sub {{ font-size:.75rem; color:#94a3b8; margin-top:.4rem; }}
  .row2 {{ display:grid; grid-template-columns:1fr 1.6fr; gap:1.25rem; }}
  .card {{ background:#1e293b; border:1px solid #334155; border-radius:.75rem; padding:1.25rem; }}
  .card h2 {{ font-size:.85rem; font-weight:600; color:#94a3b8; text-transform:uppercase; letter-spacing:.06em; margin-bottom:1rem; }}
  #map {{ height:380px; border-radius:.5rem; }}
  .chart-wrap {{ height:300px; position:relative; }}
  table {{ width:100%; border-collapse:collapse; font-size:.8rem; }}
  th {{ text-align:left; padding:.6rem .8rem; color:#64748b; font-weight:600; font-size:.7rem; text-transform:uppercase; border-bottom:1px solid #334155; }}
  td {{ padding:.6rem .8rem; border-bottom:1px solid #1e293b; color:#cbd5e1; }}
  tr:hover td {{ background:#0f172a; }}
  code {{ background:#0f172a; padding:.1rem .4rem; border-radius:.25rem; font-size:.75rem; color:#38bdf8; }}
  .badge {{ display:inline-block; padding:.2rem .6rem; border-radius:9999px; font-size:.7rem; font-weight:600; background:{cong_color}22; color:{cong_color}; border:1px solid {cong_color}44; }}
</style>
</head>
<body>
<header>
  <h1>🚢 Rotterdam Port Intelligence</h1>
  <span>Generated {generated} &nbsp;|&nbsp; Port NLRTM</span>
</header>

<div class="grid">

  <!-- KPIs -->
  <div class="kpis">
    <div class="kpi">
      <div class="label">Vessels in Port</div>
      <div class="value" style="color:#38bdf8">{total_vessels}</div>
      <div class="sub">unique MMSIs today</div>
    </div>
    <div class="kpi">
      <div class="label">Arrivals Today</div>
      <div class="value" style="color:#22c55e">{arrivals_today}</div>
      <div class="sub">port entry events</div>
    </div>
    <div class="kpi">
      <div class="label">Departures Today</div>
      <div class="value" style="color:#f59e0b">{departures_today}</div>
      <div class="sub">port exit events</div>
    </div>
    <div class="kpi">
      <div class="label">Congestion Level</div>
      <div class="value" style="color:{cong_color};font-size:1.6rem;padding-top:.3rem">
        <span class="badge">{cong_level}</span>
      </div>
      <div class="sub">based on daily traffic</div>
    </div>
  </div>

  <!-- Map + Chart -->
  <div class="row2">
    <div class="card">
      <h2>Hourly Traffic Pattern</h2>
      <div class="chart-wrap">
        <canvas id="hourlyChart"></canvas>
      </div>
    </div>
    <div class="card">
      <h2>Live Vessel Positions — Rotterdam</h2>
      <div id="map"></div>
    </div>
  </div>

  <!-- Vessel Table -->
  <div class="card">
    <h2>Vessel Activity Log</h2>
    <table>
      <thead>
        <tr>
          <th>Vessel Name</th><th>MMSI</th><th>Events</th>
          <th>Arrivals</th><th>Departures</th><th>Last Seen</th>
        </tr>
      </thead>
      <tbody>{perf_rows}</tbody>
    </table>
  </div>

</div>

<script>
// ── Hourly Chart ──────────────────────────────────────────────────────────
new Chart(document.getElementById('hourlyChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(hours_labels)},
    datasets: [{{
      label: 'Vessels',
      data: {json.dumps(hours_data)},
      backgroundColor: '#38bdf844',
      borderColor: '#38bdf8',
      borderWidth: 2,
      borderRadius: 4,
    }}]
  }},
  options: {{
    responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ ticks: {{ color:'#94a3b8' }}, grid: {{ color:'#1e293b' }} }},
      y: {{ ticks: {{ color:'#94a3b8' }}, grid: {{ color:'#334155' }}, beginAtZero: true }}
    }}
  }}
}});

// ── Leaflet Map ───────────────────────────────────────────────────────────
const map = L.map('map').setView([51.92, 4.18], 12);
L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
  attribution: '&copy; OpenStreetMap &copy; CARTO', maxZoom: 18
}}).addTo(map);

const vessels = {map_json};
vessels.forEach(v => {{
  const marker = L.circleMarker([v.lat, v.lon], {{
    radius: 6, fillColor: '#38bdf8', color: '#0ea5e9',
    weight: 2, opacity: 1, fillOpacity: 0.8
  }}).addTo(map);
  marker.bindPopup(`
    <b>${{v.name}}</b><br>
    MMSI: ${{v.mmsi}}<br>
    Event: ${{v.event}}<br>
    Heading: ${{v.heading}}°<br>
    Lat: ${{v.lat.toFixed(5)}}, Lon: ${{v.lon.toFixed(5)}}
  `);
}});
</script>
</body>
</html>"""

out = os.path.join(os.path.dirname(__file__), "..", "dashboard.html")
with open(out, "w", encoding="utf-8") as f:
    f.write(html)

print(f"Dashboard saved to: {os.path.abspath(out)}")
print(f"Vessels on map: {len(map_vessels)}")
print(f"KPIs: arrivals={arrivals_today}, departures={departures_today}, congestion={cong_level}")
