import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from vessel_api_python import VesselClient

client = VesselClient(api_key=os.getenv("VESSEL_API_KEY"))


def fetch_port_events(port_code="NLRTM", days_back=30):
    """Fetch arrivals + departures for the past N days, paginating through all results."""
    all_records = []
    now = datetime.now(timezone.utc)
    time_from = (now - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    time_to = now.strftime("%Y-%m-%dT%H:%M:%SZ")

    for event_type in ["arrival", "departure"]:
        next_token = None
        while True:
            kwargs = dict(
                filter_unlocode=port_code,
                filter_event_type=event_type,
                time_from=time_from,
                time_to=time_to,
                pagination_limit=50,
            )
            if next_token:
                kwargs["pagination_next_token"] = next_token

            response = client.port_events.list(**kwargs)
            events = response.port_events or []

            for e in events:
                all_records.append({
                    "vessel_name":   e.vessel.name if e.vessel else None,
                    "mmsi":          e.vessel.mmsi if e.vessel else None,
                    "imo":           e.vessel.imo if e.vessel else None,
                    "event_type":    e.event,
                    "port_name":     e.port.name if e.port else None,
                    "port_unlocode": e.port.unlo_code if e.port else None,
                    "port_country":  e.port.country if e.port else None,
                    "timestamp":     e.timestamp,
                    "ingested_at":   pd.Timestamp.now(),
                })

            next_token = response.next_token
            if not next_token or not events:
                break

    return all_records


def fetch_vessel_positions(mmsi_list):
    if not mmsi_list:
        return []

    records = []
    for mmsi in mmsi_list[:20]:
        try:
            resp = client.vessels.position(str(mmsi), filter_id_type="mmsi")
            p = resp.vessel_position if hasattr(resp, "vessel_position") else None
            if p:
                records.append({
                    "mmsi":        mmsi,
                    "latitude":    getattr(p, "latitude", None),
                    "longitude":   getattr(p, "longitude", None),
                    "speed":       getattr(p, "speed", None),
                    "heading":     getattr(p, "heading", None),
                    "destination": getattr(p, "destination", None),
                    "eta":         getattr(p, "eta", None),
                    "ingested_at": pd.Timestamp.now(),
                })
        except Exception:
            continue
    return records


def fetch_emissions(mmsi_list):
    records = []
    for mmsi in mmsi_list[:10]:
        try:
            resp = client.vessels.emissions(str(mmsi), filter_id_type="mmsi")
            for e in (getattr(resp, "emissions", None) or []):
                records.append({
                    "mmsi":              mmsi,
                    "reporting_period":  getattr(e, "reporting_period", None),
                    "co2_total":         getattr(e, "co2_emissions_total", None),
                    "fuel_consumed":     getattr(e, "fuel_consumed_total", None),
                    "distance_traveled": getattr(e, "distance_travelled", None),
                    "ingested_at":       pd.Timestamp.now(),
                })
        except Exception:
            continue
    return records
