import os
import pandas as pd
from sqlalchemy import create_engine, text


def _engine():
    return create_engine(os.getenv("DATABASE_URL"))


def load_to_postgres(data: list, table: str):
    if not data:
        print(f"No data to load for {table}")
        return

    df = pd.DataFrame(data)
    engine = _engine()

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))

    if table == "port_events":
        df = _dedup_port_events(df, engine)
    elif table == "vessel_positions":
        # Positions are a snapshot — replace so each vessel has one current row
        df.to_sql(table, engine, schema="raw", if_exists="replace", index=False)
        print(f"Replaced raw.{table} with {len(df)} rows (latest snapshot)")
        return

    if df.empty:
        print(f"No new rows to load for {table} (all duplicates)")
        return

    df.to_sql(table, engine, schema="raw", if_exists="append", index=False)
    print(f"Loaded {len(df)} new rows to raw.{table}")


def _dedup_port_events(df: pd.DataFrame, engine) -> pd.DataFrame:
    """Remove rows that already exist in raw.port_events (by mmsi + timestamp + event_type)."""
    try:
        existing = pd.read_sql(
            "SELECT mmsi::text AS mmsi, timestamp, event_type FROM raw.port_events",
            engine
        )
        existing_keys = set(
            zip(existing["mmsi"].astype(str),
                existing["timestamp"].astype(str),
                existing["event_type"])
        )
        before = len(df)
        df = df[~df.apply(
            lambda r: (str(r["mmsi"]), str(r["timestamp"]), r["event_type"]) in existing_keys,
            axis=1
        )]
        dupes = before - len(df)
        if dupes:
            print(f"Skipped {dupes} duplicate port_events rows")
    except Exception:
        pass  # Table doesn't exist yet — first run, load everything
    return df


def get_last_event_timestamp() -> str | None:
    """Return the most recent event timestamp already in raw.port_events."""
    try:
        result = pd.read_sql(
            'SELECT MAX("timestamp") AS last_ts FROM raw.port_events',
            _engine()
        )
        ts = result["last_ts"].iloc[0]
        return pd.Timestamp(ts).strftime("%Y-%m-%dT%H:%M:%SZ") if ts else None
    except Exception:
        return None
