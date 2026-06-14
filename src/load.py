import os
import pandas as pd
from sqlalchemy import create_engine, text


def load_to_postgres(data: list, table: str):
    if not data:
        print(f"No data to load for {table}")
        return

    df = pd.DataFrame(data)
    engine = create_engine(os.getenv("DATABASE_URL"))

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))

    df.to_sql(
        table,
        engine,
        schema="raw",
        if_exists="append",
        index=False
    )
    print(f"Loaded {len(df)} rows to raw.{table}")
