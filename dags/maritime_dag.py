from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import logging

from src.extract import fetch_port_events, fetch_vessel_positions, fetch_emissions
from src.load import load_to_postgres

DBT_DIR = "/opt/dbt/maritime"
DBT_CMD = f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
DBT_TEST = f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"

log = logging.getLogger(__name__)


def extract_and_load_events():
    try:
        events = fetch_port_events("NLRTM")
        load_to_postgres(events, "port_events")
        return [e["mmsi"] for e in events if e.get("mmsi")]
    except Exception as e:
        if "quota exceeded" in str(e).lower() or "429" in str(e):
            log.warning("API quota exceeded — skipping extraction, dbt will run on existing data.")
            return []
        raise


def extract_and_load_positions(**context):
    mmsi_list = context["ti"].xcom_pull(task_ids="extract_port_events") or []
    if not mmsi_list:
        return
    try:
        load_to_postgres(fetch_vessel_positions(mmsi_list), "vessel_positions")
    except Exception as e:
        if "quota exceeded" in str(e).lower() or "429" in str(e):
            log.warning("API quota exceeded — skipping vessel positions.")
        else:
            raise


def extract_and_load_emissions(**context):
    mmsi_list = context["ti"].xcom_pull(task_ids="extract_port_events") or []
    if not mmsi_list:
        return
    try:
        load_to_postgres(fetch_emissions(mmsi_list), "vessel_emissions")
    except Exception as e:
        if "quota exceeded" in str(e).lower() or "429" in str(e):
            log.warning("API quota exceeded — skipping emissions.")
        else:
            raise


with DAG(
    "maritime_pipeline",
    schedule="0 */6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["maritime", "rotterdam", "portfolio"]
) as dag:

    extract_events = PythonOperator(
        task_id="extract_port_events",
        python_callable=extract_and_load_events
    )

    extract_positions = PythonOperator(
        task_id="extract_vessel_positions",
        python_callable=extract_and_load_positions
    )

    extract_emission = PythonOperator(
        task_id="extract_vessel_emissions",
        python_callable=extract_and_load_emissions
    )

    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command=f"{DBT_CMD} --select staging"
    )

    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command=f"{DBT_CMD} --select marts"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_TEST
    )

    extract_events >> [extract_positions, extract_emission] >> dbt_staging >> dbt_marts >> dbt_test
