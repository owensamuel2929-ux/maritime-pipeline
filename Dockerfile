FROM apache/airflow:2.9.0-python3.11

USER root
RUN apt-get update && apt-get install -y git curl libpq-dev gcc && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
