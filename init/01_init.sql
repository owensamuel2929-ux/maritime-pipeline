-- Create raw schema in the maritime database (runs on first postgres startup)
CREATE SCHEMA IF NOT EXISTS raw;

-- Separate databases for Airflow metadata and Metabase metadata
CREATE DATABASE airflow;
CREATE DATABASE metabase;
