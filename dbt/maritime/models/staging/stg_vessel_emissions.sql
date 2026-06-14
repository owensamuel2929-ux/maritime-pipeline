{{ config(enabled=false) }}

SELECT
    mmsi,
    reporting_period,
    co2_total,
    fuel_consumed,
    distance_traveled,
    ROUND(co2_total / NULLIF(distance_traveled, 0), 4) AS co2_per_km,
    ingested_at
FROM raw.vessel_emissions
WHERE co2_total IS NOT NULL
