{{ config(enabled=false) }}

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
        CASE
            WHEN co2_per_km < 0.05 THEN 'EXCELLENT'
            WHEN co2_per_km < 0.10 THEN 'GOOD'
            WHEN co2_per_km < 0.20 THEN 'AVERAGE'
            ELSE 'HIGH_EMITTER'
        END AS emissions_grade,
        RANK() OVER (
            PARTITION BY reporting_period
            ORDER BY co2_per_km ASC
        ) AS efficiency_rank
    FROM emissions
)

SELECT * FROM carbon_summary
ORDER BY reporting_period DESC, co2_per_km ASC
