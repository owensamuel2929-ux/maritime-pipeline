WITH events AS (
    SELECT * FROM {{ ref('stg_port_events') }}
),

vessel_summary AS (
    SELECT
        vessel_name,
        mmsi,
        imo,
        COUNT(*)                                                AS total_events,
        COUNT(*) FILTER (WHERE event_type = 'Arrival')         AS arrival_count,
        COUNT(*) FILTER (WHERE event_type = 'Departure')       AS departure_count,
        MIN(event_timestamp)                                    AS first_seen,
        MAX(event_timestamp)                                    AS last_seen
    FROM events
    GROUP BY vessel_name, mmsi, imo
)

SELECT *
FROM vessel_summary
ORDER BY total_events DESC, last_seen DESC
