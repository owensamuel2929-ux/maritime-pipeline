WITH latest_events AS (
    SELECT DISTINCT ON (mmsi)
        vessel_name,
        mmsi,
        imo,
        event_type,
        event_timestamp,
        event_date
    FROM {{ ref('stg_port_events') }}
    ORDER BY mmsi, event_timestamp DESC
)

SELECT
    e.vessel_name,
    e.mmsi,
    e.imo,
    e.event_type       AS last_event,
    e.event_timestamp  AS last_seen,
    p.latitude,
    p.longitude,
    p.heading,
    p.speed
FROM latest_events e
LEFT JOIN {{ ref('stg_vessel_positions') }} p
    ON e.mmsi::text = p.mmsi::text
WHERE p.latitude IS NOT NULL
ORDER BY e.event_timestamp DESC
