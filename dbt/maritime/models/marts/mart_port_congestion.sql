WITH events AS (
    SELECT * FROM {{ ref('stg_port_events') }}
),

daily_traffic AS (
    SELECT
        event_date,
        COUNT(*)                                                    AS total_events,
        COUNT(*) FILTER (WHERE event_type = 'Arrival')             AS arrivals,
        COUNT(*) FILTER (WHERE event_type = 'Departure')           AS departures,
        COUNT(DISTINCT mmsi)                                        AS unique_vessels,
        COUNT(*) FILTER (WHERE event_type = 'Arrival')
        - COUNT(*) FILTER (WHERE event_type = 'Departure')         AS vessels_in_port,
        CASE
            WHEN COUNT(*) > 50 THEN 'HIGH'
            WHEN COUNT(*) > 25 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS congestion_level
    FROM events
    GROUP BY event_date
)

SELECT * FROM daily_traffic
ORDER BY event_date DESC
