SELECT
    event_hour,
    COUNT(*)                                                    AS vessel_count,
    COUNT(*) FILTER (WHERE event_type = 'Arrival')             AS arrivals,
    COUNT(*) FILTER (WHERE event_type = 'Departure')           AS departures,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)         AS pct_of_day
FROM {{ ref('stg_port_events') }}
GROUP BY event_hour
ORDER BY event_hour
