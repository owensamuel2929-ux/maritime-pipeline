SELECT
    vessel_name,
    mmsi,
    imo,
    event_type,
    port_name,
    port_unlocode,
    port_country,
    "timestamp"::timestamptz                    AS event_timestamp,
    DATE("timestamp"::timestamptz)              AS event_date,
    EXTRACT(HOUR FROM "timestamp"::timestamptz) AS event_hour,
    ingested_at
FROM raw.port_events
WHERE port_unlocode = 'NLRTM'
  AND timestamp IS NOT NULL
