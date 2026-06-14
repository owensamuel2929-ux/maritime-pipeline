SELECT
    mmsi,
    latitude,
    longitude,
    speed,
    heading,
    destination,
    eta::timestamptz AS eta,
    ingested_at
FROM raw.vessel_positions
WHERE latitude IS NOT NULL
  AND longitude IS NOT NULL
