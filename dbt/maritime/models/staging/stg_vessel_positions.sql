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
WHERE mmsi IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND latitude  BETWEEN 51.8 AND 52.0
  AND longitude BETWEEN 4.0  AND 4.4
