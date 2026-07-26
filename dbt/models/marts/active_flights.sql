{{ config(
    materialized='table'
) }}

WITH raw_flights_data AS (
    SELECT
        icao24,
        callsign,
        origin_country,
        on_ground,
        velocity,
        snapshot_timestamp,
        DATE(TIMESTAMP_SECONDS(CAST(snapshot_timestamp AS INT64))) AS snapshot_date
    FROM {{ source('raw_flights', 'raw_states') }}
    WHERE snapshot_timestamp IS NOT NULL
)

SELECT
    origin_country,
    COUNT(DISTINCT icao24) AS total_planes,
    COUNT(DISTINCT CASE WHEN on_ground = FALSE THEN icao24 END) AS airborne_planes,
    ROUND(AVG(velocity), 2) AS avg_velocity_mps,
    CURRENT_TIMESTAMP() AS updated_at
FROM raw_flights_data
WHERE on_ground = FALSE OR on_ground IS NULL
GROUP BY origin_country
ORDER BY total_planes DESC
