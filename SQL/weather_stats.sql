-- Insert yearly weather stats for new (station_id, year) combination
INSERT INTO staging.weather_stats (station_id, year, avg_max_temp_celsius, avg_min_temp_celsius, total_precip_cm)
SELECT
    station_id,
    EXTRACT(YEAR FROM date)::INT AS year,
    AVG(NULLIF(max_temp, -9999)) / 10.0 AS avg_max_temp_celsius,
    AVG(NULLIF(min_temp, -9999)) / 10.0 AS avg_min_temp_celsius,
    SUM(NULLIF(precipitation, -9999)) / 100.0 AS total_precip_cm
FROM
    staging.weather
GROUP BY
    station_id, EXTRACT(YEAR FROM date)
HAVING 
    NOT EXISTS (
        SELECT 1 FROM staging.weather_stats ws
        WHERE ws.station_id = weather.station_id
          AND ws.year = EXTRACT(YEAR FROM weather.date)
    )
ORDER BY
    station_id, year;
