-- populate weather stats (fact_weather_stats) table

INSERT INTO fact_weather_stats (
    station_id,
    year_id,
    avg_max_temp_celsius,
    avg_min_temp_celsius,
    total_precip_cm
)
SELECT
    fw.station_id,
    y.year_id,
    ROUND(AVG(NULLIF(fw.max_temp, -9999)) / 10.0, 2) AS avg_max_temp,
    ROUND(AVG(NULLIF(fw.min_temp, -9999)) / 10.0, 2) AS avg_min_temp,
    ROUND(SUM(NULLIF(fw.precipitation, -9999)) / 100.0, 2) AS total_precip
FROM fact_weather fw
JOIN dim_date d ON fw.date_id = d.date_id
JOIN dim_year y ON y.year = EXTRACT(YEAR FROM d.full_date)
GROUP BY fw.station_id, y.year_id
ON CONFLICT (station_id, year_id) DO NOTHING;

