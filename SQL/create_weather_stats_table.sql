-- Create the stats table to store yearly summary metrics
CREATE TABLE IF NOT EXISTS staging.weather_stats (
    station_id TEXT NOT NULL,
    year INT NOT NULL,
    avg_max_temp_celsius NUMERIC,
    avg_min_temp_celsius NUMERIC,
    total_precip_cm NUMERIC,
    PRIMARY KEY (station_id, year)
);