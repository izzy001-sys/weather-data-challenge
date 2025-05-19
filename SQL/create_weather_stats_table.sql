-- Create the stats tables
CREATE TABLE dim_year (
    year_id SERIAL PRIMARY KEY,
    year INT NOT NULL UNIQUE
);


CREATE TABLE fact_weather_stats (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(10) NOT NULL,
    year_id INT NOT NULL,
    avg_max_temp_celsius DECIMAL(5,2),
    avg_min_temp_celsius DECIMAL(5,2),
    total_precip_cm DECIMAL(6,2),
    FOREIGN KEY (station_id) REFERENCES dim_station(station_id),
    FOREIGN KEY (year_id) REFERENCES dim_year(year_id),
    UNIQUE (station_id, year_id)
);
