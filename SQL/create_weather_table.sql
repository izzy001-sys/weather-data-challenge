

-- Create the weather tables
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    CHECK (
        EXTRACT(YEAR FROM full_date) = year AND
        EXTRACT(MONTH FROM full_date) = month AND
        EXTRACT(DAY FROM full_date) = day
    )
);

CREATE TABLE dim_station (
    station_id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(255),
    state VARCHAR(50),
    latitude DECIMAL(8,5),
    longitude DECIMAL(8,5)
);

CREATE TABLE fact_weather (
    id SERIAL PRIMARY KEY,
    station_id VARCHAR(10) NOT NULL,
    date_id INT NOT NULL,
    max_temp DECIMAL(5,1),
    min_temp DECIMAL(5,1),
    precipitation DECIMAL(5,1),
    FOREIGN KEY (station_id) REFERENCES dim_station(station_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    UNIQUE (station_id, date_id)
);
