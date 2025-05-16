-- Create the database
CREATE DATABASE weather_db;
-- Create the schema
CREATE SCHEMA staging;

-- Create the weather table inside the staging schema
CREATE TABLE staging.weather (
    station_id TEXT NOT NULL,
    date DATE NOT NULL,
    max_temp INTEGER,
    min_temp INTEGER,
    precipitation INTEGER,
    PRIMARY KEY (station_id, date)
);
