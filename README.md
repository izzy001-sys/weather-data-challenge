# Weather Data Ingestion and Analysis

This project ingests historical weather data from local `.txt` files into a PostgreSQL database, calculates yearly statistics per station, and exposes a REST API for querying the results.


## Folder Structure

code-challenge-template

-wx_data/ # Raw weather data files
- sql/ # SQL scripts to create database, tables and to populate tables
- src/ # Virtual environment files and python scripts (logic) to execute the sql scripts
    - ingest_weather_data.py
    - pop_weather_stats.py
    - .env # DB credentials   
    - requirements.txt
- test/ # test.api.py 
- .gitignore
- README.md

## Setup
1. Clone the repository
2. Create a virtual environment and activate it
3. Install dependencies: `pip install -r requirements.txt`
4. Set up Postgres and create the database
5. Create .env file
6. Run the data ingestion
7. Run the weather stats calculation
8. Launch the API
9. Deployment plan

## API Reference

#### Get all items

```http
  GET /api/weather
```
#### Get item

```http
  GET /api/weather/stats
```

## Authors

- [@izzy001-sys](https://github.com/izzy001-sys)