import os
import psycopg2
from datetime import datetime
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 5432)
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "wx_data")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT
        )
        logging.info("Connected to database successfully.")
        return conn
    except Exception as e:
        logging.error(f"DB connection error: {e}")
        raise

def parse_weather_file(filepath, station_id):
    with open(filepath, 'r') as file:
        for line in file:
            parts = line.strip().split('\t')
            if len(parts) != 4:
                continue
            date_str, max_temp, min_temp, precipitation = parts
            try:
                date_obj = datetime.strptime(date_str, "%Y%m%d")
                yield (
                    station_id,
                    date_obj.date(),
                    int(max_temp),
                    int(min_temp),
                    int(precipitation)
                )
            except ValueError:
                continue

def ensure_date(cur, full_date):
    year = full_date.year
    month = full_date.month
    day = full_date.day
    cur.execute("""
        INSERT INTO dim_date (full_date, year, month, day)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (full_date) DO NOTHING
        RETURNING date_id;
    """, (full_date, year, month, day))
    result = cur.fetchone()
    if result:
        return result[0]
    cur.execute("SELECT date_id FROM dim_date WHERE full_date = %s", (full_date,))
    return cur.fetchone()[0]

def ensure_station(cur, station_id):
    cur.execute("SELECT station_id FROM dim_station WHERE station_id = %s", (station_id,))
    if not cur.fetchone():
        cur.execute("""
            INSERT INTO dim_station (station_id)
            VALUES (%s)
            ON CONFLICT DO NOTHING
        """, (station_id,))

def ingest_weather_data():
    start_time = datetime.now()
    logging.info("Starting weather ingestion...")

    conn = create_connection()
    cur = conn.cursor()
    total_inserted = 0

    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".txt"):
            station_id = os.path.splitext(filename)[0]
            filepath = os.path.join(DATA_DIR, filename)
            logging.info(f"Processing {filename}")

            ensure_station(cur, station_id)
            file_inserts = 0

            for station_id, full_date, max_temp, min_temp, precip in parse_weather_file(filepath, station_id):
                try:
                    date_id = ensure_date(cur, full_date)
                    cur.execute("""
                        INSERT INTO fact_weather (station_id, date_id, max_temp, min_temp, precipitation)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (station_id, date_id) DO NOTHING
                    """, (station_id, date_id, max_temp, min_temp, precip))
                    file_inserts += 1
                except Exception as e:
                    logging.warning(f"Insert error: {e}")
                    continue

            logging.info(f"Inserted {file_inserts} records from {filename}")
            total_inserted += file_inserts

    conn.commit()
    cur.close()
    conn.close()

    logging.info(f"Ingestion complete. Total records: {total_inserted}. Duration: {datetime.now() - start_time}")

if __name__ == "__main__":
    ingest_weather_data()
