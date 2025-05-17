from dotenv import load_dotenv
import os
import psycopg2
from datetime import datetime

# Load variables from .env
load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

DATA_DIR = "./code-challenge-template/wx_data"


def ingest_weather_data():
    start_time = datetime.now()
    total_inserted = 0

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for filename in os.listdir(DATA_DIR):
        if not filename.endswith(".txt"):
            continue

        station_id = filename.replace(".txt", "")

        with open(os.path.join(DATA_DIR, filename), "r") as f:
            for line in f:
                parts = line.strip().split("\t")
                if len(parts) != 4:
                    continue

                date_str, max_temp, min_temp, precip = parts

                # Skip lines with all -9999
                if all(val == "-9999" for val in [max_temp, min_temp, precip]):
                    continue

                try:
                    cur.execute(
                        """
                        INSERT INTO weather (station_id, date, max_temp, min_temp, precipitation)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (station_id, date) DO NOTHING
                    """,
                        (
                            station_id,
                            datetime.strptime(date_str, "%Y%m%d").date(),
                            int(max_temp) if max_temp != "-9999" else None,
                            int(min_temp) if min_temp != "-9999" else None,
                            int(precip) if precip != "-9999" else None,
                        ),
                    )
                    total_inserted += cur.rowcount
                except Exception as e:
                    print(f"Error inserting line: {line.strip()} -> {e}")

    conn.commit()
    cur.close()
    conn.close()

    end_time = datetime.now()
    print(f"Ingestion started at: {start_time}")
    print(f"Ingestion ended at:   {end_time}")
    print(f"Total records ingested: {total_inserted}")


if __name__ == "__main__":
    ingest_weather_data()
