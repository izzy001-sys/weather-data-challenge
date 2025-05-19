import os
import psycopg2
from dotenv import load_dotenv

# Load DB credentials from .env
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def run_sql_script(filename):
    sql_path = os.path.join(os.path.dirname(__file__), "..", "sql", filename)
    with open(sql_path, "r") as f:
        sql = f.read()

    try:
        with psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
                print(f"✅ Successfully executed: {filename}")
    except Exception as e:
        print(f"❌ Error executing {filename}: {e}")

if __name__ == "__main__":
    # Populate dim_year from distinct years in dim_date
    run_sql_script("insert_years.sql")

    # Calculate and populate fact_weather_stats
    run_sql_script("populate_weather_stats.sql")
