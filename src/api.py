from flask import Flask, jsonify, request
from flasgger import Swagger
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
swagger = Swagger(app)  # Initialize Swagger here

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

@app.route("/api/weather", methods=["GET"])
def get_weather():
    """
    Get raw weather data
    ---
    parameters:
      - name: station_id
        in: query
        type: string
        required: false
        description: Filter by station ID
      - name: date
        in: query
        type: string
        required: false
        description: Filter by date in YYYYMMDD format
      - name: page
        in: query
        type: integer
        default: 1
      - name: per_page
        in: query
        type: integer
        default: 50
    responses:
      200:
        description: Raw weather data records
    """
    station_id = request.args.get("station_id")
    date = request.args.get("date")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    offset = (page - 1) * per_page

    query = """
        SELECT station_id, date, max_temp, min_temp, precipitation
        FROM staging.weather
        WHERE 1=1
    """
    params = []

    if station_id:
        query += " AND station_id = %s"
        params.append(station_id)

    if date:
        query += " AND date = %s"
        params.append(date)

    query += " ORDER BY station_id, date LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        results = [
            {
                "station_id": row[0],
                "date": row[1].strftime("%Y%m%d") if row[1] else None,
                "max_temp_tenths_c": row[2],
                "min_temp_tenths_c": row[3],
                "precip_tenths_mm": row[4]
            }
            for row in rows
        ]

        return jsonify({
            "page": page,
            "per_page": per_page,
            "data": results
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route("/api/weather/stats", methods=["GET"])
def get_weather_stats():
    """
    Get yearly weather statistics
    ---
    parameters:
      - name: station_id
        in: query
        type: string
        required: false
        description: Filter by station ID
      - name: year
        in: query
        type: integer
        required: false
        description: Filter by year
      - name: page
        in: query
        type: integer
        default: 1
      - name: per_page
        in: query
        type: integer
        default: 50
    responses:
      200:
        description: List of yearly weather statistics
    """
    station_id = request.args.get("station_id")
    year = request.args.get("year", type=int)
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    offset = (page - 1) * per_page

    query = """
        SELECT station_id, year, avg_max_temp, avg_min_temp, total_precip_cm
        FROM staging.weather_stats
        WHERE 1=1
    """
    params = []

    if station_id:
        query += " AND station_id = %s"
        params.append(station_id)

    if year:
        query += " AND year = %s"
        params.append(year)

    query += " ORDER BY station_id, year LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        results = [
            {
                "station_id": row[0],
                "year": row[1],
                "avg_max_temp_c": row[2],
                "avg_min_temp_c": row[3],
                "total_precip_cm": row[4]
            }
            for row in rows
        ]

        return jsonify({
            "page": page,
            "per_page": per_page,
            "data": results
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    app.run(debug=True)
