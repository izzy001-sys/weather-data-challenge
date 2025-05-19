import os
from flask import Flask, jsonify, request
from flasgger import Swagger
from dotenv import load_dotenv
import psycopg2

# Load environment variables
load_dotenv()

app = Flask(__name__)
swagger = Swagger(app)

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
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
      - name: date
        in: query
        type: string
        description: Format YYYY-MM-DD
        required: false
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
        description: Raw weather records with date and station info
    """
    station_id = request.args.get("station_id")
    date = request.args.get("date")
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    offset = (page - 1) * per_page

    query = """
        SELECT w.station_id, d.full_date, w.max_temp, w.min_temp, w.precipitation
        FROM fact_weather w
        JOIN dim_date d ON w.date_id = d.date_id
        WHERE 1=1
    """
    params = []

    if station_id:
        query += " AND w.station_id = %s"
        params.append(station_id)
    if date:
        query += " AND d.full_date = %s"
        params.append(date)

    query += " ORDER BY d.full_date LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        data = [
            {
                "station_id": row[0],
                "date": row[1].isoformat(),
                "max_temp_tenths_c": row[2],
                "min_temp_tenths_c": row[3],
                "precip_tenths_mm": row[4]
            } for row in rows
        ]

        return jsonify({"page": page, "per_page": per_page, "data": data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()


@app.route("/api/weather/stats", methods=["GET"])
def get_weather_stats():
    """
    Get aggregated yearly weather stats
    ---
    parameters:
      - name: station_id
        in: query
        type: string
        required: false
      - name: year
        in: query
        type: integer
        required: false
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
        description: Yearly aggregated stats
    """
    station_id = request.args.get("station_id")
    year = request.args.get("year", type=int)
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    offset = (page - 1) * per_page

    query = """
        SELECT f.station_id, y.year, f.avg_max_temp, f.avg_min_temp, f.total_precip_cm
        FROM fact_weather_stats f
        JOIN dim_year y ON f.year_id = y.year_id
        WHERE 1=1
    """
    params = []

    if station_id:
        query += " AND f.station_id = %s"
        params.append(station_id)
    if year:
        query += " AND y.year = %s"
        params.append(year)

    query += " ORDER BY f.station_id, y.year LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        data = [
            {
                "station_id": row[0],
                "year": row[1],
                "avg_max_temp_c": row[2],
                "avg_min_temp_c": row[3],
                "total_precip_cm": row[4]
            } for row in rows
        ]

        return jsonify({"page": page, "per_page": per_page, "data": data})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    app.run(debug=True)
