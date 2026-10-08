"""
Canada weather ETL: Code 2's structure (separate tasks, hooks, connections)
combined with Code 1's multi-city loop.

Uses these Airflow connections:
  open_meteo_api    (HTTP, host https://api.open-meteo.com)
  postgres_default  (your weather database)
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "open_meteo_api"

# Largest Canadian cities plus every provincial/territorial capital
CANADA_CITIES = [
    {"name": "Toronto", "province": "ON", "lat": 43.6532, "lon": -79.3832},
    {"name": "Montreal", "province": "QC", "lat": 45.5019, "lon": -73.5674},
    {"name": "Vancouver", "province": "BC", "lat": 49.2827, "lon": -123.1207},
    {"name": "Calgary", "province": "AB", "lat": 51.0447, "lon": -114.0719},
    {"name": "Edmonton", "province": "AB", "lat": 53.5461, "lon": -113.4938},
    {"name": "Ottawa", "province": "ON", "lat": 45.4215, "lon": -75.6972},
    {"name": "Winnipeg", "province": "MB", "lat": 49.8951, "lon": -97.1384},
    {"name": "Mississauga", "province": "ON", "lat": 43.5890, "lon": -79.6441},
    {"name": "Quebec City", "province": "QC", "lat": 46.8139, "lon": -71.2080},
    {"name": "Hamilton", "province": "ON", "lat": 43.2557, "lon": -79.8711},
    {"name": "Kitchener", "province": "ON", "lat": 43.4516, "lon": -80.4925},
    {"name": "London", "province": "ON", "lat": 42.9849, "lon": -81.2453},
    {"name": "Windsor", "province": "ON", "lat": 42.3149, "lon": -83.0364},
    {"name": "Halifax", "province": "NS", "lat": 44.6488, "lon": -63.5752},
    {"name": "Victoria", "province": "BC", "lat": 48.4284, "lon": -123.3656},
    {"name": "Kelowna", "province": "BC", "lat": 49.8880, "lon": -119.4960},
    {"name": "Saskatoon", "province": "SK", "lat": 52.1332, "lon": -106.6700},
    {"name": "Regina", "province": "SK", "lat": 50.4452, "lon": -104.6189},
    {"name": "St. John's", "province": "NL", "lat": 47.5615, "lon": -52.7126},
    {"name": "Moncton", "province": "NB", "lat": 46.0878, "lon": -64.7782},
    {"name": "Fredericton", "province": "NB", "lat": 45.9636, "lon": -66.6431},
    {"name": "Charlottetown", "province": "PE", "lat": 46.2382, "lon": -63.1311},
    {"name": "Whitehorse", "province": "YT", "lat": 60.7212, "lon": -135.0568},
    {"name": "Yellowknife", "province": "NT", "lat": 62.4540, "lon": -114.3718},
    {"name": "Iqaluit", "province": "NU", "lat": 63.7467, "lon": -68.5170},
]

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

## DAG
with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    @task()
    def extract_weather_data():
        """Extract current weather for every city from the Open-Meteo API."""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")
        results = []

        for city in CANADA_CITIES:
            endpoint = (
                f"/v1/forecast?latitude={city['lat']}"
                f"&longitude={city['lon']}&current_weather=true"
            )
            # HttpHook raises an error on a failed request, so a bad
            # response fails the task instead of being silently skipped.
            response = http_hook.run(endpoint)
            results.append({"city": city, "data": response.json()})

        return results

    @task()
    def transform_weather_data(raw_results):
        """Keep only the fields we store, one row per city."""
        rows = []
        for item in raw_results:
            city = item["city"]
            current = item["data"]["current_weather"]
            rows.append(
                {
                    "city": city["name"],
                    "province": city["province"],
                    "latitude": city["lat"],
                    "longitude": city["lon"],
                    "temperature": current["temperature"],
                    "windspeed": current["windspeed"],
                    "winddirection": current["winddirection"],
                    "weathercode": current["weathercode"],
                }
            )
        return rows

    @task()
    def load_weather_data(rows):
        """Load all rows into PostgreSQL in a single transaction."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        try:
            # Create table if it doesn't exist
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_canada (
                city TEXT,
                province TEXT,
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)

            # Insert one row per city
            for row in rows:
                cursor.execute("""
                INSERT INTO weather_canada
                    (city, province, latitude, longitude, temperature,
                     windspeed, winddirection, weathercode)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row["city"],
                    row["province"],
                    row["latitude"],
                    row["longitude"],
                    row["temperature"],
                    row["windspeed"],
                    row["winddirection"],
                    row["weathercode"],
                ))

            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()

    ## DAG Workflow - ETL Pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)