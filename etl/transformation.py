import psycopg
import json
from shapely.geometry import shape
import os
from datetime import timedelta

POSTGRES_USER = os.getenv("POSTGRES_USER", "myuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mypassword")
POSTGRES_DB = os.getenv("POSTGRES_DB", "taxi_availability")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")  # Change to 'etl-db' in Docker
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")  # Change to 5432 in Docker

def db_connection():
    return psycopg.connect(dbname= POSTGRES_DB , user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST, port=POSTGRES_PORT)

def run_query(query, fetch_results = False):
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            if fetch_results:
                try:
                    results = cur.fetchall()
                    return results
                except:
                    print("Query does not produce any results")

def taxi_transformation_pipeline(**context):
    logical_date = context["logical_date"]
    date_str = logical_date.strftime("%Y-%m-%d")
    query = """
    CREATE TABLE IF NOT EXISTS taxi_availability (
    timestamp timestamptz PRIMARY KEY,
    geom GEOMETRY(MultiPoint, 4326),
    taxi_count integer);  
            """
    run_query(query, True)
    filename = "taxi_availability_{}.json".format(date_str.replace("-","_"))
    filepath = "data/raw/" + filename
    with open( filepath , "r") as file:
        taxi_availability_list = json.load(file)
    conn = db_connection()
    cur = conn.cursor()
    insert_query = "INSERT INTO taxi_availability (timestamp, geom, taxi_count) VALUES (%s, ST_GeomFromText(%s, 4326), %s) \
                    ON CONFLICT (timestamp) DO NOTHING;"
    for taxi_availability in taxi_availability_list:
        geometry = shape(taxi_availability["features"][0]["geometry"])
        timestamp = taxi_availability["features"][0]["properties"]["timestamp"]
        taxi_count = taxi_availability["features"][0]["properties"]["taxi_count"]
        cur.execute(insert_query, (timestamp, geometry.wkt, taxi_count))
    conn.commit()
    cur.close()
    conn.close()
    return filepath
