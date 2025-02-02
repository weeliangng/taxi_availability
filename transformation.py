import psycopg
import json
from shapely.geometry import shape

def run_query(query, fetch_results = False):
    with psycopg.connect(dbname='taxi_availability', user='myuser', password='mypassword', host='localhost', port='5432') as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            if fetch_results:
                try:
                    results = cur.fetchall()
                    return results
                except:
                    print("Query does not produce any results")

def taxi_transformation_pipeline(date_str):
    query = """
    CREATE TABLE IF NOT EXISTS taxi_availability (
    timestamp timestamptz PRIMARY KEY,
    geom GEOMETRY(MultiPoint, 4326),
    taxi_count integer);  
            """
    run_query(query, True)
    filename = "taxi_availability_{}.json".format(date_str.replace("-","_"))
    with open("data/raw/taxi_availability_2024_12_31.json", "r") as file:
        taxi_availability_list = json.load(file)
    conn = psycopg.connect(dbname='taxi_availability', user='myuser', password='mypassword', host='localhost', port='5432')
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
