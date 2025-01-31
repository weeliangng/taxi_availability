import requests
from datetime import datetime, timedelta
import json

raw_data_folder = "data/raw/"

def get_datetime_intervals(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    minute_intervals = [date_obj + timedelta(minutes=i+1) for i in range(24*60)]
    minute_intervals_str = [i.strftime("%Y-%m-%dT%H:%M:%S") for i in minute_intervals]
    return minute_intervals_str

def get_taxi_availability_timestamp(timestamp):
    url = "https://api.data.gov.sg/v1/transport/taxi-availability"
    payload = {'date_time' : timestamp}
    response = requests.get(url, params = payload)
    if response.status_code == 200:
        data = response.json()
        return data
    else: print("API failed to respond")


def taxi_availability_date(date_str):
    taxi_availability = list()
    minute_intervals = get_datetime_intervals(date_str)
    for timestamp in minute_intervals:
        data = get_taxi_availability_timestamp(timestamp)
        taxi_availability.append(data)
    return taxi_availability

def save_taxi_availability(date_str, data):
    file_path = raw_data_folder + "taxi_availability_{}.json".format(date_str.replace("-","_"))
    with open(file_path, "w") as f:
        json.dump(data, f, indent = 2)

def taxi_ingestion_pipeline(date_str):
    data = taxi_availability_date(date_str)
    save_taxi_availability(date_str, data)