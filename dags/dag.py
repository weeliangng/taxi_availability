from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import sys
import os
import shutil

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../etl")))
import ingestion
import transformation

default_args = {
    'start_date': datetime(2025, 1, 1),  # Make sure this is in the past
}

def move_file(**kwargs):
    ti = kwargs['ti']
    filepath = ti.xcom_pull(task_ids="store_data")
    logical_date = kwargs.get("logical_date")
    filename = filepath.split("/")[-1]
    destination_folder = "data/processed/"
    destination_path = os.path.join(destination_folder, os.path.basename(filename))
    try:
        # Move the file from the source to the destination
        shutil.move(filepath, destination_path)
        print(f"Successfully moved {filepath} to {destination_path}")
    except Exception as e:
        print(f"Error moving file: {e}")


with DAG(
    'etl',
    default_args=default_args,
    schedule_interval = "0 9 * * *",
    catchup = False,
    start_date = pendulum.datetime(2025,1,1, tz="Asia/Singapore")
) as dag:
    ingestion_task = PythonOperator(
        task_id = "get_api_data",
        python_callable = ingestion.taxi_ingestion_pipeline,
    )
    transformation_task = PythonOperator(
        task_id = "store_data",
        python_callable = transformation.taxi_transformation_pipeline,
    )
    move_file_task = PythonOperator(
        task_id = "move_file",
        python_callable = move_file
    )
    ingestion_task >> transformation_task >> move_file_task