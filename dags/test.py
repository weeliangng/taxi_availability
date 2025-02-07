from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2026, 3, 1),  # Make sure this is in the past
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)