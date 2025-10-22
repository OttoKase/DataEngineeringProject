from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
import subprocess
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'meteostat'])

from meteostat import Point, Daily
import pandas as pd
import os

# CONFIG
LOCATION = Point(52.52, 13.405)  # Berlin
START_DATE = datetime(2023, 10, 1)
END_DATE = datetime(2023, 10, 7)
OUTPUT_DIR = '/opt/airflow/data'

def fetch_weather_data():
    data = Daily(LOCATION, START_DATE, END_DATE)
    data = data.fetch()

    if not data.empty:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_file = os.path.join(OUTPUT_DIR, f'weather_{START_DATE.date()}_{END_DATE.date()}.csv')
        data.to_csv(output_file)
        print(f"Saved weather data to {output_file}")
    else:
        raise ValueError("No data fetched")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 10, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='meteostat_weather_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Fetch weather data using meteostat without API key'
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    fetch_task