from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

import sys
import subprocess
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'meteostat'])

from meteostat import Point, Hourly
import pandas as pd
import os

# CONFIG
LOCATION = Point(59.4133, 24.8328)  # Tallinn
START_DATE = datetime(2025, 6, 2)
END_DATE = datetime(2025, 9, 26)
OUTPUT_DIR = '/opt/airflow/data'

def fetch_weather_data():
    data = Hourly(LOCATION, START_DATE, END_DATE)
    data = data.fetch()

    if not data.empty:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_file = os.path.join(OUTPUT_DIR, f'weather_{START_DATE.date()}_{END_DATE.date()}.csv')
        return data.to_csv(output_file)
        print(f"Saved weather data to {output_file}")
    else:
        print("qqqqq")
        raise ValueError("No data fetched")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='meteostat_weather_dag',
    default_args=default_args,
    schedule_interval='@once',  #None,
    catchup=False,
    description='Fetch weather data using meteostat without API key'
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )
    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command=" echo 'inaccurate'"
    )

    fetch_task >> inaccurate