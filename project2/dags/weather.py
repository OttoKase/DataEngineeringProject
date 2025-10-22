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
CSV_PATH = 'https://raw.githubusercontent.com/OttoKase/DataEngineeringProject/main/infrared_06-09.2025.%20csv'

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

def ingest_csv(execution_date_str: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df = pd.read_csv(CSV_PATH)
    print(f"CSV data loaded, rows: {len(df)}")

    ## Filter data for the execution_date to simulate parameterization (if 'date' col exists)
    #if 'date' in df.columns:
    #    df = df[df['date'] == execution_date_str]

    ## Data quality checks - Null check in 'value' column
    #if df['value'].isnull().any():
    #    raise ValueError("Null values found in 'value' column!")

    ## Remove duplicates by 'id'
    #df = df.drop_duplicates(subset=['id'])

    # Idempotency: save CSV overwrite for that date
    output_file = os.path.join(OUTPUT_DIR, f'infrared_csv_data_{execution_date_str}.csv')
    df.to_csv(output_file, index=False)
    print(f"CSV data saved to {output_file}")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='meteostat_weather_dag',
    default_args=default_args,
    schedule_interval='@hourly',  #'@once',
    catchup=False,
    description='Fetch weather data using meteostat without API key'
) as dag:
    def _ingest_csv(**context):
        exec_date = context['ds']  # yyyy-mm-dd string of execution date
        ingest_csv(exec_date)

    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    ingest_csv_task = PythonOperator(
        task_id="ingest_csv",
        python_callable=_ingest_csv,
        provide_context=True
    )

    fetch_task >> ingest_csv_task