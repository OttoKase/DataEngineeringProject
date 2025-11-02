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

# Config for weather data api (static, since mobility data is also static and from the same period)
LOCATION = Point(59.4133, 24.8328)  # Tallinn
START_DATE = datetime(2025, 6, 2)
END_DATE = datetime(2025, 9, 26)
# Docker data directory
OUTPUT_DIR = '/opt/airflow/dbt/seeds'
# Path to data files
CSV_PATH = 'https://raw.githubusercontent.com/OttoKase/DataEngineeringProject/main/infrared_06-09.2025.%20csv'
CSV_PATH2 = 'https://raw.githubusercontent.com/OttoKase/DataEngineeringProject/main/mobility_06-09.2025.%20csv'

def fetch_weather_data(sd = START_DATE, ed = END_DATE):
    # Fetches weather data and saves it into data/weather*.csv
    data = Hourly(LOCATION, START_DATE, END_DATE)
    data = data.fetch()

    if not data.empty:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        #output_file = os.path.join(OUTPUT_DIR, f'weather_{sd.date()}_{ed.date()}.csv')
        output_file = os.path.join(OUTPUT_DIR, f'bronze_weather.csv')
        return data.to_csv(output_file)
        print(f"Saved weather data to {output_file}")
    else:
        raise ValueError("No data fetched")

def ingest_csv(cp: str, filename: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    #execution_date_str = context['ds']  # yyyy-mm-dd string of execution date

    df = pd.read_csv(cp)
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
    #output_file = os.path.join(OUTPUT_DIR, f'{filename}_{execution_date_str}.csv')
    output_file = os.path.join(OUTPUT_DIR, f'{filename}.csv')
    df.to_csv(output_file, index=False)
    print(f"CSV data saved to {output_file}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='fetch_data',
    default_args=default_args,
    schedule_interval= '*/5 * * * *', #'@once', #'@continuous', #'@hourly',
    #max_active_runs=1,
    catchup=False,
    description='Fetch weather data using meteostat without API key, fetch mobility and infrared data'
) as dag:
    #def _ingest_csv(**context):
    #    exec_date = context['ds']  # yyyy-mm-dd string of execution date
    #    ingest_csv(exec_date,CSV_PATH,)

    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    ingest_infrared_csv_task = PythonOperator(
        task_id="ingest_infrared_csv",
        python_callable=ingest_csv,
        #op_kwargs={"cp":CSV_PATH,"filename":"infrared_06-09_2025"},
        op_kwargs={"cp":CSV_PATH,"filename":"bronze_infrared"},
        provide_context=True
    )

    ingest_mobility_csv_task = PythonOperator(
        task_id="ingest_mobility_csv",
        python_callable=ingest_csv,
        #op_kwargs={"cp":CSV_PATH2,"filename":"mobility_06-09_2025"},
        op_kwargs={"cp":CSV_PATH2,"filename":"bronze_mobility"},
        provide_context=True
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="docker exec dbt dbt seed",
    )

    fetch_task >> ingest_infrared_csv_task >> ingest_mobility_csv_task >> run_dbt