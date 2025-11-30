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

OUTPUT_DIR = '/opt/airflow/sample_data'
# Path to data files
CSV_PATH = 'https://raw.githubusercontent.com/OttoKase/DataEngineeringProject/refs/heads/project_3test/resources/infrared_06-09.2025.%20csv'

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

    df = pd.read_csv(cp)
    print(f"CSV data loaded, rows: {len(df)}")


    # Idempotency: save CSV overwrite for that date
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
    schedule_interval='@daily', #'*/5 * * * *', #'@once', #'@continuous', #'@hourly',
    #max_active_runs=1,
    catchup=False,
    description='Fetch infrared-sensor, weather data. Compile dbt project.'
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data
    )

    ingest_infrared_csv_task = PythonOperator(
        task_id="ingest_infrared_csv",
        python_callable=ingest_csv,
        op_kwargs={"cp":CSV_PATH,"filename":"bronze_infrared"},
        provide_context=True
    )

    run_create_tables = BashOperator(
        task_id="run_create_tables",
        bash_command="docker exec clickhouse-server clickhouse-client --multiquery --queries-file=/sql/01_create_DB_and_tables.sql",
    )

    run_load_queries = BashOperator(
        task_id="run_load_queries",
        bash_command="docker exec clickhouse-server clickhouse-client --multiquery --queries-file=/sql/02_load_data_from_csv.sql",
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command="docker exec dbt dbt run",
    )

    fetch_task >> ingest_infrared_csv_task >>  run_create_tables  >> run_load_queries >> run_dbt

