from __future__ import annotations

import io
import pandas as pd
import requests
import json
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# ---- CONFIG ----
CSV_URL = "https://raw.githubusercontent.com/OttoKase/DataEngineeringProject/refs/heads/main/infrared_06-09.2025_ver2.%20csv"
POSTGRES_CONN_ID = "peopletraffic_db"
TARGET_TABLE = "DimBuilding"
COL_NAME = 'building_name'
FILTER_NAME = 'SuperCounter_ÜC_Tervisemaja1A'
DATA_DIR = "/tmp/data/people_avg"



# ---- TASK 1: FETCH CSV ----
def fetch_csv(**context):
    """
    Downloads the CSV from GitHub raw URL and pushes it to XCom as a CSV string.
    We store the whole CSV body (small/medium data OK). For very large data,
    you'd instead write to /tmp and pass the path.
    """
    resp = requests.get(CSV_URL)
    resp.raise_for_status()
    csv_text = resp.text

    # push raw CSV string to XCom
    context["ti"].xcom_push(key="raw_csv", value=csv_text)


# ---- TASK 2: INGEST INTO POSTGRES ----
def ingest_into_postgres(**context):
    """
    Reads CSV from XCom, parses it with pandas, then loads into Postgres table DimBuilding.
    Strategy here: simple append.
    Adjust if you need truncate/reload or merge logic.
    """
    ti = context["ti"]
    raw_csv = ti.xcom_pull(key="raw_csv", task_ids="fetch_csv")

    if not raw_csv:
        raise ValueError("No CSV data found in XCom from fetch_csv")

    # Parse CSV into DataFrame
    df = pd.read_csv(io.StringIO(raw_csv))

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    # Append data to DimBuilding
    # if_exists='append' means it will not drop/replace the table
    df.to_sql(
        TARGET_TABLE,
        engine,
        if_exists="append",
        index=False,
        method="multi",  # batch insert for speed
    )


# ---- TASK 3: CALCULATE AVERAGE and saves into DATA_DIR directory----
def calc_avg_for_name(**context):
    """
    Runs SELECT AVG("in") FROM DimBuilding WHERE name = FILTER_NAME.
    Logs result and pushes to XCom.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql = f"""
    SELECT AVG ("people_in") as avg_in
	FROM public."DimBuilding"
	WHERE building_name ='SuperCounter_ÜC_Tervisemaja1A';
    """

    records = hook.get_records(sql)#, parameters=[FILTER_NAME])

    # # get_records returns list of tuples; expect [(Decimal('123.45'),)]
    avg_value = records[0][0] if records and len(records[0]) > 0 else None

    # Log for task logs
    print(f'Average "people_in" for name="{FILTER_NAME}": {avg_value}')

    # Push to XCom so you can inspect in the Airflow UI
    context["ti"].xcom_push(key=f'avg_in_value: {FILTER_NAME}', value=str(avg_value) if avg_value is not None else None)

    # ! DID NOT WORK
    # Should save as .json; DID NOT WORK
    # dict_output[f'avg_in_value: {FILTER_NAME}'] = value=str(avg_value)
    # filename = os.path.join(DATA_DIR, f"foo.json")
    # os.makedirs(DATA_DIR, exist_ok=True)
    # with open(filename, "w") as f:
    #     json.dump(dict_output, f)


# ---- DAG DEFINITION ----
with DAG(
    dag_id="ingest_building_and_calc_avg",
    description="Fetch CSV, load into Postgres DimBuilding, then compute AVG(in) for a specific building",
    start_date=days_ago(1),
    schedule_interval=None,  # manual / triggered
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
    tags=["DimBuilding", "postgres", "csv", "analytics"],
) as dag:

    fetch_csv_task = PythonOperator(
        task_id="fetch_csv",
        python_callable=fetch_csv,
        provide_context=True,
    )

    ingest_task = PythonOperator(
        task_id="ingest_into_postgres",
        python_callable=ingest_into_postgres,
        provide_context=True,
    )

    avg_task = PythonOperator(
        task_id="calc_avg_for_name",
        python_callable=calc_avg_for_name,
        provide_context=True,
    )

    # dependency chain
    fetch_csv_task >> ingest_task >> avg_task

