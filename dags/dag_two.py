from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# import sys
# import subprocess
# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'duckdb'])

import duckdb
from minio import Minio


# ───────────────────────────────────────────────────────────
# CONFIG
# ───────────────────────────────────────────────────────────

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "project-bucket"

SEEDS_DIR = "./dbt/seeds"
DUCKDB_FILE = "lab.duckdb"

# ───────────────────────────────────────────────────────────
# FUNCTIONS
# ───────────────────────────────────────────────────────────

def upload_csv_to_minio(**context):
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # Create bucket if not exists
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    for file in os.listdir(SEEDS_DIR):
        if file.endswith(".csv"):
            local_path = os.path.join(SEEDS_DIR, file)
            remote_path = f"seeds/{file}"

            client.fput_object(
                MINIO_BUCKET,
                remote_path,
                local_path
            )
            print(f"Uploaded {local_path} → s3://{MINIO_BUCKET}/{remote_path}")


def setup_duckdb(**context):
    con = duckdb.connect(DUCKDB_FILE)
    # Enable S3 extension
    conn.install_extension("httpfs")
    conn.load_extension("httpfs")

    # # Enable S3 extension
    # con.execute("INSTALL httpfs;")
    # con.execute("LOAD httpfs;")

    con.execute(f"""
        SET s3_region='us-east-1';
        SET s3_url_style='path';
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;

    """)

    con.close()
    print("DuckDB configured with MinIO S3 settings.")


def load_minio_csvs_to_duckdb(**context):
    con = duckdb.connect(DUCKDB_FILE)

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    objects = client.list_objects(MINIO_BUCKET, prefix="seeds/")

    for obj in objects:
        if obj.object_name.endswith(".csv"):
            filename = obj.object_name.split("/")[-1]
            table = filename.replace(".csv", "")

            csv_path = f"s3://{MINIO_BUCKET}/{obj.object_name}"

            con.execute(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT * FROM read_csv('{csv_path}', AUTO_DETECT=TRUE);
            """)

            print(f"Loaded {csv_path} into DuckDB table `{table}`")

    con.close()


# ───────────────────────────────────────────────────────────
# DAG DEFINITION
# ───────────────────────────────────────────────────────────

with DAG(
    dag_id="minio_to_duckdb_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Uploads csv → MinIO → DuckDB pipeline"
) as dag:

    upload_csv = PythonOperator(
        task_id="upload_csv_to_minio",
        python_callable=upload_csv_to_minio
    )

    duckdb_setup = PythonOperator(
        task_id="setup_duckdb",
        python_callable=setup_duckdb
    )

    load_duckdb = PythonOperator(
        task_id="load_minio_csvs_to_duckdb",
        python_callable=load_minio_csvs_to_duckdb
    )

    upload_csv >> duckdb_setup >> load_duckdb
