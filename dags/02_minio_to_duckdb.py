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
DUCKDB_FILE = "/opt/airflow/db/lab.duckdb"
#
# SEEDS_DIR = "./dbt/seeds"
SEEDS_DIR = "./sample_data"


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
            # remote_path = f"seeds/{file}"
            remote_path = file
            print(f"THIS IS LOCAL PATH:{local_path}")
            print(f"THIS IS REMOTE PATH:{remote_path}")

            client.fput_object(
                MINIO_BUCKET,
                remote_path,
                local_path
            )
            print(f"Uploaded {local_path} → s3://{MINIO_BUCKET}/{remote_path}")



def setup_duckdb(**context):
    # Ensure directory exists
    os.makedirs(os.path.dirname(DUCKDB_FILE), exist_ok=True)

    # Connect to DuckDB (creates file if missing)
    con = duckdb.connect(DUCKDB_FILE)

    # Enable S3 / HTTPFS extension
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Configure S3 / MinIO
    con.execute(f"""
        SET s3_region='us-east-1';
        SET s3_url_style='path';
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
    """)

    con.close()
    print(f"DuckDB initialized at {DUCKDB_FILE} with MinIO settings.")


def load_minio_csvs_to_duckdb(**context):
    import duckdb
    from minio import Minio
    import os

    # Ensure DuckDB directory exists
    os.makedirs(os.path.dirname(DUCKDB_FILE), exist_ok=True)

    # Connect to DuckDB
    con = duckdb.connect(DUCKDB_FILE)

    # Install and load HTTPFS extension for S3/MinIO
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    # Configure MinIO S3 settings for DuckDB
    con.execute(f"""
        SET s3_region='us-east-1';
        SET s3_url_style='path';
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_use_ssl=false;
    """)

    # Connect to MinIO using the Python client
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    # List CSV files in the bucket
    for obj in client.list_objects(MINIO_BUCKET, recursive=True):
        if obj.object_name.endswith(".csv"):
            filename = os.path.basename(obj.object_name)
            table_name = filename.replace(".csv", "")
            csv_path = f"s3://{MINIO_BUCKET}/{obj.object_name}"

            # Load CSV into DuckDB table
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS
                SELECT * FROM read_csv_auto('{csv_path}');
            """)
            print(f"Loaded {csv_path} into DuckDB table `{table_name}`")

    # Show created tables
    tables = con.execute("SHOW TABLES;").fetchall()
    print("DuckDB tables:", tables)

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

    upload_seed_to_minio = PythonOperator(
        task_id="upload_csv_to_minio",
        python_callable=upload_csv_to_minio
    )

    duckdb_setup_task = PythonOperator(
        task_id="setup_duckdb",
        python_callable=setup_duckdb
    )

    load_csvs_task = PythonOperator(
        task_id="load_minio_csvs_to_duckdb",
        python_callable=load_minio_csvs_to_duckdb
    )


    upload_seed_to_minio >> duckdb_setup_task >> load_csvs_task #load_miniocsv_duckdb #duckdb_setup >> load_miniocsv_duckdb
