from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import BooleanType, IntegerType, LongType, FloatType, DoubleType, StringType, TimestampType, TimestamptzType
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from clickhouse_connect import Client  # pip install clickhouse-connect

# CONFIG
DUCKDB_FILE = "/opt/airflow/db/lab.duckdb"
CATALOG_NAME = "rest"
CATALOG_URI = "http://iceberg_rest:8181"
ICEBERG_NAMESPACE = "default"
ICEBERG_TABLE = "bronze_infrared"  # Fixed table name

CLICKHOUSE_HOST = "clickhouse-server"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASS = ""
CLICKHOUSE_DB = "default"  # Target DB where Iceberg engine is enabled

# -----------------------
# HELPER FUNCTIONS
# -----------------------
def arrow_type_to_iceberg(pa_type):
    if pa.types.is_boolean(pa_type):
        return BooleanType()
    if pa.types.is_int32(pa_type):
        return IntegerType()
    if pa.types.is_int64(pa_type):
        return LongType()
    if pa.types.is_float32(pa_type):
        return FloatType()
    if pa.types.is_float64(pa_type):
        return DoubleType()
    if pa.types.is_timestamp(pa_type):
        return TimestamptzType() if pa_type.tz else TimestampType()
    return StringType()

def cast_arrow_to_iceberg_schema(arrow_table: pa.Table, iceberg_schema: Schema) -> pa.Table:
    arrays = []
    fields = []
    for field in iceberg_schema.fields:
        col = arrow_table[field.name]
        t = field.field_type
        if isinstance(t, IntegerType):
            target_type = pa.int32()
        elif isinstance(t, LongType):
            target_type = pa.int64()
        elif isinstance(t, FloatType):
            target_type = pa.float32()
        elif isinstance(t, DoubleType):
            target_type = pa.float64()
        elif isinstance(t, TimestampType):
            target_type = pa.timestamp("us")
        elif isinstance(t, TimestamptzType):
            target_type = pa.timestamp("us", tz="UTC")
        elif isinstance(t, BooleanType):
            target_type = pa.bool_()
        else:
            target_type = pa.string()
        arrays.append(pc.cast(col, target_type))
        fields.append(pa.field(field.name, target_type))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

# -----------------------
# TASKS
# -----------------------
def create_iceberg_namespace(**context):
    catalog = load_catalog(CATALOG_NAME, uri=CATALOG_URI)
    try:
        catalog.create_namespace(ICEBERG_NAMESPACE)
        print(f"Created namespace {ICEBERG_NAMESPACE}")
    except NamespaceAlreadyExistsError:
        print(f"Namespace exists, continuing: {ICEBERG_NAMESPACE}")

def create_and_append_iceberg(**context):
    catalog = load_catalog(CATALOG_NAME, uri=CATALOG_URI)
    identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE}"

    con = duckdb.connect(DUCKDB_FILE)
    arrow_table = con.execute(f"SELECT * FROM {ICEBERG_TABLE}").arrow()
    con.close()
    print(f"Fetched {ICEBERG_TABLE} from DuckDB")

    iceberg_fields = [NestedField(idx + 1, f.name, arrow_type_to_iceberg(f.type), required=False)
                      for idx, f in enumerate(arrow_table.schema)]
    iceberg_schema = Schema(*iceberg_fields)

    try:
        table = catalog.create_table(identifier, iceberg_schema)
        print(f"Created Iceberg table: {identifier}")
    except TableAlreadyExistsError:
        table = catalog.load_table(identifier)
        print(f"Loaded existing Iceberg table: {identifier}")

    arrow_table = cast_arrow_to_iceberg_schema(arrow_table, table.schema())
    table.append(arrow_table)
    print(f"Appended {len(arrow_table)} rows to Iceberg table {identifier}")

    context["ti"].xcom_push(key="iceberg_table", value=identifier)

def show_iceberg_tables(**context):
    catalog = load_catalog(CATALOG_NAME, uri=CATALOG_URI)
    tables = catalog.list_tables(ICEBERG_NAMESPACE)
    print(f"Iceberg tables in namespace {ICEBERG_NAMESPACE}: {tables}")
    context["ti"].xcom_push(key="iceberg_tables", value=[str(t) for t in tables])

def register_iceberg_in_clickhouse(**context):
    iceberg_table = context["ti"].xcom_pull(key="iceberg_table")
    if not iceberg_table:
        raise ValueError("No Iceberg table found in XCom")

    namespace, table_name = iceberg_table.split(".")
    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                    username=CLICKHOUSE_USER, password=CLICKHOUSE_PASS,
                    database=CLICKHOUSE_DB)

    # Create Iceberg table in ClickHouse if not exists
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{namespace}_{table_name}
    ENGINE = Iceberg('{CATALOG_NAME}', '{namespace}', '{table_name}');
    """
    client.command(create_query)
    print(f"Registered Iceberg table in ClickHouse: {namespace}_{table_name}")

    # Optionally fetch sample
    sample = client.query(f"SELECT * FROM {CLICKHOUSE_DB}.{namespace}_{table_name} LIMIT 10").result_rows
    print(f"Sample rows from ClickHouse: {sample}")
    context["ti"].xcom_push(key="clickhouse_sample", value=sample)

# -----------------------
# DAG DEFINITION
# -----------------------
with DAG(
    dag_id="duckdb_to_iceberg_clickhouse",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    create_ns = PythonOperator(
        task_id="create_iceberg_namespace",
        python_callable=create_iceberg_namespace
    )

    create_iceberg = PythonOperator(
        task_id="create_and_append_iceberg",
        python_callable=create_and_append_iceberg
    )

    show_tables = PythonOperator(
        task_id="show_iceberg_tables",
        python_callable=show_iceberg_tables
    )

    register_ch = PythonOperator(
        task_id="register_iceberg_clickhouse",
        python_callable=register_iceberg_in_clickhouse
    )

    # DAG dependencies
    create_ns >> create_iceberg >> show_tables >> register_ch
