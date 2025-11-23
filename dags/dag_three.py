from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    StringType,
    TimestampType,
    TimestamptzType,
)
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

# CONFIG
DUCKDB_FILE = "/opt/airflow/db/lab.duckdb"
CATALOG_NAME = "rest"
CATALOG_URI = "http://iceberg_rest:8181"
ICEBERG_NAMESPACE = "default"
ICEBERG_TABLE = "bronze_infrared"  # Fixed table name

# HELPER FUNCTIONS
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
        if pa_type.tz:
            return TimestamptzType()
        else:
            return TimestampType()
    return StringType()

def cast_arrow_to_iceberg_schema(arrow_table: pa.Table, iceberg_schema: Schema) -> pa.Table:
    cast_arrays = []
    cast_fields = []

    for field in iceberg_schema.fields:
        col = arrow_table[field.name]
        iceberg_type = field.field_type

        if isinstance(iceberg_type, IntegerType):
            target_type = pa.int32()
        elif isinstance(iceberg_type, LongType):
            target_type = pa.int64()
        elif isinstance(iceberg_type, FloatType):
            target_type = pa.float32()
        elif isinstance(iceberg_type, DoubleType):
            target_type = pa.float64()
        elif isinstance(iceberg_type, TimestampType):
            target_type = pa.timestamp("ns")
        elif isinstance(iceberg_type, TimestamptzType):
            target_type = pa.timestamp("ns", tz="UTC")
        elif isinstance(iceberg_type, BooleanType):
            target_type = pa.bool_()
        else:
            target_type = pa.string()

        cast_arrays.append(pc.cast(col, target_type))
        cast_fields.append(pa.field(field.name, target_type))

    new_schema = pa.schema(cast_fields)
    return pa.Table.from_arrays(cast_arrays, schema=new_schema)

# TASKS
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

    # Fetch DuckDB table as Arrow
    con = duckdb.connect(DUCKDB_FILE)
    arrow_table = con.execute(f"SELECT * FROM {ICEBERG_TABLE}").arrow()
    con.close()
    print(f"Fetched {ICEBERG_TABLE} from DuckDB")

    # Build Iceberg schema
    iceberg_fields = []
    for idx, field in enumerate(arrow_table.schema, start=1):
        iceberg_type = arrow_type_to_iceberg(field.type)
        iceberg_fields.append(NestedField(field_id=idx, name=field.name, type=iceberg_type, required=False))
    iceberg_schema = Schema(*iceberg_fields)

    # Create or load Iceberg table
    try:
        table = catalog.create_table(identifier, iceberg_schema)
        print(f"Created Iceberg table: {identifier}")
    except TableAlreadyExistsError:
        table = catalog.load_table(identifier)
        print(f"Loaded existing Iceberg table: {identifier}")

    # Cast and append
    arrow_table = cast_arrow_to_iceberg_schema(arrow_table, table.schema())

    for col in arrow_table.schema.names:
        field = arrow_table.schema.field(col)
        if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
            arrow_table = arrow_table.set_column(
                arrow_table.schema.get_field_index(col),
                col,
                arrow_table[col].cast(pa.timestamp('us'))
            )
    table.append(arrow_table)
    print(f"Appended {len(arrow_table)} rows to Iceberg table {identifier}")

    # Push table name to XCom
    context["ti"].xcom_push(key="iceberg_table", value=identifier)


def show_iceberg_tables(**context):
    catalog = load_catalog(CATALOG_NAME, uri=CATALOG_URI)
    tables = catalog.list_tables(ICEBERG_NAMESPACE)  # get all tables in namespace
    print("Iceberg tables in namespace", ICEBERG_NAMESPACE, ":", tables)

    # Push the list of Iceberg tables to XCom
    context["ti"].xcom_push(key="iceberg_tables", value=[str(t) for t in tables])


# DAG
with DAG(
    dag_id="duckdb_to_iceberg_bronze",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    create_ns_load_icecatalog = PythonOperator(
        task_id="create_iceberg_namespace",
        python_callable=create_iceberg_namespace,
    )

    create_iceberg_data_append = PythonOperator(
        task_id="create_and_append_iceberg",
        python_callable=create_and_append_iceberg,
    )

    show_icetables = PythonOperator(
        task_id="show_iceberg_tables",
        python_callable=show_iceberg_tables,
    )


    create_ns_load_icecatalog >> create_iceberg_data_append >> show_icetables


#
# PythonOperator DAG task to register/query Iceberg in ClickHouse
# from airflow.operators.python import PythonOperator
# from clickhouse_connect import Client
#
# def register_and_query_iceberg_clickhouse(**context):
#     """
#     Connects ClickHouse server and ensures Iceberg table is queryable.
#     """
#     iceberg_tables = context["ti"].xcom_pull(key="iceberg_tables")
#     print("Iceberg tables from XCom:", iceberg_tables)
#
#     # Connect to ClickHouse
#     client = Client(
#         host='clickhouse-server',
#         username='${CLICKHOUSE_USER}',
#         password='${CLICKHOUSE_PASS}',
#         database='peopletraffic',  # default DB
#         port=8123
#     )
#
#     # Loop over Iceberg tables and ensure queryable
#     for table_id in iceberg_tables:
#         namespace, table_name = eval(table_id)  # Convert string tuple to Python tuple
#         full_table_name = f"{namespace}_{table_name}"  # e.g., default_bronze_infrared
#
#         # Create ClickHouse Iceberg table if it doesn't exist
#         create_query = f"""
#         CREATE TABLE IF NOT EXISTS peopletraffic.{full_table_name} ENGINE = Iceberg('rest', '{namespace}', '{table_name}');
#         """
#         client.command(create_query)
#         print(f"Registered Iceberg table in ClickHouse: {full_table_name}")
#
#     # Example: run a sample query (optional)
#     sample_table = iceberg_tables[0]
#     ns, tname = eval(sample_table)
#     query = f"SELECT * FROM peopletraffic.{ns}_{tname} LIMIT 10"
#     rows = client.query(query).result_rows
#     print(f"Sample rows from ClickHouse Iceberg table {ns}_{tname}: {rows}")
#
#     # Push sample to XCom
#     context['ti'].xcom_push(key='iceberg_table_data_ch', value=rows)


#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import pyarrow as pa
# import pyarrow.compute as pc
# import duckdb
#
# from pyiceberg.catalog import load_catalog
# from pyiceberg.schema import Schema, NestedField
# from pyiceberg.types import (
#     BooleanType,
#     IntegerType,
#     LongType,
#     FloatType,
#     DoubleType,
#     StringType,
#     TimestampType,
#     TimestamptzType,
# )
# from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
#
#
#
# # For only one Iceberg table: FACT
# # -------------------------------------------------------------------
# # CONFIG
# # -------------------------------------------------------------------
# DUCKDB_FILE = "/opt/airflow/db/lab.duckdb"
#
# CATALOG_NAME = "rest"
# CATALOG_URI = "http://iceberg_rest:8181"
# ICEBERG_NAMESPACE = "default"
# ICEBERG_TABLE = "duckdb_export"
#
# # -------------------------------------------------------------------
# # 1. DETECT DUCKDB TABLES
# # -------------------------------------------------------------------
# def detect_duckdb_tables(**context):
#     con = duckdb.connect(DUCKDB_FILE)
#     tables = con.execute("SHOW TABLES;").fetchall()
#     con.close()
#
#     table_list = [t[0] for t in tables]
#     print("Detected DuckDB tables:", table_list)
#
#     context["ti"].xcom_push(key="duckdb_tables", value=table_list)
#
# # -------------------------------------------------------------------
# # ARROW → ICEBERG TYPE MAPPING
# # -------------------------------------------------------------------
# def arrow_type_to_iceberg(pa_type):
#     if pa.types.is_boolean(pa_type):
#         return BooleanType()
#
#     if pa.types.is_int32(pa_type):
#         return IntegerType()
#     if pa.types.is_int64(pa_type):
#         return LongType()
#
#     if pa.types.is_float32(pa_type):
#         return FloatType()
#     if pa.types.is_float64(pa_type):
#         return DoubleType()
#
#     if pa.types.is_timestamp(pa_type):
#         if pa_type.tz:
#             return TimestamptzType()
#         else:
#             return TimestampType()
#
#     # fallback
#     return StringType()
#
# # -------------------------------------------------------------------
# # ARROW CASTING TO MATCH ICEBERG SCHEMA
# # -------------------------------------------------------------------
# def cast_arrow_to_iceberg_schema(arrow_table: pa.Table, iceberg_schema: Schema) -> pa.Table:
#     cast_arrays = []
#     cast_fields = []
#
#     for field in iceberg_schema.fields:
#         col = arrow_table[field.name]
#         iceberg_type = field.field_type
#
#         # Mapping iceberg type → arrow type
#         if isinstance(iceberg_type, IntegerType):
#             target_type = pa.int32()
#         elif isinstance(iceberg_type, LongType):
#             target_type = pa.int64()
#         elif isinstance(iceberg_type, FloatType):
#             target_type = pa.float32()
#         elif isinstance(iceberg_type, DoubleType):
#             target_type = pa.float64()
#         elif isinstance(iceberg_type, TimestampType):
#             target_type = pa.timestamp("ns")
#         elif isinstance(iceberg_type, TimestamptzType):
#             target_type = pa.timestamp("ns", tz="UTC")
#         elif isinstance(iceberg_type, BooleanType):
#             target_type = pa.bool_()
#         else:
#             target_type = pa.string()
#
#         cast_arrays.append(pc.cast(col, target_type))
#         cast_fields.append(pa.field(field.name, target_type))
#
#     new_schema = pa.schema(cast_fields)
#     return pa.Table.from_arrays(cast_arrays, schema=new_schema)
#
# # -------------------------------------------------------------------
# # 2. CREATE ICEBERG NAMESPACE
# # -------------------------------------------------------------------
# def create_iceberg_namespace(**context):
#     catalog = load_catalog(CATALOG_NAME, uri=CATALOG_URI)
#
#     try:
#         catalog.create_namespace(ICEBERG_NAMESPACE)
#         print(f"Created namespace {ICEBERG_NAMESPACE}")
#     except NamespaceAlreadyExistsError:
#         print(f"Namespace exists, continuing: {ICEBERG_NAMESPACE}")
#
# # -------------------------------------------------------------------
# # 3. CREATE ICEBERG TABLE + APPEND DUCKDB DATA
# # -------------------------------------------------------------------
# def create_and_append_iceberg(**context):
#     catalog = load_catalog(CATALOG_NAME, uri=CATALOG_URI)
#
#     # Get detected DuckDB table list
#     tables = context["ti"].xcom_pull(key="duckdb_tables")
#     if not tables:
#         raise Exception("No DuckDB tables found")
#
#     table_name = tables[0]
#     identifier = f"{ICEBERG_NAMESPACE}.{table_name}"
#
#     # Fetch data from DuckDB as PyArrow Table
#     con = duckdb.connect(DUCKDB_FILE)
#     arrow_table = con.execute(f"SELECT * FROM {table_name}").arrow()
#     con.close()
#     print(f"Fetched {table_name} from DuckDB")
#
#     # Build Iceberg schema from Arrow schema
#     iceberg_fields = []
#     for idx, field in enumerate(arrow_table.schema, start=1):
#         iceberg_type = arrow_type_to_iceberg(field.type)
#
#         iceberg_fields.append(
#             NestedField(
#                 field_id=idx,
#                 name=field.name,
#                 type=iceberg_type,
#                 required=False,
#             )
#         )
#
#     iceberg_schema = Schema(*iceberg_fields)
#
#     # Create Iceberg table or load existing
#     try:
#         table = catalog.create_table(identifier, iceberg_schema)
#         print(f"Created Iceberg table: {identifier}")
#     except TableAlreadyExistsError:
#         table = catalog.load_table(identifier)
#         print(f"Loaded existing table: {identifier}")
#
#     # Cast Arrow table → Iceberg Schema
#     arrow_table = cast_arrow_to_iceberg_schema(arrow_table, table.schema())
#
#
#     # Append records
#     table.append(arrow_table)
#     print(f"Appended {len(arrow_table)} rows to Iceberg table {identifier}")
#
#
# def show_iceberg_tables(**context):
#
#     catalog = load_catalog(CATALOG_NAME, uri=CATALOG_URI)
#     tables = catalog.list_tables(ICEBERG_NAMESPACE)
#
#     print("Iceberg tables in namespace", ICEBERG_NAMESPACE, ":", tables)
#
#     # Push table list to XCom
#     context["ti"].xcom_push(key="iceberg_tables", value=tables)
#
# # -------------------------------------------------------------------
# # DAG DEFINITION
# # -------------------------------------------------------------------
# with DAG(
#     dag_id="duckdb_to_iceberg_dynamic",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:
#
#     detect_duckdb_tbls = PythonOperator(
#         task_id="detect_duckdb_tables",
#         python_callable=detect_duckdb_tables,
#     )
#
#     create_ns_load_icecatalog = PythonOperator(
#         task_id="create_iceberg_namespace",
#         python_callable=create_iceberg_namespace,
#     )
#
#     create_iceberg_data_append = PythonOperator(
#         task_id="create_and_append_iceberg",
#         python_callable=create_and_append_iceberg,
#     )
#
#     show_icetables = PythonOperator(
#     task_id="show_iceberg_tables",
#     python_callable=show_iceberg_tables
#     )
#
#     detect_duckdb_tbls >> create_ns_load_icecatalog >> create_iceberg_data_append >> show_icetables


#
#
#
#
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import pyarrow.compute as pc
# import duckdb
# import pyarrow as pa
# from pyiceberg.catalog import load_catalog
# from pyiceberg.exceptions import NamespaceAlreadyExistsError
# from pyiceberg.exceptions import TableAlreadyExistsError
# from pyiceberg.schema import NestedField
# from pyiceberg.schema import Schema
# # from pyiceberg.table import NestedField
#
# from pyiceberg.types import (
#     BooleanType,
#     IntegerType,
#     LongType,
#     FloatType,
#     DoubleType,
#     StringType,
#     TimestampType,
#     TimestamptzType,
# )
#
#
# DUCKDB_FILE = "/opt/airflow/db/lab.duckdb"
#
# CATALOG_NAME = "rest"
# CATALOG_URI = "http://iceberg_rest:8181"
# ICEBERG_NAMESPACE = "default"
# ICEBERG_TABLE = "duckdb_export"
#
#
# # -------------------------------------------------
# # 1. Detect DuckDB tables
# # -------------------------------------------------
# def detect_duckdb_tables(**context):
#     con = duckdb.connect(DUCKDB_FILE)
#     tables = con.execute("SHOW TABLES;").fetchall()
#     con.close()
#
#     table_list = [t[0] for t in tables]
#     print("Detected DuckDB tables:", table_list)
#
#     context["ti"].xcom_push(key="duckdb_tables", value=table_list)
#
# # -------------------------------------------------
# # 2. Create namespace if missing & Load PyIceberg REST Catalog
# # -------------------------------------------------
# def create_iceberg_namespace(**context):
#     catalog = load_catalog(CATALOG_NAME, uri=CATALOG_URI)
#
#     try:
#         catalog.create_namespace(ICEBERG_NAMESPACE)
#         print(f"Created namespace: {ICEBERG_NAMESPACE}")
#     except NamespaceAlreadyExistsError:
#         print(f"Namespace already exists — continuing: {ICEBERG_NAMESPACE}")
#
# # -------------------------------------------------
# # 3. etch DuckDB table as PyArrow (inside task)
# # -------------------------------------------------
# # def fetch_duckdb_table(table_name: str) -> pa.Table:
# #     con = duckdb.connect(DUCKDB_FILE)
# #     arrow_table = con.execute(f"SELECT * FROM {table_name}").arrow()
# #     con.close()
# #     print(f"Fetched table {table_name} from DuckDB")
#
# def cast_arrow_to_iceberg_schema(arrow_table: pa.Table, iceberg_schema) -> pa.Table:
#     fields = []
#     arrays = []
#
#     for idx, f in enumerate(iceberg_schema.fields, start=0):
#         col_name = f.name
#         col_type = f.field_type
#
#         # Map Iceberg type to Arrow type
#         if isinstance(col_type, StringType):
#             arrays.append(pc.cast(arrow_table[col_name], pa.string()))
#         elif isinstance(col_type, LongType):
#             arrays.append(pc.cast(arrow_table[col_name], pa.int64()))
#         elif isinstance(col_type, DoubleType):
#             arrays.append(pc.cast(arrow_table[col_name], pa.float64()))
#         else:
#             arrays.append(arrow_table[col_name])  # default
#
#         fields.append(pa.field(col_name, arrays[-1].type))
#
#     return pa.Table.from_arrays(arrays, schema=pa.schema(fields))
# # -------------------------------------------------
# # 4. Create Iceberg table and Append Data
# # -------------------------------------------------
# def create_and_append_iceberg(**context):
#     # from pyiceberg.io.pyarrow import write_table
#
#     catalog = load_catalog(CATALOG_NAME, uri=CATALOG_URI)
#
#     # Pull table name from previous task
#     tables = context["ti"].xcom_pull(key="duckdb_tables")
#     if not tables:
#         raise Exception("No DuckDB tables found")
#     table_name = tables[0]
#     identifier = f"{ICEBERG_NAMESPACE}.{table_name}"
#
#     # Fetch Arrow table from DuckDB
#     con = duckdb.connect(DUCKDB_FILE)
#     arrow_table = con.execute(f"SELECT * FROM {table_name}").arrow()
#     con.close()
#     print(f"Fetched table {table_name} from DuckDB")
#
#     # ------------------------------
#     # Build Iceberg schema from Arrow
#     # ------------------------------
#     fields = []
#     for idx, f in enumerate(arrow_table.schema, start=1):
#         pa_type = f.type
#
#         if pa.types.is_int64(pa_type):
#             iceberg_type = LongType()
#
#         elif pa.types.is_int32(pa_type):
#             iceberg_type = IntegerType()
#
#         elif pa.types.is_float64(pa_type):
#             iceberg_type = DoubleType()
#
#         elif pa.types.is_float32(pa_type):
#             iceberg_type = FloatType()
#
#         elif pa.types.is_timestamp(pa_type):
#             if pa_type.tz is not None:
#                 iceberg_type = TimestamptzType()   # timezone-aware
#             else:
#                 iceberg_type = TimestampType()     # naive timestamp
#
#         else:
#             iceberg_type = StringType()
#
#         fields.append(
#             NestedField(
#                 field_id=idx,
#                 name=f.name,
#                 type=iceberg_type,
#                 required=False,
#             )
#         )
#
#     iceberg_schema = Schema(*fields)
#
#     # ------------------------------
#     # Create or load Iceberg table
#     # ------------------------------
#     try:
#         table = catalog.create_table(identifier, iceberg_schema)
#         print(f"Created Iceberg table: {identifier}")
#
#     except TableAlreadyExistsError:
#         table = catalog.load_table(identifier)
#         print(f"Table {identifier} already exists, loading it")
#
#     # ------------------------------
#     # Cast Arrow table to Iceberg schema
#     # Ensures consistency before append
#     # ------------------------------
#     arrow_table = arrow_table.cast(
#         pa.schema([
#             pa.field(f.name, arrow_table.schema.field(f.name).type)
#             for f in table.schema.fields
#         ])
#     )
#
#     # ------------------------------
#     # Append records to Iceberg table
#     # ------------------------------
#     table.append(arrow_table)
#     print(f"Appended data into {identifier}")
#
#
# # -------------------------------------------------
# # 5. NEW: Show Iceberg tables and push to XCom
# # -------------------------------------------------
# # def show_iceberg_tables(**context):
# #     catalog = load_catalog(CATALOG_NAME, uri=CATALOG_URI)
# #     tables = catalog.list_tables(ICEBERG_NAMESPACE)
# #
# #     print("Iceberg tables created:", tables)
# #
# #     context["ti"].xcom_push(key="iceberg_tables", value=tables)
# #
#
#
# # -------------------------------------------------
# # DAG Definition
# # -------------------------------------------------
# with DAG(
#     dag_id="duckdb_to_iceberg_dynamic",
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None,
#     catchup=False
# ) as dag:
#
#     detect_duckdb_tbls = PythonOperator(
#         task_id="detect_duckdb_tables",
#         python_callable=detect_duckdb_tables
#     )
#
#     create_ns_load_icecatalog = PythonOperator(
#         task_id="create_iceberg_namespace",
#         python_callable=create_iceberg_namespace
#     )
#
#     # fetch_duck_as_pyarrow = PythonOperator(
#     #     task_id="fetch_duckdb_arrow",
#     #     python_callable=fetch_duckdb_arrow
#     # )
#
#     create_iceberg_data_append = PythonOperator(
#         task_id="create_and_append_iceberg",
#         python_callable=create_and_append_iceberg
#     )
#
#     # show_icetables = PythonOperator(
#     #     task_id="show_iceberg_tables",
#     #     python_callable=show_iceberg_tables
#     # )
#
#     detect_duckdb_tbls >> create_ns_load_icecatalog >> create_iceberg_data_append #>> show_icetables
