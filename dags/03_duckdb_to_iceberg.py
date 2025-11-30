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
    dag_id="duckdb_to_iceberg_infrared",
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
        task_id="show_iceberg_tables_inXCom",
        python_callable=show_iceberg_tables,
    )


    create_ns_load_icecatalog >> create_iceberg_data_append >> show_icetables

