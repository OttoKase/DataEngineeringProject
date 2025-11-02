from datetime import datetime
import subprocess
import requests
# import pymongo
from airflow import DAG
from airflow.operators.python import PythonOperator

# -------------------------------
# CONFIGURATION
# -------------------------------


# GitHub JSON URL
GITHUB_JSON_URL = (
    "https://raw.githubusercontent.com/OttoKase/DataEngineeringProject/refs/heads/main/mobility_09.2025_unicode.json"
)

# MongoDB credentials and configuration
MONGO_URI = "mongodb://admin:password@localhost:27017/"
MONGO_DB = "mongodb_mob"
MONGO_COLLECTION = "vehicles_traffic"


# -------------------------------
# TASK FUNCTIONS
# -------------------------------

def install_pymongo():
    """Ensure pymongo is installed inside the Airflow environment."""
    subprocess.run(["pip", "install", "pymongo"], check=True)
    print("pymongo installation complete.")



def fetch_json_from_github(**context):
    """Fetch JSON data from a public GitHub repository."""
    # print(f"Fetching JSON from: {GITHUB_JSON_URL}")
    response = requests.get(GITHUB_JSON_URL)
    response.raise_for_status()

    data = response.json()
    context['ti'].xcom_push(key='json_data', value=data)

    # print("✅ Successfully fetched JSON from GitHub.")
    # print(f"Records fetched: {len(data) if isinstance(data, list) else 1}")


def create_mongo_db_and_collection():
    """Create MongoDB database and collection if they do not exist."""
    import pymongo
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    if MONGO_COLLECTION not in db.list_collection_names():
        db.create_collection(MONGO_COLLECTION)
        print(f"✅ Created collection '{MONGO_COLLECTION}' in database '{MONGO_DB}'.")
    else:
        print(f"ℹ️ Collection '{MONGO_COLLECTION}' already exists.")

    client.close()


# def load_data_into_mongodb(**context):
#     """Load fetched JSON data into MongoDB."""
#     import pymongo
#
#     ti = context['ti']
#     data = ti.xcom_pull(key='json_data', task_ids='fetch_json_from_github')
#
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client[MONGO_DB]
#     collection = db[MONGO_COLLECTION]
#
#     # Insert depending on structure
#     if isinstance(data, list):
#         if data:
#             collection.insert_many(data)
#         else:
#             print("No records to insert (empty list).")
#     elif isinstance(data, dict):
#         collection.insert_one(data)
#     else:
#         raise ValueError("Unsupported JSON structure (not a list or dict).")
#
#     client.close()
#     print("MongoDB connection closed.")
#

# def filter_mongo_records(**context):
#     """Filter MongoDB records for 'Lootsa-Sepise360' where label='car'."""
#     import pymongo
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client[MONGO_DB]
#     collection = db[MONGO_COLLECTION]
#
#     query = {"building_name": "Lootsa-Sepise360", "label": "car"}
#     results = list(collection.find(query))
#
#     print(f"✅ Found {len(results)} matching records.")
#     for r in results[:5]:  # show only a few examples
#         print(r)
#
#     client.close()

#
# def show_xcom_results(**context):
#     """Retrieve filtered records from XCom and print them."""
#     ti = context['ti']
#     results = ti.xcom_pull(key='filtered_results', task_ids='filter_mongo_records')
#
#     if not results:
#         print("No matching records found.")
#     else:
#         for r in results[:5]:  # preview first 5
#             print(r)


# -------------------------------
# DAG DEFINITION
# -------------------------------
with DAG(
    dag_id="github_to_mongo_mobility_xcom_dag",
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,
    catchup=False,
    description="Fetch JSON from GitHub, create MongoDB db/collection, load data, filter Lootsa-Sepise360 cars, show in XCom",
    tags=["github", "mongodb", "etl", "xcom"],
) as dag:

    t1 = PythonOperator(
        task_id="install_pymongo",
        python_callable=install_pymongo,
    )

    t2 = PythonOperator(
        task_id="fetch_json_from_github",
        python_callable=fetch_json_from_github,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="create_mongo_db_and_collection",
        python_callable=create_mongo_db_and_collection,
    )

    # t4 = PythonOperator(
    #     task_id="load_data_into_mongodb",
    #     python_callable=load_data_into_mongodb,
    #     provide_context=True,
    # )

    # t5 = PythonOperator(
    #     task_id="filter_mongo_records",
    #     python_callable=filter_mongo_records,
    #     provide_context=True,
    # )
    #
    # t6 = PythonOperator(
    #     task_id="show_xcom_results",
    #     python_callable=show_xcom_results,
    #     provide_context=True,
    # )

    # Task dependencies
    t1 >> t2 >> t3# >> t4 #>> t5 >> t6





#
# with DAG(
#     dag_id="github_to_mongo_dag",
#     start_date=datetime(2025, 11, 1),
#     schedule_interval=None,  # Manual trigger
#     catchup=False,
#     description="Fetches JSON from GitHub and loads it into MongoDB",
#     tags=["github", "mongodb", "json", "etl"],
# ) as dag:
#
#     fetch_task = PythonOperator(
#         task_id="fetch_json_from_github",
#         python_callable=fetch_json_from_github,
#         provide_context=True,
#     )
#
#     # load_task = PythonOperator(
#     #     task_id="load_data_into_mongodb",
#     #     python_callable=load_data_into_mongodb,
#     #     provide_context=True,
#     # )
#
#     # Task dependencies
#     fetch_task #>> load_task
