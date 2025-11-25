from airflow.datasets import Dataset

BUILDING_DATASET = Dataset("clickhouse://peopletraffic/raw_Building")
WEATHER_DATASET = Dataset("clickhouse://peopletraffic/raw_Weather")
