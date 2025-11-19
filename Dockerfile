FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl iputils-ping wget && rm -rf /var/lib/apt/lists/*


# Install dependencies
RUN pip install --no-cache-dir duckdb pyarrow pyiceberg pandas

WORKDIR /lab
# COPY sample_data/ ./sample_data/
# COPY scripts/ ./scripts/


# Copy files into container
# COPY etc/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r meteostat #etc/requirements.txt

# Install dbt and the clickhouse adapter
RUN pip install --no-cache-dir dbt-core dbt-clickhouse clickhouse-connect

#
# THIS IS FOR AIRFLOW-WEBSERVER DAGs, i.e. to run duckdb tasks
# FROM apache/airflow:2.8.1 #3.1.3 #2.8.2
# COPY etc/requirements.txt . #requirements.txt /
# RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
