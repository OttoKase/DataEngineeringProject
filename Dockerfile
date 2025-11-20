FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl iputils-ping wget && rm -rf /var/lib/apt/lists/*


# Install dependencies
RUN pip install --no-cache-dir duckdb pyarrow pyiceberg pandas



# COPY sample_data/ ./sample_data/
# COPY scripts/ ./scripts/

# Copy files into container
COPY etc/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt


# Install dbt and the clickhouse adapter
RUN pip install --no-cache-dir dbt-core dbt-clickhouse clickhouse-connect
#
# ENTRYPOINT ["bash"]

WORKDIR /lab
