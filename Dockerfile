# Dockerfile

# Use Python base image
FROM python:3.12.5-bookworm

# Copy files into container
COPY etc/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r etc/requirements.txt

# Install dbt and the clickhouse adapter
RUN pip install --no-cache-dir dbt-core dbt-clickhouse clickhouse-connect

# Default command (opens interactive shell unless overridden)
#CMD ["tail", "-f", "/dev/null"]