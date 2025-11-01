# Dockerfile

# Use Python base image
FROM python:3.12.5-bookworm

# Copy files into container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default command (opens interactive shell unless overridden)
#CMD ["tail", "-f", "/dev/null"]