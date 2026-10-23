# Extend official Apache Airflow image
FROM apache/airflow:2.8.1-python3.10

USER root

# Install system dependencies required for PostgreSQL and compilation
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy and install python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt
