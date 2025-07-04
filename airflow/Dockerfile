FROM apache/airflow:2.11.0-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    git \
    python3-dev \
    pkg-config \
    default-libmysqlclient-dev \
    build-essential \
    libpq-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt