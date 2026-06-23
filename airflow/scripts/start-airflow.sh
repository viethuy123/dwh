#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_DIR"
echo "==== $(date) ====" >> /tmp/airflow_start.log

/usr/bin/docker compose up -d >> /tmp/airflow_start.log 2>&1

sleep 30

/usr/bin/docker compose ps >> /tmp/airflow_start.log 2>&1