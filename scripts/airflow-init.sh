#!/bin/bash
# Optional manual init — prefer: docker compose -f docker-compose.airflow.yml run --rm airflow-init
set -euo pipefail
echo "=== airflow-init: start $(date -u +%Y-%m-%dT%H:%M:%SZ) user=$(id -un) uid=$(id -u) ==="
STORAGE="/opt/airflow/project/storage"
mkdir -p "${STORAGE}/.locks" /opt/airflow/logs
echo "=== airflow-init: running airflow db migrate ==="
airflow db migrate
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@local.dev 2>/dev/null || true
airflow variables set API_TOKEN "${API_TOKEN:-}" 2>/dev/null || true
airflow connections delete ingestion-api 2>/dev/null || true
airflow connections add ingestion-api \
  --conn-type http --conn-host ingestion-api --conn-port 8000 2>/dev/null || true
echo "=== airflow-init: completed OK ==="
