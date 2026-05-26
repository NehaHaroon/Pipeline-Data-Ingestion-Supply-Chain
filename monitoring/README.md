# Monitoring Stack

This project now includes a standardized monitoring stack for large-scale operations:

- Prometheus scrapes `ingestion-api` at `/metrics/prometheus`
- Grafana is provisioned automatically with layer-specific dashboards:
  - Ingestion Layer
  - Transformation Layer
  - Storage Layer
  - Serving Layer

## Run

**Parts 1–2 (Prometheus + layer dashboards):**

```bash
docker compose -f docker-compose.yml up -d ingestion-api prometheus
```

**Part 3 (BI + workload monitoring — requires `pipeline-net` from ingestion compose):**

```bash
docker compose -f docker-compose.part3.yml up -d analytics-service grafana
```

Prometheus (port 9090) scrapes both `ingestion-api:8000` and `analytics-service:8002` when all stacks are up.

## Access

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin` / `admin`)

## API Layer Metrics

For service integrations and internal dashboards:

- `GET /observability/layers` returns JSON KPIs for ingestion/transformation/storage/serving
- `GET /metrics/prometheus` includes both existing ingestion metrics and normalized `pipeline_layer_metric` series
