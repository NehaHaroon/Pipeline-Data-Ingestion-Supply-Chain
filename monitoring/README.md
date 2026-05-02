# Monitoring Stack

This project now includes a standardized monitoring stack for large-scale operations:

- Prometheus scrapes `ingestion-api` at `/metrics/prometheus`
- Grafana is provisioned automatically with layer-specific dashboards:
  - Ingestion Layer
  - Transformation Layer
  - Storage Layer
  - Serving Layer

## Run

```bash
docker compose up -d ingestion-api prometheus grafana
```

## Access

- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin` / `admin`)

## API Layer Metrics

For service integrations and internal dashboards:

- `GET /observability/layers` returns JSON KPIs for ingestion/transformation/storage/serving
- `GET /metrics/prometheus` includes both existing ingestion metrics and normalized `pipeline_layer_metric` series
