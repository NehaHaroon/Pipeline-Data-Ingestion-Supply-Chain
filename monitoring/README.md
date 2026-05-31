# Monitoring Stack

Prometheus + Grafana for ingestion, analytics workloads, and executive BI.

## Port map

| Where | URL |
|-------|-----|
| **Browser (your PC)** | http://localhost:**9090** |
| **Grafana → Prometheus (Docker)** | http://pipeline-prometheus:**9090** |
| **Prometheus scrapes** | `ingestion-api:8000`, `analytics-service:8002` |

## Run

**Part 1 — Prometheus + ingestion metrics:**

```cmd
docker compose -f docker-compose.yml up -d ingestion-api prometheus
```

**Part 3 — Grafana + analytics (needs `pipeline-net`):**

```cmd
docker compose -f docker-compose.part3.yml up -d analytics-service grafana
```

## Verify

```cmd
curl http://localhost:9090/-/healthy
curl http://localhost:9090/targets
curl http://localhost:8000/metrics/prometheus
curl http://localhost:8002/metrics/prometheus
```

Both scrape targets should be **UP**.

## Access

- Prometheus UI: http://localhost:9090
- Grafana: http://localhost:3000 (`admin` / `admin`)

## API endpoints scraped

- `ingestion-api:8000/metrics/prometheus` — ingestion job counters
- `analytics-service:8002/metrics/prometheus` — `analytics_bi_metric`, `analytics_query_metric`
