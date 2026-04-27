# Supply Chain Data Engineering Pipeline

A complete enterprise-scale supply chain data engineering pipeline implementing Lambda architecture with Iceberg storage, comprehensive observability, and production-ready orchestration.

## Architecture Overview

### Lambda Architecture Implementation
- **Batch Pipeline**: CSV ingestion → Iceberg Bronze → Silver transformations → Gold aggregations
- **Streaming Pipeline**: Kafka streams → Real-time processing → Iceberg Bronze layer
- **Serving Layer**: Unified Gold layer with business-ready aggregations

### Key Components
- **Control Plane**: Data contracts, transformation policies, schema evolution
- **Data Plane**: Batch/streaming ingestion, Bronze→Silver→Gold transformations
- **Storage Plane**: Iceberg tables with partitioning, snapshots, and compaction
- **Observability Plane**: Real-time KPIs, alerting, and dashboard monitoring

## Quick Start (Docker)

### Prerequisites
```bash
# Install Docker and Docker Compose
# Clone repository and navigate to project directory
cd supply_chain_pipeline
```

### 1. Start Complete Pipeline
```bash
# Build containers and start the entire pipeline
python run_production.py
```

This automatically:
-  Builds all Docker containers
-  Starts infrastructure (Kafka, API, Airflow)
-  Runs batch ingestion to Iceberg bronze tables
-  Executes Silver → Gold transformations
-  Starts continuous ingestion schedulers
-  Launches real-time dashboard monitoring

### 2. Access Services
- **API**: http://localhost:8000
- **Airflow**: http://localhost:8080 (admin/admin)
- **Dashboard**: Open `ui_manager.py` in browser

## Alternative: Individual Pipeline Steps

If you need more control, you can also use:

```bash
# Run only Docker orchestration (no continuous schedulers)
python run_docker_pipeline.py

# Run individual steps
python run_docker_pipeline.py infrastructure  # Start containers only
python run_docker_pipeline.py ingest         # Run ingestion only
python run_docker_pipeline.py transform      # Run transformations only
```

## Project Structure

```
supply_chain_pipeline/
├── run_docker_pipeline.py              # Docker orchestration runner
├── run_production_pipeline.py          # Direct Python pipeline runner
├── api.py                              # FastAPI service with all endpoints
├── ui_manager.py                       # Real-time dashboard (HTML/JS)
├── docker-compose.yml                  # Main services (Kafka, API)
├── docker-compose.airflow.yml          # Airflow orchestration
├── Dockerfile                          # Python application container
├── requirements.txt                    # Python dependencies
├── .env                               # Environment configuration
├── config.py                           # Application configuration
├── control_plane/
│   ├── entities.py                     # Data sources, contracts, entities
│   ├── contracts.py                    # Data quality contracts & policies
│   └── transformation_contracts.py     # Transformation rules & justifications
├── data_plane/
│   ├── ingestion/
│   │   ├── batch_ingest.py             # Batch ingestion with Iceberg
│   │   ├── iot_stream_ingest.py        # Streaming ingestion
│   │   └── real_time_iot_ingest.py     # Real-time IoT processing
│   ├── transformation/
│   │   ├── bronze_writer.py            # Iceberg bronze layer writer
│   │   ├── silver_transformer.py       # Silver layer transformations
│   │   └── gold_aggregator.py          # Gold layer aggregations
│   └── cdc/
│       ├── cdc_consumer.py             # CDC event processing
│       └── cdc_strategies.py           # CDC consumption patterns
├── storage_plane/
│   ├── iceberg_catalog.py              # Iceberg catalog management
│   ├── iceberg_entities.py             # Iceberg table definitions
│   ├── storage_kpis.py                 # Storage health & performance KPIs
│   └── table_registry.py               # Table metadata registry
├── observability_plane/
│   └── telemetry.py                    # Telemetry & monitoring
├── airflow/dags/
│   ├── ingestion_dag.py                # Ingestion orchestration
│   ├── transformation_dag.py           # Transformation pipeline
│   └── compaction_dag.py               # Storage maintenance
└── storage/
    ├── raw/                           # Raw CSV data
    ├── iceberg_warehouse/             # Iceberg data lake
    ├── ingested/                      # Processed data
    └── ingested/detail_logs/          # Audit logs & KPIs
```

## API Endpoints

### Ingestion
- `POST /ingest/{source_id}` - Ingest data for a source
- `GET /sources` - List configured sources
- `GET /datasets` - List datasets

### Transformations
- `GET /transformation/summary` - Transformation overview
- `GET /transformation/kpis` - Detailed KPIs

### Storage
- `GET /storage/summary` - Iceberg table health
- `GET /iceberg/kpis/{table}` - Table-specific metrics

### Dashboard
- `GET /dashboard/json` - Dashboard data
- `GET /metrics` - System metrics

## Iceberg Tables

### Bronze Layer (Raw Data)
- `bronze.warehouse_master`
- `bronze.manufacturing_logs`
- `bronze.sales_history`
- `bronze.inventory_transactions`
- `bronze.iot_rfid_stream`

### Silver Layer (Cleaned)
- `silver.warehouse_master`
- `silver.manufacturing_logs`
- `silver.sales_history`
- `silver.inventory_transactions`
- `silver.iot_rfid_stream`

### Gold Layer (Aggregated)
- `gold.replenishment_signals` - Business replenishment recommendations

## Configuration

### Environment Variables (.env)
```bash
# API Configuration
API_TOKEN=ee910d618e617c559f1ca41a3a48c3c7

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Iceberg Storage
STORAGE_ICEBERG_WAREHOUSE=storage/iceberg_warehouse
STORAGE_ICEBERG_CATALOG_URI=sqlite:///storage/iceberg_catalog.db

# Data Sources
INGESTION_SCALE_FACTOR=3
```

## Production Features

### Scalability
- Parallel transformation execution
- Iceberg partitioning for query performance
- Configurable batch sizes and concurrency

### Reliability
- Idempotent operations with snapshot tracking
- Comprehensive error handling and retry logic
- Data quality validation at each stage

### Observability
- Real-time KPIs and health monitoring
- Detailed audit trails and lineage tracking
- Automated alerting and reporting

### Maintainability
- Modular architecture with clear separation of concerns
- Comprehensive documentation and logging
- Automated testing and validation

## Troubleshooting

### Dashboard Shows No Data
1. Run `python run_docker_pipeline.py` to populate data
2. Check API server: `curl http://localhost:8000/health`
3. Verify Iceberg tables exist in `storage/iceberg_warehouse/`

### Transformations Fail
1. Ensure bronze tables exist from ingestion
2. Check logs in `storage/ingested/detail_logs/`
3. Verify contract configurations

### Docker Issues
1. Check Docker services: `docker-compose ps`
2. View logs: `docker-compose logs [service]`
3. Restart services: `docker-compose restart`

## Development

### Adding New Data Sources
1. Define source in `control_plane/entities.py`
2. Create contract in `control_plane/contracts.py`
3. Add ingestion logic in `data_plane/ingestion/`
4. Update transformation DAGs

### Extending Transformations
1. Modify transformation classes in `data_plane/transformation/`
2. Update KPIs in `transformation_kpis.py`
3. Add API endpoints if needed

### Testing
```bash
# Run tests
pytest tests/

# Run specific test
pytest tests/test_api.py
```

## Enterprise Considerations

### Security
- API token authentication
- Data classification and access controls
- Audit logging and compliance

### Performance
- Iceberg query optimization
- Parallel processing and resource management
- Caching and indexing strategies

### Operations
- Automated compaction and maintenance
- Backup and disaster recovery
- Monitoring and alerting integration

---

*This pipeline implements all requirements from "Data Engineering Project – Part 2.md" for enterprise-scale supply chain data engineering with Lambda architecture, Iceberg storage, and comprehensive observability.*