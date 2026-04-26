# Supply Chain Ingestion Pipeline

A complete 7-phase Python ingestion pipeline for supply chain sources with contract enforcement, CDC strategies, and observability. The repository implements the full control plane, synthetic data generators, batch + micro-batch ingestion, CDC trigger simulation, three CDC consumption patterns, and telemetry reporting.

---

## Latest Updates

- Full 7-phase pipeline executed by `run_all.py`
- Control plane metadata and contract enforcement in `control_plane/`
- Synthetic generators with distribution profiling in `data_plane/generators/`
- Batch, micro-batch, and IoT stream ingestion in `data_plane/ingestion/`
- CDC trigger simulations for steady and burst workloads in `data_plane/cdc/cdc_trigger.py`
- Log-based, trigger-based, and timestamp-based CDC strategies in `data_plane/cdc/cdc_strategies.py`
- Job telemetry persisted to `storage/telemetry/` and runtime plots generated in `logs/`
- FastAPI service with ingestion, metrics, transformation summaries, and Iceberg storage observability endpoints in `api.py`
- Docker Compose support for optional Kafka usage

---

## Project Structure

```
supply_chain_ingestion/
├── run_all.py                          # Master runner — executes all 7 phases
├── run_production.py                   # Start API + production-mode ingestion
├── api.py                              # FastAPI management and ingestion service
├── config.py                           # Environment and runtime configuration
├── docker-compose.yml                  # Optional Kafka / service integration
├── requirements.txt                    # Python dependencies
├── ARCHITECTURE.md                     # Architecture overview and diagrams
├── EXECUTION_GUIDE.md                  # Detailed execution instructions and outputs
├── Data Engineering Project – Part 1.md # Project requirements and grading rubric
├── README.md                           # This document
├── control_plane/
│   ├── entities.py                     # Phase 1 — DataSource, Dataset, IngestionJob, EventEnvelope
│   └── contracts.py                    # Phase 2 — DataContract enforcement and policies
├── data_plane/
│   ├── generators/
│   │   ├── base_generator.py           # Phase 3 — Distribution profiling + synthetic generation
│   │   └── source_generators.py        # Phase 3 — source-specific synthetic data producers
│   ├── ingestion/
│   │   ├── batch_ingest.py             # Phase 4 — batch + micro-batch ingestion
│   │   └── iot_stream_ingest.py        # Phase 4 — IoT stream simulation
│   └── cdc/
│       ├── cdc_trigger.py              # Phase 5 — CDC trigger generator (steady + burst)
│       └── cdc_strategies.py           # Phase 6 — log/trigger/timestamp CDC strategies
├── observability_plane/
│   └── telemetry.py                    # Phase 7 — telemetry, metrics, and heartbeat logging
├── tests/
│   └── test_api.py                     # API unit tests
└── storage/
    ├── raw/                            # Input CSV sources
    ├── ingested/                       # Validated Parquet outputs
    ├── quarantine/                     # Quarantined invalid records
    ├── cdc_log/                        # Raw CDC event logs
    ├── micro_batch/                    # Micro-batch output slices
    ├── stream_buffer/                  # IoT stream buffer outputs
    ├── telemetry/                      # Persisted telemetry reports
    └── checkpoints/                    # CDC checkpoint state
```

---

---

## Data Sources

| Source | File | Rows | Type | Contract Policy |
|--------|------|------|------|-----------------|
| Warehouse Master | `warehouse_master.csv` | 10,000 | Dimension | QUARANTINE |
| Manufacturing Logs | `manufacturing_logs.csv` | 10,000 | Fact (supply) | AUTO_COERCE |
| Sales History | `sales_history.csv` | 10,000 | Fact (demand) | QUARANTINE |
| Legacy Trends | `legacy_trends.csv` | 10,000 | Historical | AUTO_COERCE |
| IoT RFID Stream | `real_time_stream.py` | ∞ | Stream | QUARANTINE |

---

## Quickstart

```bash
# Install dependencies
pip install -r requirements.txt

# Copy raw CSVs to storage/raw/
cp warehouse_master.csv storage/raw/warehouse_master.csv
cp manufacturing_logs.csv storage/raw/manufacturing_logs.csv
cp sales_history.csv storage/raw/sales_history.csv
cp legacy_trends.csv storage/raw/legacy_trends.csv

# Run all 7 phases (simulation mode)
python run_all.py
```

## Production Deployment

### Using Docker Compose (Recommended)

```bash
# Build and run with optional Kafka support
docker-compose up --build

# API will be available at http://localhost:8000
```

### Manual Production Setup

1. **Run the API:**
   ```bash
   uvicorn api:app --host 0.0.0.0 --port 8000
   ```

2. **Run production ingestion/watch mode:**
   ```bash
   python run_production.py
   ```

3. **Optional Kafka producer:**
   ```bash
   python kafka_producer.py
   ```

### API Endpoints

- `GET /health` — Health check
- `GET /metrics` — Runtime metrics
- `GET /sources` — List registered sources
- `GET /datasets` — List registered datasets
- `POST /ingest/{source_id}` — Send ingestion records
- `GET /jobs` — Job statuses and telemetry
- `GET /telemetry` — Persisted telemetry records
- `GET /dashboard-plots` — Generated plots list
- `GET /storage-summary` — Storage audit HTML
    - `GET /transformation/summary` — Transformation layer summary
    - `GET /transformation/kpis` — Transformation KPI history
    - `GET /storage/iceberg-kpis` — Iceberg table health and compaction KPIs
    - `GET /gold/replenishment-signals` — Top Gold-layer replenishment signals
---

## Phase-by-Phase Reference

### Phase 1 — Control Plane: Entities (`entities.py`)

Defines the core data model. Every pipeline component operates on these types.

**Key classes:**

| Class | Purpose |
|-------|---------|
| `DataSource` | Source metadata: type, extraction mode, change-capture mode, expected schema |
| `Dataset` | Named, versioned, classified landing zone with a retention policy |
| `IngestionJob` | Links source → dataset with execution mode, batch size, retry config |
| `EventEnvelope` | Wraps every record with lineage metadata (event_id, trace_id, operation_type, timestamps) |

**Enumerations:**

- `SourceType`: DB, API, STREAM, FILE
- `ExtractionMode`: PULL, PUSH, QUERY
- `ChangeCaptureMode`: FULL_SNAPSHOT, INCREMENTAL, CDC_LOG_BASED, CDC_TRIGGER_BASED, STREAM_EVENT
- `IngestionFrequency`: REAL_TIME, HOURLY, DAILY, WEEKLY, ON_DEMAND
- `ExecutionMode`: BATCH, MICRO_BATCH, STREAMING, CDC_CONTINUOUS
- `OperationType`: INSERT, UPDATE, DELETE, SNAPSHOT

**Design rationale:** Separating source metadata from job logic enables the same source to be used across multiple jobs, schema drift detection, and replay without touching business logic.

---

### Phase 2 — Control Plane: Data Contracts (`contracts.py`)

Every record must pass a contract before being written to storage. Contracts are defined at the field level.

**`FieldConstraint`** — per-column rules:
- `dtype`: expected Python type (`str`, `int`, `float`, `datetime`)
- `nullable`: whether NULL is allowed
- `allowed_values`: enumeration of valid values (e.g. `{"XS","S","M","L","XL","XXL"}`)
- `min_value` / `max_value`: inclusive numeric bounds
- `unit`: metadata annotation (e.g. `"USD"`)

**Violation policies:**

| Policy | Behaviour |
|--------|-----------|
| `REJECT` | Record discarded, not written anywhere. Pipeline continues. |
| `QUARANTINE` | Record written to `storage/quarantine/` with `_violations` and `_quarantine_reason` fields |
| `AUTO_COERCE` | Pipeline attempts to fix type mismatches. If still broken after coercion → QUARANTINE |

**Contract registry** (source_id → DataContract):

| Source | Policy | Key constraints |
|--------|--------|-----------------|
| `src_warehouse_master` | QUARANTINE | `size` ∈ {XS,S,M,L,XL,XXL}, `unit_cost` ≥ 0, required: product_id, unit_cost |
| `src_manufacturing_logs` | AUTO_COERCE | `quantity_produced` ≥ 0, `defect_count` nullable ≥ 0 |
| `src_sales_history` | QUARANTINE | `receipt_id` not null, `product_id` nullable |
| `src_legacy_trends` | AUTO_COERCE | `total_monthly_sales` ≥ 0, `market_region` nullable |
| `src_iot_rfid_stream` | QUARANTINE | `shelf_location` ∈ {ZONE-A,ZONE-B,ZONE-C}, `current_stock_on_shelf` 0–10000 |

---

### Phase 3 — Generators: Distribution Profiling (`generators/`)

**`BaseGenerator`** (abstract):
- `profile(df)` — learns the statistical fingerprint of a real CSV
  - categorical columns → frequency distributions
  - numeric columns → Gaussian mean/std sampling
  - timestamp columns → range sampling
- `generate(n)` → synthetic records matching the original data patterns
- `upsample(n)` → generate more records than the original dataset size

**Source-specific generators:**

| Generator | Purpose |
|-----------|---------|
| `WarehouseMasterGenerator` | Generate SKU and inventory metadata records |
| `ManufacturingLogsGenerator` | Generate production events with realistic batch patterns |
| `SalesHistoryGenerator` | Generate sales history events with demand patterns |
| `LegacyTrendsGenerator` | Generate legacy trend records with historical period metadata |
| `IoTStreamGenerator` | Generate RFID stream events with duplicates and sensor behavior |

---

### Phase 4 — Ingestion (`ingestion/`)

#### 4a: Full Batch Load (`batch_ingest.py`)

Loads batch CSV sources, normalizes rows, deduplicates, enforces contracts, wraps records in `EventEnvelope`, writes valid records to `storage/ingested/`, and quarantines invalid records.

#### 4b: Micro-batch ingestion

Splits batch sources into smaller slices and writes independent Parquet outputs to `storage/micro_batch/`.

#### 4c: IoT stream simulation (`iot_stream_ingest.py`)

Simulates RFID stream ingestion and flushes buffered events to `storage/stream_buffer/`.

---

### Phase 5 — CDC Trigger (`cdc/cdc_trigger.py`)

Simulates CDC change events with INSERT / UPDATE / DELETE operations.

**Scenarios:**
- `Steady Stream`: 10 records/sec for 10 seconds
- `Burst`: 5,000 records emitted in one second

Outputs are written to `storage/cdc_log/`.

---

### Phase 6 — CDC Strategies (`cdc/cdc_strategies.py`)

Implements three CDC ingestion patterns with exactly-once semantics:
- log-based CDC with checkpointing
- trigger-based CDC with event deduplication
- timestamp-based CDC with watermarking

Each strategy writes results to `storage/ingested/` and checkpoint state to `storage/checkpoints/`.

---

### Phase 7 — Observability (`observability_plane/telemetry.py`)

`JobTelemetry` logs metrics for each job and persists them to `storage/telemetry/telemetry_records.jsonl`.

Captured metrics include:
- `records_ingested`
- `records_failed`
- `records_quarantined`
- `records_coerced`
- `throughput_rec_sec`
- `avg_ingestion_latency_sec`
- `processing_lag_sec`
- `file_count_per_partition`
- `snapshot_count`

---

## Storage Layout After a Full Run

- `storage/raw/` — source CSV files
- `storage/ingested/` — validated Parquet outputs
- `storage/quarantine/` — invalid/quarantined records
- `storage/cdc_log/` — raw CDC event logs
- `storage/micro_batch/` — micro-batch outputs
- `storage/stream_buffer/` — IoT stream flush outputs
- `storage/ingested/detail_logs/` — transformation KPI history and audit logs
- `storage/iceberg_warehouse/` — Iceberg data warehouse files
- `storage/iceberg_catalog.db` — Iceberg catalog metadata store
- `storage/telemetry/` — persisted telemetry reports
- `storage/checkpoints/` — CDC checkpoint state

---

## Tests

```bash
pytest tests/test_api.py
```

---

## Additional Documentation

- `ARCHITECTURE.md` — architecture overview and plane separation
- `EXECUTION_GUIDE.md` — detailed run instructions and output examples
- `Data Engineering Project – Part 1.md` — project requirements and grading rubric


1. **Load** — `pd.read_csv` of the source file
2. **Normalize** — uppercase IDs, ISO-8601 timestamps, impute null defect_count → 0.0, migrate legacy schema
3. **Deduplicate** — `receipt_id` for sales, `production_batch_id` for manufacturing, `product_id` for warehouse
4. **Contract enforcement** — PASS / QUARANTINE / REJECT / COERCE
5. **EventEnvelope** — wrap with event_id, trace_id, operation_type, timestamps
6. **Write** — good records → `storage/ingested/{source_id}.parquet`; bad → `storage/quarantine/`
7. **Audit log** — per-record JSONL written to `storage/ingested/detail_logs/`

**Log format per record:**
```
[PASS]      source=src_sales_history | row=42 | key=A1B2C3D4 | contract=contract_sales_v1 | policy=quarantine | norm_changes=0
[QUARANTINE] source=src_sales_history | row=99 | key=None | contract=contract_sales_v1 | policy=quarantine | violations=[Required field 'product_id' is missing or null.] | norm_changes=0
[COERCED]   source=src_manufacturing_logs | row=7 | key=BATCH-50007 | contract=contract_manufacturing_v1 | policy=auto_coerce | norm_changes=1 | NORMALIZE | field=product_id | 'art-1001-mid-m' → 'ART-1001-MID-M'
[DUPLICATE] source=src_sales_history | row=104 | key=A1B2C3D4 | reason=dedup_key_already_seen | skipped=True
```

#### 4b: Micro-Batch (`batch_ingest.py → run_micro_batch_ingestion()`)

Splits source CSV into time-windowed slices of `batch_size` rows. Each slice is a fully independent job with its own `job_id`, telemetry report, and Parquet output. Simulates near-real-time ingestion without a streaming broker.

```python
run_micro_batch_ingestion(
    source_id  = "src_sales_history",
    raw_path   = "storage/raw/sales_history.csv",
    dataset_id = "ds_sales_history",
    batch_size = 200,    # rows per micro-batch
    max_batches= 5,
)
```

Outputs: `storage/micro_batch/{source_id}_microbatch_001.parquet`, `_002`, ...

#### 4c: IoT Stream (`iot_stream_ingest.py → run_stream_simulation()`)

Simulates `real_time_stream.py` without a live Kafka broker. Generates RFID pings in a loop using real product_ids from warehouse_master, then flushes to disk every N events.

```
[IOT-PASS]      event=47  | event_id=3fa8... | product_id=ART-1042-NAV-M | shelf=ZONE-B | stock=87 | battery=64%
[IOT-DUPLICATE] event=103 | event_id=3fa8... | reason=duplicate_sensor_ping_policy | discarded=True
[IOT-QUARANTINE]event=211 | violations=[shelf_location 'ZONE-X' not in allowed set]
[IOT-FLUSH]     Micro-batch 1 flushed to disk
```

Flush outputs: `storage/stream_buffer/iot_stream_{timestamp}_batch001.parquet`

---

### Phase 5 — CDC Trigger (`cdc/cdc_trigger.py`)

Generates INSERT / UPDATE / DELETE events against already-ingested records.

**Event weighting:** INSERT 60% | UPDATE 30% | DELETE 10%

**Scenarios:**

| Scenario | Config | Purpose |
|----------|--------|---------|
| Steady Stream | 10 rec/s × 10s | Validates CDC throughput matches target rate |
| Burst | 5,000 records in ~1s | Tests buffer capacity — are events dropped or processed? |

Each event is wrapped in an EventEnvelope and saved to `storage/cdc_log/{source_id}_{scenario}_cdc.parquet`.

---

### Phase 6 — CDC Strategies (`cdc/cdc_strategies.py`)

Three production CDC patterns, each with exactly-once guarantees:

#### Strategy 1: Log-Based CDC
- Reads the CDC event log (simulating Postgres WAL / MySQL BINLOG via Debezium)
- Resumes from checkpoint (last processed row index)
- Transactional commits every 500 records → prevents partial writes
- Production equivalent: **Debezium → WAL → Kafka → Sink**

#### Strategy 2: Trigger-Based CDC
- Filters CDC log for UPDATE and DELETE events only (INSERTs come from initial load)
- Exactly-once via event_id deduplication set
- Production equivalent: **DB TRIGGER → change_log table → pipeline**

#### Strategy 3: Timestamp-Based CDC
- Selects records WHERE `_ingestion_timestamp > watermark`
- Watermark advanced to max timestamp seen each run
- Risk acknowledged in code: sub-second updates may be missed at watermark boundary
- Production equivalent: **SELECT * FROM table WHERE updated_at > :watermark**

All three write outputs to `storage/ingested/{source_id}_{strategy}_cdc_{scenario}.parquet` and save checkpoints to `storage/checkpoints/`.

---

### Phase 7 — Observability (`observability_plane/telemetry.py`)

**`JobTelemetry`** — attached to every ingestion job:

| Metric | Description |
|--------|-------------|
| `records_ingested` | Successfully processed and written |
| `records_failed` | REJECT policy hits |
| `records_quarantined` | QUARANTINE policy hits |
| `records_coerced` | AUTO_COERCE corrections |
| `throughput_rec_sec` | records_ingested / duration |
| `avg_ingestion_latency_sec` | Mean source→ingestion lag |
| `processing_lag_sec` | Wall-clock duration of job |
| `file_count_per_partition` | Parquet files written |
| `snapshot_count` | Full snapshots completed |

**`Heartbeat`** — background thread emitting live status every 5 seconds:
```
[HEARTBEAT][job_id] elapsed=5.0s | ingested=50 | failed=0 | quarantined=0 | throughput=10.0 rec/s
```

**Phase 7 storage audit** (run at end of pipeline):
- Scans all storage directories, counts files and sizes
- Reads row counts from all Parquet files
- Logs a summary table to console

---

## Storage Layout After a Full Run

```
storage/
├── raw/                                     # Input CSVs
│   ├── warehouse_master.csv
│   ├── manufacturing_logs.csv
│   ├── sales_history.csv
│   └── legacy_trends.csv
│
├── ingested/                                # Good records (Parquet)
│   ├── src_warehouse_master.parquet
│   ├── src_manufacturing_logs.parquet
│   ├── src_sales_history.parquet
│   ├── src_legacy_trends.parquet
│   ├── src_sales_history_ts_cdc_steady.parquet
│   ├── src_manufacturing_logs_ts_cdc_burst.parquet
│   └── detail_logs/                         # Per-record audit JSONL
│       ├── src_warehouse_master_record_log.jsonl
│       └── ...
│
├── quarantine/                              # Contract-violating records
│   └── {source_id}_quarantine.parquet
│
├── cdc_log/                                 # Raw CDC event logs
│   ├── src_sales_history_steady_cdc.parquet
│   └── src_manufacturing_logs_burst_cdc.parquet
│
├── micro_batch/                             # Micro-batch Parquet slices
│   └── src_sales_history_microbatch_001.parquet
│
├── stream_buffer/                           # IoT stream flush buffers
│   └── iot_stream_20260405T124600_batch001.parquet
│
└── checkpoints/                             # CDC watermarks + offsets
    ├── log_based_src_sales_history.json
    └── timestamp_based_src_manufacturing_logs.json
```

---

## Key Design Decisions

**Why decouple DataSource from IngestionJob?**
Same source can feed multiple jobs (batch vs. streaming). Source schema drift is detected at the source level, not buried in job code.

**Why EventEnvelope on every record?**
Without lineage metadata (event_id, trace_id, operation_type, timestamps), a data lake becomes unauditable. The envelope enables replay, exactly-once deduplication, and schema evolution tracking.

**Why QUARANTINE over REJECT for most sources?**
Rejected records are gone forever. Quarantined records can be re-processed after the root cause is fixed. Only use REJECT when the record is provably dangerous to downstream systems.

**Why micro-batch instead of streaming for batch sources?**
Reduces memory pressure, enables per-slice monitoring, and provides natural checkpoints for failure recovery — without requiring Kafka or Spark.

**Why three CDC strategies?**
No single CDC strategy fits all sources. Log-based is most complete but requires transaction log access. Trigger-based works without log access. Timestamp-based is the fallback for legacy systems.

---

## Dependencies

```
pandas>=2.0
pyarrow>=14.0
numpy>=1.26
```

# Inventory Transactions DB + CDC

<!-- This module provisions PostgreSQL and Debezium CDC for `inventory_transactions`, then streams CDC events into Kafka topic `supply_chain.public.inventory_transactions`. -->

## Start full stack

```bash
docker-compose up -d
```

Windows automatic fallback starter (uses `5432`, falls back to `5433` if occupied):

```powershell
powershell -ExecutionPolicy Bypass -File data_plane/db/up_with_port_fallback.ps1
```

Note: `db-seeder` and `debezium-setup` are one-shot jobs. They typically show `Exited (0)` after success, which is expected.

## Verify PostgreSQL

```bash
docker exec -it <postgres_container> psql -U etl_user -d supply_chain_db -c "SELECT COUNT(*) FROM inventory_transactions;"
```

## Check Debezium connector status

```bash
curl http://localhost:8083/connectors/inventory-transactions-connector/status
```

## Watch CDC events in Kafka

```bash
docker exec -it <kafka_container> kafka-console-consumer --bootstrap-server localhost:9092 --topic supply_chain.public.inventory_transactions --from-beginning
```

## Run generator

Steady mode (10 inserts per second):

```bash
python data_plane/generators/inventory_transaction_generator.py --mode steady
```

Burst mode (5000 records rapidly, then 10 second pause):

```bash
python data_plane/generators/inventory_transaction_generator.py --mode burst
```

## Run CDC consumer

```bash
python data_plane/cdc/cdc_consumer.py
```

## Scheduled ingestion behavior

- Weather API ingestion (`src_weather_api`) runs every 2 minutes in production runner.
- Database ingestion (`src_inventory_transactions`) also runs every 2 minutes in production runner.
- Both emit telemetry records into:
  - `storage/telemetry/telemetry_records.jsonl`
  - source-specific telemetry logs in `storage/telemetry/`

Run production mode:

```bash
python run_production.py
```

Optional scheduler overrides:

- `WEATHER_INGESTION_INTERVAL_SECONDS` (default: auto-detected from WEATHER_API_SOURCE.ingestion_frequency)
- `DB_INGESTION_INTERVAL_SECONDS` (default: auto-detected from INVENTORY_TRANSACTIONS_SOURCE.ingestion_frequency)

## Kafka topic naming convention

Debezium topic naming follows:

`{server_name}.{schema}.{table}`

For this setup:

`supply_chain.public.inventory_transactions`

## Troubleshooting

- PostgreSQL log line `invalid length of startup packet`:
  - typically a harmless probe/noise from a non-Postgres client hitting port `5432`.
- Replication slot conflict: drop stale slot if required:
  - `SELECT pg_drop_replication_slot('debezium_slot');`
- Connector returns 409 on re-register:
  - this is expected when connector already exists; `data_plane/cdc/register_connector.sh` handles it gracefully.
- Verify WAL logical level:
  - `SHOW wal_level;` should return `logical`.
- If host cannot reach `localhost:5432`:
  - run `docker-compose ps` and verify `postgres` is healthy
  - check local port conflicts (`5432`) from another local PostgreSQL service
  - if conflict exists, stop local PostgreSQL service or set `POSTGRES_HOST_PORT` (e.g., `5433`)
  - compose now supports `POSTGRES_HOST_PORT` via `${POSTGRES_HOST_PORT:-5432}:5432`
