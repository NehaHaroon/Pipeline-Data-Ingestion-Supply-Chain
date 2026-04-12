# Supply Chain Ingestion Pipeline

A production-grade, 7-phase data ingestion pipeline for a multi-source supply chain system. Built entirely in Python with no external orchestration dependencies — runs end-to-end with `python run_all.py`.

---

## Project Structure

```
supply_chain_ingestion/
├── run_all.py                          # Master runner — executes all 7 phases
│
├── control_plane/
│   ├── entities.py                     # Phase 1 — DataSource, Dataset, IngestionJob, EventEnvelope
│   └── contracts.py                    # Phase 2 — DataContract + REJECT / QUARANTINE / AUTO_COERCE
│
├── data_plane/
│   ├── generators/
│   │   ├── base_generator.py           # Phase 3 — Abstract base with distribution profiling
│   │   └── source_generators.py        # Phase 3 — One generator per source (fitted distributions)
│   ├── ingestion/
│   │   ├── batch_ingest.py             # Phase 4 — Full batch load + micro-batch ingestion
│   │   └── iot_stream_ingest.py        # Phase 4 — Simulated IoT RFID stream (no Kafka needed)
│   └── cdc/
│       ├── cdc_trigger.py              # Phase 5 — INSERT / UPDATE / DELETE, steady + burst
│       └── cdc_strategies.py           # Phase 6 — Log-based, trigger-based, timestamp-based CDC
│
├── observability_plane/
│   └── telemetry.py                    # Phase 7 — JobTelemetry + Heartbeat (every 5s)
│
└── storage/
    ├── raw/                            # Source CSVs (copied from uploads)
    ├── ingested/                       # Parquet outputs (good records)
    │   └── detail_logs/               # Per-record JSONL audit logs
    ├── quarantine/                     # Parquet outputs (violating records)
    ├── cdc_log/                        # CDC event logs (steady + burst)
    ├── micro_batch/                    # Micro-batch Parquet slices
    ├── stream_buffer/                  # IoT stream flush buffers
    └── checkpoints/                    # CDC strategy checkpoints (JSON)
```

---

## Production Deployment

### Prerequisites
- Docker & Docker Compose
- Python 3.11+
- API Token (set `API_TOKEN` env var)
- Optional: OpenWeatherMap API key for real weather data

### Quick Start
```bash
# Clone and setup
git clone <repo>
cd supply_chain_ingestion

# Set environment variables
export API_TOKEN="token "
export WEATHER_API_KEY="token "  # Optional

# Run with Docker Compose
docker-compose up --build

# API available at http://localhost:8000
# Health check: http://localhost:8000/health
# Metrics: http://localhost:8000/metrics
```

### API Endpoints
- `GET /health` - Health check
- `GET /sources` - List data sources
- `POST /ingest/{source_id}` - Ingest data (requires Bearer token)
- `GET /jobs/{job_id}` - Check job status
- `GET /datasets/{dataset_id}` - Query ingested data
- `GET /inventory/alerts` - Supply chain alerts (low stock)

### Security
- Bearer token authentication on protected endpoints
- Rate limiting: 10 requests/minute per IP
- Non-root Docker user
- Input validation and sanitization

### Monitoring
- Health checks built into Docker
- Basic metrics endpoint
- Structured logging to files
- Telemetry collection per job

### Testing
```bash
pip install -r requirements.txt
pytest tests/
```

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
# Build and run with Kafka
docker-compose up --build

# API will be available at http://localhost:8000
# Kafka at localhost:9092
```

### Manual Production Setup

1. **Start Kafka:**
   ```bash
   # Using Docker
   docker run -p 9092:9092 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 confluentinc/cp-kafka:7.4.0
   ```

2. **Run the API:**
   ```bash
   uvicorn api:app --host 0.0.0.0 --port 8000
   ```

3. **Run Production Mode:**
   ```bash
   python run_production.py
   # This starts the API and real-time IoT consumer
   ```

4. **Send IoT Events:**
   ```bash
   python kafka_producer.py
   ```

### API Endpoints

- `GET /` - Health check
- `GET /sources` - List all data sources
- `GET /datasets` - List all datasets
- `POST /ingest/{source_id}` - Ingest records for a source
- `GET /jobs/{job_id}` - Get job status
- `GET /datasets/{dataset_id}` - Query dataset records

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
  - Categorical columns → frequency distribution → `np.random.choice`
  - Numeric columns (high-cardinality) → Gaussian fit (mean, std, min, max clamp)
  - Timestamp columns → uniform range sampling
- `generate(n)` → list of n synthetic dicts matching the learned distribution
- `upsample(n)` → alias for generate when n > original row count

**Source-specific generators:**

| Generator | Dirty data injected | Purpose |
|-----------|---------------------|---------|
| `WarehouseMasterGenerator` | None (clean dimension) | Generates new SKU variants with realistic codes |
| `ManufacturingLogsGenerator` | 20% lowercase product_id, 5% null defect_count | Tests normalization + imputation policies |
| `SalesHistoryGenerator` | 5% null product_id, 3% negative units_sold | Tests quarantine + return-event handling |
| `LegacyTrendsGenerator` | Schema migration: old_product_code → product_id | Tests schema enforcement pipeline |
| `IoTStreamGenerator` | 3% duplicate event_id (exact sensor re-pings) | Tests deduplication policy |

All source generators implement the `BaseGenerator` ABC (`_get_raw_path()` + `_post_process()`).

---

### Phase 4 — Ingestion (`ingestion/`)

#### 4a: Full Batch Load (`batch_ingest.py → run_all_batch_ingestion()`)

Ingests all 4 CSV sources end-to-end. Per record, the pipeline:

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

No Kafka, Spark, Airflow, or external services required. The IoT stream simulation runs natively.
