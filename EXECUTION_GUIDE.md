# Execution Guide — Supply Chain Ingestion Pipeline

Complete step-by-step instructions for running the entire pipeline.

---

## **Option 1: Quick Start (Local — All 7 Phases)**

### Prerequisites
```bash
# Install Python 3.11+
python --version  # Should be 3.11 or higher

# Navigate to project directory
cd /path/to/supply_chain_ingestion
```

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Prepare Raw Data
```bash
# Ensure raw CSVs are in correct location
ls -la storage/raw/
# Should show:
# - warehouse_master.csv
# - manufacturing_logs.csv
# - sales_history.csv
# - legacy_trends.csv
```

### Step 3: Run All 7 Phases
```bash
python run_all.py
```

**Expected Output:**
```
======================================================================
  PHASE 1+2: Control Plane — Entities & Contracts
======================================================================
  Sources registered: 6
  Datasets registered: 6
  Contracts registered: 6
  ...

======================================================================
  PHASE 3: Data Generators — Distribution Profiling + Synthetic Generation
======================================================================
  [WarehouseMaster] Generated 5 synthetic records...
  [ManufacturingLogs] Generated 5 synthetic records...
  ...

======================================================================
  PHASE 4: Ingestion — Batch Initial Load + Micro-Batch + IoT Stream
======================================================================
  ── 4a: Full Batch Initial Load ──
  [LOAD] Loaded 10000 rows from storage/raw/warehouse_master.csv
  [WRITE] ✅ 9950 records → storage/ingested/src_warehouse_master.parquet
  [QUARANTINE] ⚠ 50 records → storage/quarantine/src_warehouse_master_quarantine.parquet
  ...

  ── 4b: Micro-Batch on Sales History ──
  Micro-Batch 1/3 | rows 0–200
  [WRITE] ✅ 190 records → storage/micro_batch/...
  ...

======================================================================
  PHASE 5: CDC Trigger — INSERT / UPDATE / DELETE (Steady + Burst)
======================================================================
  ── 5a: Steady stream — Sales (10 rec/s × 10s) ──
  [CDC STEADY] Emitted 100 events | Throughput: 10.0 rec/s
  ...

  ── 5b: Burst — Manufacturing (5000 records) ──
  [CDC BURST] Emitted 5000 events in 0.542s | Throughput: 9223 rec/s
  ...

======================================================================
  PHASE 6: CDC Strategies — Log-Based, Trigger-Based, Timestamp-Based
======================================================================
  LOG-BASED CDC | src_sales_history | scenario=steady
  ✅ Processed 100 events → storage/ingested/src_sales_history_log_cdc_steady.parquet

  TRIGGER-BASED CDC | src_sales_history | scenario=steady
  ✅ 30 trigger events (UPDATE/DELETE) processed
  ...

  TIMESTAMP-BASED CDC | src_sales_history | scenario=steady
  ✅ Processed 100 records → storage/ingested/src_sales_history_ts_cdc_steady.parquet
  ...

======================================================================
  PHASE 7: Observability — Pipeline Summary & Storage Audit
======================================================================
  ── Storage Audit ──
  Ingested (Parquet)     42 files   2345.6 KB
  Quarantine              8 files    234.1 KB
  CDC Log                12 files   1023.4 KB
  Micro-Batch            15 files   890.2 KB
  ─────────────────────────────────────────
  TOTAL                 77 files   4493.3 KB

  Dashboard plot saved to logs/dashboard_throughput.png

======================================================================
  ALL 7 PHASES COMPLETE
  Total pipeline duration: 45.23s
  Check storage/ for Parquet files, quarantine/, stream_buffer/, etc.
======================================================================
```

### Step 4: Review Outputs
```bash
# Check ingested data
ls -la storage/ingested/
# Check quarantine records
ls -la storage/quarantine/
# Check CDC logs
ls -la storage/cdc_log/
# Check detail logs (audit trail)
ls -la storage/ingested/detail_logs/
# Check telemetry logs
ls -la logs/
```

### Step 5: Inspect Results
```bash
# View a sample ingested record (Parquet)
python -c "import pandas as pd; df = pd.read_parquet('storage/ingested/src_warehouse_master.parquet'); print(df.head())"

# View quarantine records
python -c "import pandas as pd; df = pd.read_parquet('storage/quarantine/src_warehouse_master_quarantine.parquet'); print(df[['_quarantine_reason']].head())"

# View audit log (JSON Lines)
head -5 storage/ingested/detail_logs/src_warehouse_master_record_log.jsonl
```

---

## **Option 2: Docker Compose (Production-Grade)**

### Prerequisites
```bash
# Install Docker and Docker Compose
docker --version      # Docker 20.10+
docker-compose --version  # Docker Compose 2.0+
```

### Step 1: Set Environment Variables
```bash
# Create .env file in project root
cat > .env << EOF
API_TOKEN=token
WEATHER_API_KEY=token 
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
EOF
```

### Step 2: Build and Start Services
```bash
# Build images and start all services (Zookeeper, Kafka, API)
docker-compose up --build

# In another terminal, verify services are running
docker-compose ps
```

**Expected Output:**
```
NAME                  COMMAND                  SERVICE      STATUS      PORTS
zookeeper-1          "..."                    zookeeper    Up          2181/tcp
kafka-1              "..."                    kafka        Up          9092/tcp
ingestion-api-1      "uvicorn api:app..."     ingestion-api Up          0.0.0.0:8000->8000/tcp
```

### Step 3: Test API Health
```bash
# In another terminal, check health
curl http://localhost:8000/health
# Response:
# {"status":"healthy","timestamp":"2026-04-07T12:34:56.789012"}
```

### Step 4: Get API Token and Ingest Data
```bash
# Set token variable
TOKEN="your_secure_token_here_change_in_production"

# View available sources
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/sources

# Ingest records
curl -X POST http://localhost:8000/ingest/src_sales_history \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {
        "receipt_id": "R001",
        "product_id": "ART-1001-MID-M",
        "sale_timestamp": "2026-04-07T12:00:00Z",
        "units_sold": 5,
        "store_id": "LDN-OXFORD"
      }
    ]
  }'

# Response:
# {"job_id":"abc-123-def","message":"Ingestion started"}
```

### Step 5: Monitor Job Status
```bash
TOKEN="your_secure_token_here_change_in_production"
JOB_ID="abc-123-def"

# Check job status
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/jobs/$JOB_ID

# Response:
# {
#   "job":"...",
#   "status":"completed",
#   "telemetry":{
#     "records_ingested":1,
#     "throughput_rec_sec":100.0,
#     ...
#   }
# }
```

### Step 6: Query Ingested Data
```bash
TOKEN="your_secure_token_here_change_in_production"

curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/datasets/ds_sales_history?limit=10
```

### Step 7: Check Supply Chain Alerts
```bash
TOKEN="your_secure_token_here_change_in_production"

curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/inventory/alerts
```

### Step 8: Shutdown Services
```bash
docker-compose down

# Clean up volumes (careful — removes data!)
docker-compose down -v
```

---

## **Option 3: Run Individual Phases**

### Phase 1+2: Verify Control Plane (Entities & Contracts)
```python
from control_plane.entities import ALL_SOURCES, ALL_DATASETS
from control_plane.contracts import CONTRACT_REGISTRY

print(f"Sources: {len(ALL_SOURCES)}")
for src in ALL_SOURCES:
    print(f"  - {src.source_id}: {src.source_type.value}")

print(f"\nDatasets: {len(ALL_DATASETS)}")
for ds in ALL_DATASETS:
    print(f"  - {ds.dataset_id}: {ds.domain}")

print(f"\nContracts: {len(CONTRACT_REGISTRY)}")
for source_id, contract in CONTRACT_REGISTRY.items():
    print(f"  - {source_id}: {contract.violation_policy.value}")
```

### Phase 3: Test Generators Only
```python
import pandas as pd
from data_plane.generators.source_generators import (
    WarehouseMasterGenerator, ManufacturingLogsGenerator,
    SalesHistoryGenerator, LegacyTrendsGenerator, IoTStreamGenerator
)

# Load real data
wh_df = pd.read_csv("storage/raw/warehouse_master.csv")
pids = wh_df["product_id"].tolist()

# Create generators
wh_gen = WarehouseMasterGenerator(wh_df)
iot_gen = IoTStreamGenerator(pids)

# Generate synthetic records
wh_samples = wh_gen.generate(10)
iot_samples = [iot_gen.generate_one() for _ in range(10)]

print(f"Generated {len(wh_samples)} warehouse records")
print(f"Generated {len(iot_samples)} IoT records")
print(wh_samples[0])
```

### Phase 4: Batch Ingestion Only
```python
from data_plane.ingestion.batch_ingest import run_all_batch_ingestion, run_micro_batch_ingestion

# Full batch
telemetry = run_all_batch_ingestion()

# Micro-batch on sales
run_micro_batch_ingestion(
    source_id  = "src_sales_history",
    raw_path   = "storage/raw/sales_history.csv",
    dataset_id = "ds_sales_history",
    batch_size = 200,
    max_batches= 3,
)
```

### Phase 5: CDC Trigger (Steady + Burst)
```python
import pandas as pd
from data_plane.generators.source_generators import SalesHistoryGenerator
from data_plane.cdc.cdc_trigger import run_steady_stream, run_burst, load_existing_records

sal_df = pd.read_csv("storage/raw/sales_history.csv")
pids = pd.read_csv("storage/raw/warehouse_master.csv")["product_id"].tolist()
sal_gen = SalesHistoryGenerator(sal_df, pids)

# Load existing records
existing = load_existing_records("storage/ingested/src_sales_history.parquet")

# Steady stream: 10 rec/s for 10s
run_steady_stream(
    source_id    = "src_sales_history",
    dataset_id   = "ds_sales_history",
    existing     = existing,
    generator_fn = sal_gen.generate_one,
    duration_sec = 10,
    target_rps   = 10,
)

# Burst: 5000 records in 1 second
run_burst(
    source_id    = "src_sales_history",
    dataset_id   = "ds_sales_history",
    existing     = existing,
    generator_fn = sal_gen.generate_one,
    burst_count  = 5000,
)
```

### Phase 6: CDC Strategies (All Three)
```python
from data_plane.cdc.cdc_strategies import (
    run_log_based_cdc, run_trigger_based_cdc, run_timestamp_based_cdc
)

cdc_log_path = "storage/cdc_log/src_sales_history_steady_cdc.parquet"

# Log-based CDC
run_log_based_cdc("src_sales_history", cdc_log_path, "steady")

# Trigger-based CDC
run_trigger_based_cdc("src_sales_history", cdc_log_path, "steady")

# Timestamp-based CDC
run_timestamp_based_cdc("src_sales_history", cdc_log_path, "steady")
```

### Phase 7: Telemetry Review
```python
import glob
import pandas as pd

# Find all Parquet files
for label, path in {
    "Ingested": "storage/ingested",
    "Quarantine": "storage/quarantine",
    "CDC Log": "storage/cdc_log",
}.items():
    files = glob.glob(f"{path}/*.parquet")
    print(f"\n{label}:")
    for f in files:
        df = pd.read_parquet(f)
        print(f"  {f}: {len(df)} rows")
```

---

## **Option 4: Run API Only (No Pipeline Execution)**

### Start API Server
```bash
# Terminal 1: Start Kafka (Docker)
docker run -p 9092:9092 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka:7.4.0

# Terminal 2: Start API
API_TOKEN="your_token_here" python -m uvicorn api:app --host 0.0.0.0 --port 8000
```

### Make API Requests
```bash
TOKEN="your_token_here"

# Health check
curl http://localhost:8000/health

# List sources
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/sources

# Ingest data
curl -X POST http://localhost:8000/ingest/src_warehouse_master \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "records": [
      {
        "product_id": "TEST-001",
        "article_id": "ART-001",
        "product_name": "Test Product",
        "color": "Red",
        "size": "M",
        "reorder_threshold": 100,
        "max_capacity": 500,
        "unit_cost": 29.99
      }
    ]
  }'

# Get job status
JOB_ID="<from previous response>"
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/jobs/$JOB_ID

# Query dataset
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/datasets/ds_warehouse_master

# Get metrics
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/metrics

# Get inventory alerts
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/inventory/alerts
```

---

## **Option 5: Run Tests**

### Unit Tests
```bash
# Install test dependencies
pip install pytest

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_api.py -v

# Run with coverage
pytest tests/ --cov=. --cov-report=html
```

### Integration Tests (Manual)
```bash
# 1. Start API
API_TOKEN="test_token" python -m uvicorn api:app --port 8000

# 2. In another terminal, test endpoints
python -c "
import requests
BASE_URL = 'http://localhost:8000'
TOKEN = 'test_token'
headers = {'Authorization': f'Bearer {TOKEN}'}

# Health
print('Health:', requests.get(f'{BASE_URL}/health').json())

# Sources
print('Sources:', len(requests.get(f'{BASE_URL}/sources', headers=headers).json()['sources']))

# Metrics
print('Metrics:', requests.get(f'{BASE_URL}/metrics', headers=headers).json())
"
```

---

## **Troubleshooting**

### Issue: "Module not found" errors
```bash
# Solution: Ensure you're in project root and have installed dependencies
pip install -r requirements.txt
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Issue: "FileNotFoundError: storage/raw/xxx.csv"
```bash
# Solution: Ensure CSV files are in correct location
ls storage/raw/
# Should show: warehouse_master.csv, manufacturing_logs.csv, sales_history.csv, legacy_trends.csv
```

### Issue: Docker Compose "Connection refused"
```bash
# Solution: Wait for services to start (can take 10-30 seconds)
docker-compose ps
# Wait until all services show "Up"
sleep 30
curl http://localhost:8000/health
```

### Issue: Rate limiting "429 Too Many Requests"
```bash
# API limits to 10 requests/minute per IP
# Solution: Wait 60 seconds or use different IP/token
```

### Issue: Port 8000 already in use
```bash
# Solution: Use different port
python -m uvicorn api:app --port 8001

# Or kill existing process
lsof -i :8000
kill -9 <PID>
```

---

## **Performance Benchmarks**

Typical execution times on a standard laptop:

| Phase | Duration | Notes |
|-------|----------|-------|
| Phase 1+2 | <1s | Control Plane validation |
| Phase 3 | 2-3s | Generator profiling + synthesis |
| Phase 4a | 10-15s | 40K records batch ingestion |
| Phase 4b | 3-5s | Micro-batches |
| Phase 4c | 2-3s | IoT stream simulation |
| Phase 5 | 15-20s | CDC trigger (steady 10s + burst ~1s) |
| Phase 6 | 5-10s | All three CDC strategies |
| Phase 7 | 2-3s | Storage audit + reporting |
| **Total** | **40-60s** | Full pipeline end-to-end |

---

## **Next Steps**

1. ✅ Run `python run_all.py` to validate local setup
2. ✅ Deploy with Docker Compose for production
3. ✅ Configure API_TOKEN and WEATHER_API_KEY
4. ✅ Run tests with `pytest tests/`
5. ✅ Monitor with `/health` and `/metrics` endpoints
6. ✅ Integration with downstream systems (dashboards, ML models, alerts)

