# dbt Lineage — RFID Events → Bronze → Silver → Gold → Dashboard Metrics

**Canonical source ID:** `src_iot_rfid_stream`  
**Business label:** RFID shelf events (`rfid_events`) — Kafka IoT stream of product_id, shelf stock, and zone.

---

## End-to-end lineage (platform view)

```mermaid
flowchart TB
    subgraph Source["Source — RFID events"]
        KF[Kafka topic\nsupply_chain_inventory]
        RT[real_time_stream.py\n/ iot-producer]
        EV[rfid_events\nproduct_id · current_stock_on_shelf\nshelf_location · timestamp]
    end

    subgraph P1["Part 1 — Ingestion"]
        API[ingestion-api\nPOST /ingest/src_iot_rfid_stream]
        BW[BronzeWriter]
    end

    subgraph Iceberg["Iceberg medallion"]
        B[(bronze.iot_rfid_stream\nappend micro-batches)]
        S[(silver.iot_rfid_stream\nvalidated + typed)]
        G[(gold.replenishment_signals\ncurrent_stock_on_shelf\nneeds_replenishment · urgency_score)]
    end

    subgraph P2["Part 2 — Transform"]
        ST[SilverTransformer\nsrc_iot_rfid_stream]
        GA[GoldAggregator\nlatest_stock from IoT]
    end

    subgraph Export["Semantic export"]
        PQ[storage/semantic/parquet/\nsilver_iot_rfid_stream.parquet\ngold_replenishment_signals.parquet]
    end

    subgraph DBT["dbt semantic layer"]
        STG[stg_iot_rfid_stream]
        INT[int_latest_shelf_stock]
        WH[stg_warehouse_master]
        FCT[fct_replenishment_signals]
        MET[metric: inventory_turnover]
    end

    subgraph Analytics["Analytics + BI"]
        OP1[SQL op_001\ncurrent_inventory]
        EX1[SQL exec_001\nkpi_scorecard]
        BI[bi_kpis.py → Prometheus]
        GR[Grafana supply-chain-bi\n+ :8002/dashboard]
    end

    KF --> RT --> EV --> API --> BW --> B
    B --> ST --> S
    S --> GA
    WH2[stg_warehouse_master\n+ sales + weather] --> GA
    GA --> G

    S --> PQ
    G --> PQ
    PQ --> STG --> INT
    PQ --> FCT
    WH --> FCT
    INT -.->|same logic as Gold latest_by| GA
    FCT --> MET

    S --> OP1
    G --> EX1
    EX1 --> BI --> GR
    MET --> GR
    OP1 --> GR

    style Source fill:#e3f2fd
    style Iceberg fill:#fff8e1
    style DBT fill:#fce4ec
    style Analytics fill:#e8f5e9
```

---

## dbt model lineage (RFID branch only)

```mermaid
flowchart LR
    subgraph External["Outside dbt (Iceberg)"]
        SRC[rfid_events / src_iot_rfid_stream]
        BRZ[bronze.iot_rfid_stream]
        SLV[silver.iot_rfid_stream]
        GLD[gold.replenishment_signals]
    end

    subgraph Staging["staging"]
        STG[stg_iot_rfid_stream]
    end

    subgraph Intermediate["intermediate"]
        INT[int_latest_shelf_stock]
    end

    subgraph Marts["marts"]
        FCT[fct_replenishment_signals]
    end

    subgraph Exposures["exposures.yml"]
        DASH[supply_chain_executive_dashboard]
    end

    subgraph Metrics["Dashboard metrics"]
        M1[skus_needing_replenishment]
        M2[avg_urgency_score]
        M3[weather_risk_active]
        M4[inventory_turnover]
        M5[current_stock_on_shelf\nop_001 panel]
    end

    SRC --> BRZ --> SLV --> STG --> INT
    SLV --> GLD
    GLD --> FCT
    STG --> FCT
    FCT --> DASH
    INT -.->|operational path| M5
    FCT --> M4
    GLD --> EX[exec_001 SQL] --> M1 & M2 & M3
    DASH --> M1 & M2 & M3 & M4
```

---

## Node reference

| Stage | Object | Key fields from RFID | Implemented in |
|-------|--------|----------------------|----------------|
| **Source** | `rfid_events` | `product_id`, `current_stock_on_shelf`, `shelf_location`, `timestamp` | `real_time_stream.py`, `data_plane/ingestion/iot_stream_ingest.py` |
| **Bronze** | `bronze.iot_rfid_stream` | Raw append + envelope metadata | `BronzeWriter("src_iot_rfid_stream")` |
| **Silver** | `silver.iot_rfid_stream` | Typed, contract-validated stream | `SilverTransformer` for `src_iot_rfid_stream` |
| **Gold** | `gold.replenishment_signals` | `current_stock_on_shelf` (latest per product), `needs_replenishment`, `urgency_score`, `suggested_order_qty` | `GoldAggregator` — `latest_stock` from IoT Silver |
| **dbt staging** | `stg_iot_rfid_stream` | Parquet read of Silver IoT | `models/staging/stg_iot_rfid_stream.sql` |
| **dbt intermediate** | `int_latest_shelf_stock` | `ROW_NUMBER()` latest event per `product_id` | `models/intermediate/int_latest_shelf_stock.sql` |
| **dbt mart** | `fct_replenishment_signals` | At-risk SKUs + `inventory_turnover` | `models/marts/fct_replenishment_signals.sql` |
| **Exposure** | `supply_chain_executive_dashboard` | Grafana BI dependency | `models/exposures.yml` |

---

## Gold logic (RFID → replenishment)

`GoldAggregator` derives shelf state from the IoT Silver stream:

```
latest_stock = iot.sort_values("timestamp")
                 .drop_duplicates("product_id", keep="last")
                 [["product_id", "current_stock_on_shelf", "shelf_location"]]

needs_replenishment = current_stock_on_shelf < reorder_threshold   -- from warehouse Silver
urgency_score       = (reorder_threshold - current_stock_on_shelf) / max_capacity
```

RFID is the **speed layer** input; warehouse master provides thresholds and costs.

---

## Dashboard metrics fed by RFID lineage

| Metric / panel | SQL or model | RFID contribution |
|----------------|--------------|-------------------|
| **SKUs needing replenishment** | `exec_001` → `gold_replenishment_signals` | Stock level vs threshold from latest RFID |
| **Avg / max urgency score** | `exec_001` | Urgency derived from `current_stock_on_shelf` |
| **Suggested order units** | `exec_001` | Gap to `max_capacity` using RFID stock |
| **Weather risk flag** | `exec_001` | Combined in Gold (not from RFID) |
| **Inventory turnover** | `fct_replenishment_signals` / `metric_catalog` | `units_sold_7d / current_stock_on_shelf` |
| **Current shelf inventory** | `op_001` → `silver_iot_rfid_stream` | **Direct** latest RFID event per product |
| **Supplier bottlenecks** | `op_002` | Joins IoT latest stock + inventory transactions |

Prometheus series (`analytics_bi_metric`) are populated by `bi_kpis.py` running `exec_001` and `exec_002` after Gold refresh.

---

## Enterprise lineage contract

From `enterprise_contract_builder.py`:

```
src_iot_rfid_stream → bronze.iot_rfid_stream → silver.iot_rfid_stream
    → gold.replenishment_signals → grafana:supply-chain-bi
```

Edge label on Gold join: **`latest_by`** (most recent RFID event per product).

---

## Generate interactive dbt lineage graph

After parquet export and dbt install:

```powershell
docker compose -f docker-compose.part3.yml --profile tools run --rm semantic-dbt
# or locally:
cd semantic_plane/dbt/supply_chain_semantic
dbt docs generate
dbt docs serve
```

In **dbt docs**, open **`stg_iot_rfid_stream`** → follow edges to **`int_latest_shelf_stock`**, **`fct_replenishment_signals`**, and exposure **`supply_chain_executive_dashboard`**.

---

## Related SQL workloads

| Query ID | Reads RFID at | Purpose |
|----------|---------------|---------|
| `op_001` | Silver IoT | Near-real-time shelf inventory |
| `op_002` | Silver IoT + transactions | Warehouse bottlenecks |
| `exec_001` | Gold (RFID-derived stock) | Executive scorecard |
| `adhoc_001` | Silver IoT + sales | Exploratory low-stock / returns |

See `docs/part3/TASK3_SQL_WORKLOADS.md` and `data_plane/analytics/query_catalog.py`.
