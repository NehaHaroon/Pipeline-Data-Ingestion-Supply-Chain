# Last Updated: 2026-04-05
# Phase 1 — Task 1: Core entity classes for the Control Plane.
# Defines the "what" and "how" of every data source, dataset, job, and record envelope.

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


# ─────────────────────────────────────────────
# ENUMERATIONS
# ─────────────────────────────────────────────

class SourceType(Enum):
    DB     = "db"
    API    = "api"
    STREAM = "stream"
    FILE   = "file"

class ExtractionMode(Enum):
    PULL  = "pull"
    PUSH  = "push"
    QUERY = "query"

class ChangeCaptureMode(Enum):
    FULL_SNAPSHOT     = "full_snapshot"
    INCREMENTAL       = "incremental"
    CDC_LOG_BASED     = "cdc_log_based"
    CDC_TRIGGER_BASED = "cdc_trigger_based"
    STREAM_EVENT      = "stream_event"

class IngestionFrequency(Enum):
    REAL_TIME = "real_time"
    HOURLY    = "hourly"
    DAILY     = "daily"
    WEEKLY    = "weekly"
    ON_DEMAND = "on_demand"

class ClassificationLevel(Enum):
    PUBLIC       = "public"
    INTERNAL     = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED   = "restricted"

class ExecutionMode(Enum):
    BATCH          = "batch"
    MICRO_BATCH    = "micro_batch"
    STREAMING      = "streaming"
    CDC_CONTINUOUS = "cdc_continuous"

class OperationType(Enum):
    INSERT   = "INSERT"
    UPDATE   = "UPDATE"
    DELETE   = "DELETE"
    SNAPSHOT = "SNAPSHOT"


# ─────────────────────────────────────────────
# ENTITY: DataSource
# Justification: separating source metadata from job logic lets us reuse
# the same source definition across multiple jobs and detect schema drift.
# ─────────────────────────────────────────────
@dataclass
class DataSource:
    source_id:           str
    name:                str
    source_type:         SourceType
    extraction_mode:     ExtractionMode
    change_capture_mode: ChangeCaptureMode
    ingestion_frequency: IngestionFrequency
    connection_info:     Dict[str, Any]
    expected_schema:     Dict[str, str]   # col_name -> dtype string
    tags:                List[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.source_id:
            raise ValueError("source_id cannot be empty.")

    def __repr__(self):
        return (f"DataSource(id={self.source_id}, type={self.source_type.value}, "
                f"capture={self.change_capture_mode.value}, freq={self.ingestion_frequency.value})")


# ─────────────────────────────────────────────
# ENTITY: Dataset
# Justification: explicit versioned dataset objects enforce schema
# versioning and retention rules before data lands in storage.
# ─────────────────────────────────────────────
@dataclass
class Dataset:
    dataset_id:           str
    name:                 str
    domain:               str
    classification_level: ClassificationLevel
    schema_version:       str
    retention_policy:     str
    owner:                str
    description:          str = ""

    def __repr__(self):
        return (f"Dataset(id={self.dataset_id}, domain={self.domain}, "
                f"version={self.schema_version}, sensitivity={self.classification_level.value})")


# ─────────────────────────────────────────────
# ENTITY: IngestionJob
# Justification: decoupling job config from execution lets us schedule,
# retry, or replay jobs without touching business logic.
# ─────────────────────────────────────────────
@dataclass
class IngestionJob:
    job_id:          str
    source_id:       str
    dataset_id:      str
    execution_mode:  ExecutionMode
    batch_size:      Optional[int] = None
    max_retries:     int = 3
    timeout_seconds: int = 3600
    created_at:      str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def __repr__(self):
        return (f"IngestionJob(id={self.job_id}, source={self.source_id}, "
                f"dataset={self.dataset_id}, mode={self.execution_mode.value})")


# ─────────────────────────────────────────────
# ENTITY: EventEnvelope
# Justification: every record gets a wrapper for lineage, auditability,
# schema evolution, and replay. Without this the data lake becomes a swamp.
# ─────────────────────────────────────────────
@dataclass
class EventEnvelope:
    payload:            Dict[str, Any]
    source_id:          str
    dataset_id:         str
    schema_version:     str
    operation_type:     OperationType
    event_id:           str = field(default_factory=lambda: str(uuid.uuid4()))
    ingestion_timestamp:str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    trace_id:           str = field(default_factory=lambda: str(uuid.uuid4()))
    event_timestamp:    Optional[str] = None
    source_timestamp:   Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "_event_id":            self.event_id,
            "_trace_id":            self.trace_id,
            "_source_id":           self.source_id,
            "_dataset_id":          self.dataset_id,
            "_schema_version":      self.schema_version,
            "_operation_type":      self.operation_type.value,
            "_event_timestamp":     self.event_timestamp,
            "_source_timestamp":    self.source_timestamp,
            "_ingestion_timestamp": self.ingestion_timestamp,
            **self.payload
        }

    def __repr__(self):
        return (f"EventEnvelope(event_id={self.event_id[:8]}..., "
                f"op={self.operation_type.value}, source={self.source_id})")


# ─────────────────────────────────────────────
# SOURCE REGISTRY — pre-configured for this project
# ─────────────────────────────────────────────

WAREHOUSE_SOURCE = DataSource(
    source_id="src_warehouse_master",
    name="Warehouse Master",
    source_type=SourceType.FILE,
    extraction_mode=ExtractionMode.PULL,
    change_capture_mode=ChangeCaptureMode.FULL_SNAPSHOT,
    ingestion_frequency=IngestionFrequency.DAILY,
    connection_info={"path": "storage/raw/warehouse_master.csv"},
    expected_schema={
        "product_id": "str", "article_id": "str", "product_name": "str",
        "color": "str", "size": "str", "reorder_threshold": "int",
        "max_capacity": "int", "unit_cost": "float"
    },
    tags=["supply-chain", "master-data", "batch"]
)

MANUFACTURING_SOURCE = DataSource(
    source_id="src_manufacturing_logs",
    name="Manufacturing Logs",
    source_type=SourceType.FILE,
    extraction_mode=ExtractionMode.PULL,
    change_capture_mode=ChangeCaptureMode.INCREMENTAL,
    ingestion_frequency=IngestionFrequency.DAILY,
    connection_info={"path": "storage/raw/manufacturing_logs.csv"},
    expected_schema={
        "production_batch_id": "str", "product_id": "str",
        "mfg_timestamp": "datetime", "quantity_produced": "int", "defect_count": "float"
    },
    tags=["supply-chain", "manufacturing", "batch"]
)

SALES_SOURCE = DataSource(
    source_id="src_sales_history",
    name="Sales History",
    source_type=SourceType.FILE,
    extraction_mode=ExtractionMode.PULL,
    change_capture_mode=ChangeCaptureMode.INCREMENTAL,
    ingestion_frequency=IngestionFrequency.DAILY,
    connection_info={"path": "storage/raw/sales_history.csv"},
    expected_schema={
        "receipt_id": "str", "product_id": "str",
        "sale_timestamp": "datetime", "units_sold": "int", "store_id": "str"
    },
    tags=["supply-chain", "sales", "batch"]
)

LEGACY_SOURCE = DataSource(
    source_id="src_legacy_trends",
    name="Legacy Trends",
    source_type=SourceType.FILE,
    extraction_mode=ExtractionMode.PULL,
    change_capture_mode=ChangeCaptureMode.FULL_SNAPSHOT,
    ingestion_frequency=IngestionFrequency.WEEKLY,
    connection_info={"path": "storage/raw/legacy_trends.csv"},
    expected_schema={
        "old_product_code": "str", "historical_period": "str",
        "total_monthly_sales": "int", "market_region": "str"
    },
    tags=["supply-chain", "legacy", "analytics", "batch"]
)

IOT_SOURCE = DataSource(
    source_id="src_iot_rfid_stream",
    name="IoT RFID Stream",
    source_type=SourceType.STREAM,
    extraction_mode=ExtractionMode.PUSH,
    change_capture_mode=ChangeCaptureMode.STREAM_EVENT,
    ingestion_frequency=IngestionFrequency.REAL_TIME,
    connection_info={"bootstrap_servers": "localhost:9092", "topic": "supply_chain_inventory"},
    expected_schema={
        "event_id": "str", "timestamp": "datetime", "product_id": "str",
        "shelf_location": "str", "current_stock_on_shelf": "int", "battery_level": "str"
    },
    tags=["supply-chain", "iot", "streaming", "real-time"]
)

WEATHER_API_SOURCE = DataSource(
    source_id="src_weather_api",
    name="Weather API",
    source_type=SourceType.API,
    extraction_mode=ExtractionMode.PULL,
    change_capture_mode=ChangeCaptureMode.INCREMENTAL,
    ingestion_frequency=IngestionFrequency.HOURLY,
    connection_info={"url": "https://api.openweathermap.org/data/2.5/weather", "params": {"q": "Lahore", "appid": "your_api_key"}},
    expected_schema={
        "city": "str", "temperature": "float", "humidity": "int", "weather_description": "str", "timestamp": "datetime"
    },
    tags=["external", "weather", "api", "pull"]
)

ALL_SOURCES = [WAREHOUSE_SOURCE, MANUFACTURING_SOURCE, SALES_SOURCE, LEGACY_SOURCE, IOT_SOURCE, WEATHER_API_SOURCE]

# ─────────────────────────────────────────────
# DATASET REGISTRY
# ─────────────────────────────────────────────

WAREHOUSE_DATASET = Dataset(
    dataset_id="ds_warehouse_master",
    name="Warehouse Master Dataset",
    domain="supply_chain",
    classification_level=ClassificationLevel.INTERNAL,
    schema_version="v1",
    retention_policy="forever",
    owner="data-engineering-team",
    description="Product catalog with reorder thresholds and capacity limits."
)

MANUFACTURING_DATASET = Dataset(
    dataset_id="ds_manufacturing_logs",
    name="Manufacturing Logs Dataset",
    domain="supply_chain",
    classification_level=ClassificationLevel.INTERNAL,
    schema_version="v1",
    retention_policy="365 days",
    owner="data-engineering-team",
    description="Production batch records with defect counts."
)

SALES_DATASET = Dataset(
    dataset_id="ds_sales_history",
    name="Sales History Dataset",
    domain="supply_chain",
    classification_level=ClassificationLevel.CONFIDENTIAL,
    schema_version="v1",
    retention_policy="365 days",
    owner="data-engineering-team",
    description="POS transaction records across all retail stores."
)

LEGACY_DATASET = Dataset(
    dataset_id="ds_legacy_trends",
    name="Legacy Trends Dataset",
    domain="supply_chain",
    classification_level=ClassificationLevel.INTERNAL,
    schema_version="v1",
    retention_policy="forever",
    owner="data-engineering-team",
    description="Historical monthly sales for seasonality analysis."
)

IOT_DATASET = Dataset(
    dataset_id="ds_iot_rfid_stream",
    name="IoT RFID Stream Dataset",
    domain="supply_chain",
    classification_level=ClassificationLevel.INTERNAL,
    schema_version="v1",
    retention_policy="90 days",
    owner="data-engineering-team",
    description="Real-time shelf inventory pings from RFID sensors."
)

WEATHER_DATASET = Dataset(
    dataset_id="ds_weather_api",
    name="Weather API Dataset",
    domain="external",
    classification_level=ClassificationLevel.PUBLIC,
    schema_version="v1",
    retention_policy="30 days",
    owner="data-engineering-team",
    description="Hourly weather data for supply chain planning."
)

ALL_DATASETS = [WAREHOUSE_DATASET, MANUFACTURING_DATASET, SALES_DATASET, LEGACY_DATASET, IOT_DATASET, WEATHER_DATASET]
