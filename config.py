import os
from pathlib import Path
from typing import Dict, Any

# Load .env file if present, to support local development and container startup
def load_env_file(env_path: str = None):
    env_file = Path(env_path) if env_path else Path(__file__).resolve().parent / ".env"
    if not env_file.exists():
        return
    with env_file.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value

load_env_file()

# Configuration for the Supply Chain Ingestion Pipeline

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_IOT = os.getenv("KAFKA_TOPIC_IOT", "supply_chain_inventory")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "supply_chain_group")

# Storage paths
STORAGE_RAW = os.getenv("STORAGE_RAW", "storage/raw")
STORAGE_INGESTED = os.getenv("STORAGE_INGESTED", "storage/ingested")
STORAGE_QUARANTINE = os.getenv("STORAGE_QUARANTINE", "storage/quarantine")
STORAGE_CDC_LOG = os.getenv("STORAGE_CDC_LOG", "storage/cdc_log")
STORAGE_CHECKPOINTS = os.getenv("STORAGE_CHECKPOINTS", "storage/checkpoints")
STORAGE_SILVER = os.getenv("STORAGE_SILVER", "storage/silver")
STORAGE_GOLD = os.getenv("STORAGE_GOLD", "storage/gold")
STORAGE_ICEBERG_WAREHOUSE = os.getenv("STORAGE_ICEBERG_WAREHOUSE", "storage/iceberg_warehouse")
STORAGE_ICEBERG_CATALOG_URI = os.getenv("STORAGE_ICEBERG_CATALOG_URI", "sqlite:///storage/iceberg_catalog.db")
ICEBERG_NAMESPACE_BRONZE = os.getenv("ICEBERG_NAMESPACE_BRONZE", "bronze")
ICEBERG_NAMESPACE_SILVER = os.getenv("ICEBERG_NAMESPACE_SILVER", "silver")
ICEBERG_NAMESPACE_GOLD = os.getenv("ICEBERG_NAMESPACE_GOLD", "gold")

# API settings
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))
API_TOKEN = os.getenv("API_TOKEN", "ee910d618e617c559f1ca41a3a48c3c7")
LOCAL_API_HOST = os.getenv("LOCAL_API_HOST","localhost")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Database settings (for production, replace with real DB)
DB_TYPE = os.getenv("DB_TYPE", "parquet")  # parquet, postgres, etc.

# Batch settings
BATCH_SIZE_DEFAULT = int(os.getenv("BATCH_SIZE_DEFAULT", "1000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# CDC settings
CDC_CHECKPOINT_INTERVAL = int(os.getenv("CDC_CHECKPOINT_INTERVAL", "500"))

# Telemetry settings
TELEMETRY_HEARTBEAT_INTERVAL = int(os.getenv("TELEMETRY_HEARTBEAT_INTERVAL", "5"))  # seconds
TRANSFORMATION_KPI_LOG_PATH = os.getenv("TRANSFORMATION_KPI_LOG_PATH", "storage/ingested/detail_logs/transformation_kpis.jsonl")


INGESTION_API_URL=os.getenv("INGESTION_API_URL","http://ingestion-api:8000")
