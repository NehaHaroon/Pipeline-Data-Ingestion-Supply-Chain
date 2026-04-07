import os
from typing import Dict, Any

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

# API settings
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "8000"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Database settings (for production, replace with real DB)
DB_TYPE = os.getenv("DB_TYPE", "parquet")  # parquet, postgres, etc.

# Weather API key (for external API source)
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "your_api_key_here")

# Batch settings
BATCH_SIZE_DEFAULT = int(os.getenv("BATCH_SIZE_DEFAULT", "1000"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# CDC settings
CDC_CHECKPOINT_INTERVAL = int(os.getenv("CDC_CHECKPOINT_INTERVAL", "500"))

# Telemetry settings
TELEMETRY_HEARTBEAT_INTERVAL = int(os.getenv("TELEMETRY_HEARTBEAT_INTERVAL", "5"))  # seconds