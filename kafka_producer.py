from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import logging
import time
import os
from typing import Dict, Any
from datetime import datetime

log = logging.getLogger(__name__)

class IoTProducer:
    def __init__(self, bootstrap_servers: str = None, topic: str = "supply_chain_inventory", max_retries: int = 15):
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.producer = self._connect_with_retry(bootstrap_servers, max_retries)
        self.topic = topic

    def _connect_with_retry(self, bootstrap_servers: str, max_retries: int):
        """Connect to Kafka with exponential backoff retry logic."""
        for attempt in range(max_retries):
            try:
                log.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
                producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    api_version=(3, 0, 0),
                    request_timeout_ms=5000,
                    connections_max_idle_ms=30000,
                    max_request_size=10485760,
                    retries=3
                )
                log.info("Successfully connected to Kafka")
                return producer
            except NoBrokersAvailable as e:
                wait_time = min(2 ** attempt, 10)  # Exponential backoff, capped at 10 seconds
                if attempt < max_retries - 1:
                    log.warning(f"Failed to connect to Kafka: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    log.error(f"Failed to connect to Kafka after {max_retries} attempts")
                    raise

    def send_event(self, event: Dict[str, Any], key: str = None):
        try:
            future = self.producer.send(self.topic, value=event, key=key)
            record_metadata = future.get(timeout=10)
            log.info(f"Event sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
        except Exception as e:
            log.error(f"Failed to send event: {e}")

    def close(self):
        self.producer.close()

# Usage example
if __name__ == "__main__":
    producer = IoTProducer()
    event = {
        "event_id": "12345",
        "timestamp": datetime.now().isoformat(),
        "product_id": "ART-1001-MID-M",
        "shelf_location": "ZONE-A",
        "current_stock_on_shelf": 50,
        "battery_level": "85%"
    }
    producer.send_event(event, key="ART-1001-MID-M")
    producer.close()