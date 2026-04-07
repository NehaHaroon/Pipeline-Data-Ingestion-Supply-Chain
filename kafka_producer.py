from kafka import KafkaProducer
import json
import logging
from typing import Dict, Any
from datetime import datetime

log = logging.getLogger(__name__)

class IoTProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "supply_chain_inventory"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.topic = topic

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