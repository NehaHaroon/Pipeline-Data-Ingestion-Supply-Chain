from kafka import KafkaConsumer
import json
import logging
from typing import Callable, Dict, Any
from control_plane.contracts import CONTRACT_REGISTRY
from observability_plane.telemetry import JobTelemetry

log = logging.getLogger(__name__)

class IoTConsumer:
    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "supply_chain_inventory", group_id: str = "supply_chain_group"):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        self.source_id = "src_iot_rfid_stream"
        self.contract = CONTRACT_REGISTRY[self.source_id]
        self.telemetry = JobTelemetry(job_id="streaming_job", source_id=self.source_id)

    def consume_and_process(self, process_callback: Callable[[Dict[str, Any]], None]):
        self.telemetry.start()
        try:
            for message in self.consumer:
                event = message.value
                log.info(f"Received event: {event}")

                # Enforce contract
                result = self.contract.enforce(event)
                if result["status"] in ["ok", "coerced"]:
                    process_callback(result["record"])
                    self.telemetry.records_ingested += 1
                elif result["status"] == "quarantine":
                    self.telemetry.records_quarantined += 1
                else:
                    self.telemetry.records_failed += 1

        except KeyboardInterrupt:
            log.info("Stopping consumer")
        finally:
            self.telemetry.end()
            self.consumer.close()

# Usage example
if __name__ == "__main__":
    def process_record(record):
        print(f"Processing: {record}")

    consumer = IoTConsumer()
    consumer.consume_and_process(process_record)