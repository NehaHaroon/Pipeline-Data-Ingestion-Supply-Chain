from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import logging
import time
import os
from typing import Callable, Dict, Any, List
import config
from control_plane.contracts import CONTRACT_REGISTRY
from observability_plane.telemetry import JobTelemetry

log = logging.getLogger(__name__)

class IoTConsumer:
    def __init__(self, bootstrap_servers: str = None, topic: str = "supply_chain_inventory", group_id: str = "supply_chain_group", max_retries: int = 15):
        if bootstrap_servers is None:
            bootstrap_servers = config.KAFKA_BOOTSTRAP_SERVERS
        self.consumer = self._connect_with_retry(topic, bootstrap_servers, group_id, max_retries)
        self.source_id = "src_iot_rfid_stream"
        self.contract = CONTRACT_REGISTRY[self.source_id]
        self.telemetry = JobTelemetry(job_id="streaming_job", source_id=self.source_id)

    def _build_bootstrap_candidates(self, bootstrap_servers: str) -> List[str]:
        raw_servers = [server.strip() for server in bootstrap_servers.split(",") if server.strip()]
        candidates: List[str] = []
        for server in raw_servers:
            if server not in candidates:
                candidates.append(server)

        # If docker DNS host leaks into local runs, add host-network fallbacks.
        if any(server.startswith("kafka:") for server in candidates):
            for fallback in ["localhost:9092", "127.0.0.1:9092"]:
                if fallback not in candidates:
                    candidates.append(fallback)
        return candidates

    def _connect_with_retry(self, topic: str, bootstrap_servers: str, group_id: str, max_retries: int):
        """Connect to Kafka with exponential backoff retry logic."""
        bootstrap_candidates = self._build_bootstrap_candidates(bootstrap_servers)
        for attempt in range(max_retries):
            try:
                log.info(
                    "Attempting to connect to Kafka (attempt %s/%s) using %s",
                    attempt + 1,
                    max_retries,
                    bootstrap_candidates,
                )
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=bootstrap_candidates,
                    group_id=group_id,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    key_deserializer=lambda k: k.decode('utf-8') if k else None,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    # api_version=(3, 0, 0),
                    request_timeout_ms=15000,
                    session_timeout_ms=10000,
                    connections_max_idle_ms=30000,
                    fetch_max_bytes=10485760,
                    max_partition_fetch_bytes=1048576
                )
                log.info("Successfully connected to Kafka")
                return consumer
            except NoBrokersAvailable as e:
                wait_time = min(2 ** attempt, 10)  # Exponential backoff, capped at 10 seconds
                if attempt < max_retries - 1:
                    log.warning(f"Failed to connect to Kafka: {e}. Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    log.error(f"Failed to connect to Kafka after {max_retries} attempts")
                    raise

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
            self.telemetry.log_report()
            self.consumer.close()

# Usage example
if __name__ == "__main__":
    def process_record(record):
        print(f"Processing: {record}")
        
    consumer = IoTConsumer()
    consumer.consume_and_process(process_record)