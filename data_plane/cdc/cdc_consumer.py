"""Consumes Debezium CDC events and forwards normalized records to ingestion API."""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

# Setup path and logging
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from common import setup_logging, build_bootstrap_candidates, send_records_to_api, build_api_url
from kafka import KafkaConsumer
from kafka.consumer.subscription_state import ConsumerRebalanceListener

log = setup_logging("cdc_consumer")

from control_plane.entities import (  # noqa: E402
    EventEnvelope,
    INVENTORY_TRANSACTIONS_DATASET,
    INVENTORY_TRANSACTIONS_SOURCE,
    OperationType,
)


@dataclass
class ConsumerConfig:
    """Runtime configuration for CDC consumer execution."""

    bootstrap_servers: List[str]
    topic: str
    group_id: str
    api_url: str
    api_token: str


class LoggingRebalanceListener(ConsumerRebalanceListener):
    """Logs consumer group assignment changes for operational visibility."""

    def on_partitions_revoked(self, revoked):
        print(f"[CDC] Partitions revoked: {list(revoked)}")

    def on_partitions_assigned(self, assigned):
        print(f"[CDC] Partitions assigned: {list(assigned)}")


def _load_config() -> ConsumerConfig:
    
    load_dotenv(override=False)

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("CDC_TOPIC", "supply_chain.public.inventory_transactions")
    group_id = os.getenv("CDC_GROUP_ID", "cdc-ingestion-group")
    base_api_url = os.getenv("INGESTION_API_URL", "http://ingestion-api:8000")
    api_url = build_api_url(base_api_url, "src_inventory_transactions")
    api_token = os.getenv("API_TOKEN", "")
    print(f"[CDC] Loaded config: bootstrap_servers={bootstrap_servers}, topic={topic}, group_id={group_id}, api_url={api_url} api_token={api_token}")
    if not api_token:
        raise RuntimeError("API_TOKEN is required for authenticated ingestion.")
    return ConsumerConfig(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        api_url=api_url,
        api_token=api_token,
    )


def _map_operation(op_code: Optional[str]) -> OperationType:
    mapping = {
        "c": OperationType.INSERT,
        "u": OperationType.UPDATE,
        "d": OperationType.DELETE,
        "r": OperationType.SNAPSHOT,
    }
    return mapping.get((op_code or "").lower(), OperationType.SNAPSHOT)


def _extract_business_payload(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    excluded_prefixes = {"__", "_event_", "_source_", "_dataset_", "_schema_", "_operation_"}
    payload: Dict[str, Any] = {}
    for key, value in raw_record.items():
        if any(key.startswith(prefix) for prefix in excluded_prefixes):
            continue
        payload[key] = value
    return payload

def _forward_to_ingestion_api(config: ConsumerConfig, envelope: EventEnvelope) -> None:
    """Forward event envelope to ingestion API using centralized utility."""
    records = [envelope.to_dict()]
    success = send_records_to_api(records, config.api_url, config.api_token)
    if success:
        print(f"[CDC] Successfully sent event envelope to API")
    else:
        print(f"[CDC] Failed to send event envelope to API")


def _forward_batch_to_ingestion_api(config: ConsumerConfig, envelopes: List[EventEnvelope]) -> None:
    if not envelopes:
        return
    records = [env.to_dict() for env in envelopes]
    success = send_records_to_api(records, config.api_url, config.api_token)
    if success:
        print(f"[CDC] Successfully sent batch to API | batch_size={len(records)}")
    else:
        print(f"[CDC] Failed to send batch to API | batch_size={len(records)}")


def run_consumer() -> None:
    """Main loop consuming Debezium topic and forwarding to ingestion API."""
    config = _load_config()
    print(f"[CDC] Connecting to Kafka using bootstrap servers: {config.bootstrap_servers}")
    consumer = KafkaConsumer(
        bootstrap_servers=config.bootstrap_servers,
        group_id=config.group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        consumer_timeout_ms=60000,
        max_poll_interval_ms=300000,
        session_timeout_ms=30000,
    )
    listener = LoggingRebalanceListener()
    consumer.subscribe([config.topic], listener=listener)
    print(f"[CDC] Subscribed to topic: {config.topic}")

    try:
        while True:
            polled_records = consumer.poll(timeout_ms=1000)
            if not polled_records:
                time.sleep(0.1)
                continue

            for topic_partition, messages in polled_records.items():
                _ = topic_partition
                batch_envelopes: List[EventEnvelope] = []
                for message in messages:
                    raw_value = message.value or {}
                    op_code = raw_value.get("__op") or raw_value.get("op")
                    source_ts_ms = raw_value.get("__source_ts_ms") or raw_value.get("source.ts_ms")
                    operation_type = _map_operation(op_code)
                    payload = _extract_business_payload(raw_value)

                    source_timestamp: Optional[str] = None
                    if source_ts_ms is not None:
                        source_timestamp = datetime.fromtimestamp(
                            int(source_ts_ms) / 1000.0, tz=timezone.utc
                        ).isoformat()

                    envelope = EventEnvelope(
                        payload=payload,
                        source_id=INVENTORY_TRANSACTIONS_SOURCE.source_id,
                        dataset_id=INVENTORY_TRANSACTIONS_DATASET.dataset_id,
                        schema_version=INVENTORY_TRANSACTIONS_DATASET.schema_version,
                        operation_type=operation_type,
                        event_timestamp=datetime.now(timezone.utc).isoformat(),
                        source_timestamp=source_timestamp,
                    )
                    batch_envelopes.append(envelope)
                    print(
                        "[CDC] op={op} txn_id={txn} product={product}".format(
                            op=operation_type.value,
                            txn=payload.get("transaction_id", "UNKNOWN"),
                            product=payload.get("product_id", "UNKNOWN"),
                        )
                    )
                _forward_batch_to_ingestion_api(config, batch_envelopes)
    except KeyboardInterrupt:
        print("[CDC] Shutdown requested by user.")
    finally:
        consumer.close()
        print("[CDC] Consumer closed cleanly.")


if __name__ == "__main__":
    run_consumer()
