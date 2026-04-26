"""
Centralized Kafka connection and retry logic to avoid duplication.
"""

import json
import logging
import time
from typing import List

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

log = logging.getLogger(__name__)


def build_bootstrap_candidates(bootstrap_servers: str) -> List[str]:
    """
    Build a list of Kafka bootstrap server candidates with fallbacks.
    
    Args:
        bootstrap_servers: Comma-separated list of bootstrap servers
    
    Returns:
        List of bootstrap server candidates with fallbacks
    """
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


def connect_kafka_consumer(
    topic: str,
    bootstrap_servers: str,
    group_id: str,
    max_retries: int = 15
) -> KafkaConsumer:
    """
    Connect to Kafka consumer with exponential backoff retry logic.
    
    Args:
        topic: Kafka topic to consume
        bootstrap_servers: Comma-separated bootstrap servers
        group_id: Consumer group ID
        max_retries: Maximum connection attempts
    
    Returns:
        Connected KafkaConsumer instance
    
    Raises:
        NoBrokersAvailable: If connection fails after max_retries
    """
    bootstrap_candidates = build_bootstrap_candidates(bootstrap_servers)
    
    for attempt in range(max_retries):
        try:
            log.info(
                f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries}) "
                f"using {bootstrap_candidates}"
            )
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_candidates,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                api_version=(3, 0, 0),
                request_timeout_ms=5000,
                connections_max_idle_ms=30000,
                value_deserializer=lambda m: m.decode('utf-8') if m else None,
            )
            log.info("Successfully connected to Kafka consumer")
            return consumer
        except NoBrokersAvailable as e:
            wait_time = min(2 ** attempt, 10)  # Exponential backoff, capped at 10 seconds
            if attempt < max_retries - 1:
                log.warning(f"Failed to connect to Kafka: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                log.error(f"Failed to connect to Kafka after {max_retries} attempts")
                raise


def connect_kafka_producer(
    bootstrap_servers: str,
    max_retries: int = 15
) -> KafkaProducer:
    """
    Connect to Kafka producer with exponential backoff retry logic.
    
    Args:
        bootstrap_servers: Comma-separated bootstrap servers
        max_retries: Maximum connection attempts
    
    Returns:
        Connected KafkaProducer instance
    
    Raises:
        NoBrokersAvailable: If connection fails after max_retries
    """
    bootstrap_candidates = build_bootstrap_candidates(bootstrap_servers)
    
    for attempt in range(max_retries):
        try:
            log.info(
                f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries}) "
                f"using {bootstrap_candidates}"
            )
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_candidates,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                api_version=(3, 0, 0),
                request_timeout_ms=5000,
                connections_max_idle_ms=30000,
                max_request_size=10485760,
                retries=3
            )
            log.info("Successfully connected to Kafka producer")
            return producer
        except NoBrokersAvailable as e:
            wait_time = min(2 ** attempt, 10)  # Exponential backoff, capped at 10 seconds
            if attempt < max_retries - 1:
                log.warning(f"Failed to connect to Kafka: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                log.error(f"Failed to connect to Kafka after {max_retries} attempts")
                raise
