import json
import time
import random
import uuid
import pandas as pd
import os
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

def create_producer_with_retry(max_retries: int = 15):
    """Create KafkaProducer with exponential backoff retry logic."""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    for attempt in range(max_retries):
        try:
            print(f"🔗 Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
            print(f"   Bootstrap servers: {bootstrap_servers}")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(3, 0, 0),
                request_timeout_ms=5000,
                connections_max_idle_ms=30000,
                max_request_size=10485760,
                retries=3,
                linger_ms=10
            )
            print("✅ Successfully connected to Kafka")
            return producer
        except NoBrokersAvailable as e:
            wait_time = min(2 ** attempt, 10)  # Exponential backoff, capped at 10 seconds
            if attempt < max_retries - 1:
                print(f"⚠️  Failed to connect to Kafka: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"❌ Failed to connect to Kafka after {max_retries} attempts")
                raise

producer = create_producer_with_retry()

# Load the actual Product IDs from the file we just made
try:
    PRODUCT_IDS = pd.read_csv("storage/raw/warehouse_master.csv")['product_id'].tolist()
except:
    print("Error: warehouse_master.csv not found. Run the generator first.")
    exit()

def stream_rfid_data():
    print(f"📡 RFID Sensors Online. Tracking {len(PRODUCT_IDS)} products. Press Ctrl+C to pause.")
    while True:
        p_id = random.choice(PRODUCT_IDS)
        t3 = datetime.utcnow().isoformat()
        
        data = {
            "event_id": str(uuid.uuid4()),
            "timestamp": t3,
            "product_id": p_id,
            "shelf_location": random.choice(["ZONE-A", "ZONE-B", "ZONE-C"]),
            "current_stock_on_shelf": random.randint(5, 150),
            "battery_level": f"{random.randint(10, 100)}%"
        }

        # Simulating duplicate sensor pings (Management Policy Test)
        if random.random() < 0.03:
            for _ in range(2): producer.send('supply_chain_inventory', value=data)
        else:
            producer.send('supply_chain_inventory', value=data)
            
        print(f"PING: {p_id} | On-Shelf: {data['current_stock_on_shelf']}")
        time.sleep(1)

if __name__ == "__main__":
    stream_rfid_data()
