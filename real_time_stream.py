import json
import time
import random
import uuid
import pandas as pd
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load the actual Product IDs from the file we just made
try:
    PRODUCT_IDS = pd.read_csv("warehouse_master.csv")['product_id'].tolist()
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
