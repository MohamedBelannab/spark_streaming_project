import json
import time
import pandas as pd
from kafka import KafkaProducer

# Configuration
KAFKA_TOPIC = 'clickstream'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def run_producer():
    print(f"📡 Producer started. Streaming to Kafka...")
    try:
        df = pd.read_csv('data/events.csv')
        for _, row in df.iterrows():
            message = row.to_dict()
            producer.send(KAFKA_TOPIC, value=message)
            print(f"✅ Sent: {message}")
            time.sleep(1) # Send one event per second
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_producer()