from kafka import KafkaConsumer
import json

# Create Consumer
consumer = KafkaConsumer(
    'bankbranch',
    bootstrap_servers="localhost:9092",
    group_id='atm-app',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🎧 Consumer started. Waiting for messages...")

# Read messages
for msg in consumer:
    print(f"📨 Received: {msg.value}")
