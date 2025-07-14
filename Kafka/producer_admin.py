from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka import KafkaProducer
import json

# Create Kafka topic
admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='test'
)

new_topic = NewTopic(name="bankbranch", num_partitions=2, replication_factor=1)

# Attempt topic creation
try:
    admin_client.create_topics(new_topics=[new_topic], validate_only=False)
    print("✅ Topic 'bankbranch' created.")
except Exception as e:
    print(f"⚠️ Topic creation skipped or failed: {e}")

# Optional: inspect topic configs
configs = admin_client.describe_configs(
    config_resources=[ConfigResource(ConfigResourceType.TOPIC, "bankbranch")]
)
print("🔍 Topic Configs:", configs)

# Create Producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send messages
producer.send("bankbranch", {'atmid': 1, 'transid': 100})
producer.send("bankbranch", {'atmid': 2, 'transid': 101})
producer.flush()
print("🚀 Messages sent.")
