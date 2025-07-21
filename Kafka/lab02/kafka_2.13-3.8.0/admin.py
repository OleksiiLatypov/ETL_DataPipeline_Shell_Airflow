#KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
#bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
#bin/kafka-server-start.sh config/kraft/server.properties

from kafka.admin import KafkaAdminClient,NewTopic

admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id='test')
topic_list = []
new_topic = NewTopic(name="bankbranch", num_partitions= 2, replication_factor=1)
topic_list.append(new_topic)
admin_client.create_topics(new_topics=topic_list)