
# Download Kafka by running the command below.
#wget https://archive.apache.org/dist/kafka/3.7.0/kafka_2.12-3.7.0.tgz


# Extract Kafka from the zip file by running the command below. 
#tar -xzf kafka_2.12-3.7.0.tgz


# Change to the kafka_2.12-3.7.0 directory.
#cd kafka_2.12-3.7.0


# Generate a cluster UUID that will uniquely identify the Kafka cluster.
#KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"


# KRaft requires the log directories to be configured. Run the following command to configure the log directories passing the cluster id.
#bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties


# Now that KRaft is configured, you can start the Kafka server by running the following command.
#bin/kafka-server-start.sh config/kraft/server.properties


#MySQL
#mysql --host=mysql --port=3306 --user=root --password=Replace your password
#create database tolldata;
#use tolldata;
#create table livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);
#exit