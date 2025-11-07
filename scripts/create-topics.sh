#!/bin/bash
# create-topics.sh: Script để tạo các Kafka topics

echo "Waiting for Kafka to be ready..."

# Kiểm tra kết nối với Kafka Broker (sử dụng localhost vì script chạy bên trong container Kafka)
while ! nc -z localhost 9092; do   
  sleep 1
done

echo "Kafka is ready. Creating topics..."

BOOTSTRAP_SERVER="localhost:9092"

# Tạo topic ingest.media.events
/opt/bitnami/kafka/bin/kafka-topics.sh --create \
    --topic ingest.media.events \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

# Tạo topic model.inference.results
/opt/bitnami/kafka/bin/kafka-topics.sh --create \
    --topic model.inference.results \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

echo "Topics creation finished."

# THÊM: In ra danh sách các Topics đã tạo
echo "Created Topics:"
/opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server $BOOTSTRAP_SERVER
