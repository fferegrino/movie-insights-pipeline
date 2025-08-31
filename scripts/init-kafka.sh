#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server kafka:29092 --list; do
  echo "Kafka is not ready yet. Waiting..."
  sleep 5
done

echo "Kafka is ready!"

topics=(
  "raw-videos"
  "video-chunks"
  "scenes"
)

for topic in "${topics[@]}"; do
  echo "Creating $topic topic..."
  kafka-topics --bootstrap-server kafka:29092 \
    --create \
    --topic $topic \
    --partitions 1 \
    --replication-factor 1 \
    --if-not-exists

  echo "Topic '$topic' created successfully!"
done

# List all topics to verify
echo "Available topics:"
kafka-topics --bootstrap-server kafka:29092 --list
