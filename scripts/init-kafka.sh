#!/bin/bash

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server kafka:29092 --list; do
  echo "Kafka is not ready yet. Waiting..."
  sleep 5
done

echo "Kafka is ready!"

# Create the video-chunks topic if it doesn't exist
echo "Creating video-chunks topic..."
kafka-topics --bootstrap-server kafka:29092 \
  --create \
  --topic video-chunks \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo "Topic 'video-chunks' created successfully!"

# List all topics to verify
echo "Available topics:"
kafka-topics --bootstrap-server kafka:29092 --list
