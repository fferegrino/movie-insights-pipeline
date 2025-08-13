#!/bin/bash

# MinIO Bucket Initialization Script
# This script creates buckets and sets policies for the MinIO server

set -e

# Configuration
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"
ALIAS_NAME="minio-server"

# Buckets to create (add or remove as needed)
BUCKETS=(
    "$RAW_DATA_BUCKET"
)

# Wait for MinIO to be ready
echo "Waiting for MinIO server to be ready..."
until mc alias set $ALIAS_NAME $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY > /dev/null 2>&1; do
    echo "MinIO not ready yet, waiting..."
    sleep 5
done

echo "MinIO server is ready!"

# Create buckets
echo "Creating buckets..."
for bucket in "${BUCKETS[@]}"; do
    echo "Creating bucket: $bucket"
    mc mb $ALIAS_NAME/$bucket --ignore-existing
done

# Set bucket policies (optional - customize as needed)
echo "Setting bucket policies..."
for bucket in "${BUCKETS[@]}"; do
    mc policy set public $ALIAS_NAME/$bucket
done

# List all buckets
echo "Listing all buckets:"
mc ls $ALIAS_NAME

echo "Bucket initialization completed successfully!"
