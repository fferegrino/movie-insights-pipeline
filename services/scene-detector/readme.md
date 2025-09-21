# Scene Detector Service

A microservice that processes video chunks to detect scenes, extract keyframes, and assign unique identifiers to prevent duplicate scene detection across video processing sessions.

## Overview

The Scene Detector is part of a larger movie insights pipeline that analyzes video content to extract meaningful scenes. It operates as a Kafka consumer that:

1. **Receives video chunks** from a Kafka topic
2. **Downloads video files** from S3 storage
3. **Detects scenes** within each video chunk using computer vision
4. **Computes perceptual fingerprints** for scene keyframes
5. **Assigns unique scene IDs** with deduplication support
6. **Publishes scene information** back to Kafka

## Architecture

**TBA**

## Core Components

### 1. Scene Detection Pipeline

The main processing flow occurs in `main.py`:

- **SceneDetector**: Main orchestrator that consumes Kafka messages
- **Scene Detection**: Uses computer vision to identify scene boundaries
- **Fingerprint Generation**: Creates perceptual hashes for scene keyframes
- **ID Assignment**: Assigns unique identifiers with deduplication

### 2. ID Assignment System (`id_assigner.py`)

The `IdAssigner` class is the core component responsible for managing scene identifiers and preventing duplicates:

#### Purpose

- Assigns unique identifiers to video scenes
- Prevents duplicate scene detection across processing sessions
- Ensures consistent scene tracking using fingerprint-based matching

#### How It Works

The assignment process follows these steps:

1. **Fingerprint Matching**: Check if a similar scene already exists using perceptual hash comparison
2. **Temporal Overlap Check**: Verify that scenes have overlapping time boundaries (within overlap threshold)
3. **ID Reuse**: If both fingerprint similarity AND temporal overlap criteria are met, reuse the existing scene ID
4. **Temporal Merging**: When scenes are merged, expand their time boundaries to encompass both occurrences
5. **New ID Generation**: If no match is found (either no similar fingerprint OR no temporal overlap), generate a new UUID
6. **Storage Update**: Update the scene index with the assigned ID and metadata

#### Key Features

- **Dual-Criteria Matching**: Scenes must match on BOTH fingerprint similarity AND temporal overlap
- **Deduplication**: Similar scenes (based on fingerprints + time overlap) receive the same ID
- **Temporal Merging**: When scenes are merged, their time boundaries are expanded
- **Consistent Tracking**: Same scenes get same IDs across different processing runs
- **False Positive Prevention**: Temporal overlap check prevents matching scenes that occur at completely different times

### 3. Redis Scene Index (`redis_scene_index.py`)

The `RedisSceneIndex` class provides the storage backend for scene fingerprints and metadata:

#### Purpose

- Stores scene fingerprints in Redis for fast similarity matching
- Provides efficient retrieval and comparison of scene data
- Manages scene metadata (timing, IDs) alongside fingerprints

#### Data Structure

Redis uses hash structures to organize data:

```
video:{video_id}:scenes
├── scene_id_1 → [fingerprint1, fingerprint2, ...]
├── scene_id_2 → [fingerprint3, fingerprint4, ...]
└── ...

video:{video_id}:scene_info:{scene_id}
├── video_start_time → "10.5"
├── video_end_time → "15.2"
└── ...
```

#### Key Operations

**Storage Operations:**

- Store scene fingerprints as JSON arrays in Redis hash structures
- Maintain scene metadata (timing, IDs) in separate hash keys
- Support multiple fingerprints per scene for enhanced matching

**Similarity Matching:**

- Retrieve all stored fingerprints for a given video
- Calculate Hamming distance between query and stored fingerprints
- Apply fingerprint similarity threshold (distance ≤ threshold)
- **Check temporal overlap** between scene time boundaries (within overlap_threshold)
- Only return matches that satisfy BOTH fingerprint similarity AND temporal overlap criteria
- Return the best matching scene with metadata

#### Configuration Parameters

- **`threshold`**: Maximum fingerprint distance for considering a match (default: 10)
- **`overlap_threshold`**: Maximum time overlap between scenes in seconds (default: 0.1)

### 4. Fingerprint System

The fingerprint system uses perceptual hashing to identify similar scenes:

- **Algorithm**: Wavelet hash (whash) with configurable hash size
- **Distance Metric**: Hamming distance between hash strings
- **Robustness**: Tolerant to minor variations in lighting, compression, and transformations
- **Efficiency**: Fast comparison suitable for real-time processing

## Data Flow

1. **Input**: Kafka message with video chunk metadata
2. **Download**: Video file from S3 to temporary storage
3. **Scene Detection**: Computer vision algorithms identify scene boundaries
4. **Fingerprint Generation**: Perceptual hashes computed for scene keyframes
5. **ID Assignment**: 
   - Check for similar existing scenes
   - Assign new ID or reuse existing ID
   - Update scene boundaries if merging
6. **Storage**: Store fingerprints and metadata in Redis
7. **Output**: Publish scene information to Kafka

## Configuration

The service is configured through environment variables and settings:

- **Kafka**: Bootstrap servers, topics, consumer groups
- **S3**: Endpoint URL, access credentials
- **Redis**: Host, port, database selection
- **Processing**: Fingerprint thresholds, scene detection parameters

## Dependencies

- **Computer Vision**: OpenCV for video processing
- **Perceptual Hashing**: imagehash library for fingerprint generation
- **Storage**: Redis for scene index, S3 for video files
- **Messaging**: Kafka for event streaming
- **Monitoring**: Prometheus metrics for telemetry

## Usage

The service runs as a standalone application that continuously:

1. **Poll for video chunk messages** from Kafka
2. **Process each chunk** to detect scenes using computer vision
3. **Assign unique IDs** with deduplication support
4. **Publish results** back to Kafka
5. **Track processing metrics** for monitoring

## Key Benefits

- **Deduplication**: Prevents duplicate scene detection across video processing sessions
- **Scalability**: Redis-backed storage supports high-throughput processing
- **Consistency**: Same scenes receive same IDs regardless of processing order
- **Efficiency**: Perceptual hashing enables fast similarity matching
- **Reliability**: Handles temporal overlaps and scene merging gracefully
