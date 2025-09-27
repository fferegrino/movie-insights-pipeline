# Scene Manager Service

A microservice that processes scene intervals from the scene detector, merges overlapping scenes, and publishes complete scene sequences when videos are fully covered.

## Overview

The Scene Manager is part of a larger movie insights pipeline that consolidates scene data from video processing. It operates as a Kafka consumer that:

1. **Receives scene intervals** from the scene detector via Kafka
2. **Merges overlapping intervals** to create consolidated scene timelines
3. **Tracks video coverage** to detect when entire videos are processed
4. **Publishes complete scene sequences** back to Kafka when videos are fully covered

## Architecture

**TBA**

## Core Components

### 1. Scene Processing Pipeline

The main processing flow occurs in `__main__.py` and `processor.py`:

- **SceneProcessor**: Main orchestrator that consumes Kafka messages
- **SceneMerger**: Handles interval merging and video coverage tracking
- **Redis Storage**: Maintains scene intervals and video metadata
- **Kafka Publishing**: Outputs complete scene sequences

### 2. Scene Merger System (`scene_merger.py`)

The `SceneMerger` class is the core component responsible for managing scene intervals and detecting complete video coverage:

#### Purpose

- Stores and manages video scene intervals in Redis using sorted sets
- Merges overlapping intervals to create consolidated scene timelines
- Tracks scene coverage across entire videos
- Returns complete scene sequences when a video is fully covered

#### How It Works

The merging process follows these steps:

1. **Interval Storage**: Store scene intervals in Redis sorted sets, sorted by start time
2. **Overlap Detection**: Check if new intervals overlap with existing ones
3. **Interval Merging**: Combine overlapping intervals by extending end times
4. **Coverage Tracking**: Monitor if video is fully covered from 0 to duration
5. **Complete Sequence**: When fully covered, return all scenes sorted chronologically

#### Merging Algorithm

The scene merger uses a **sorted interval merging algorithm** to consolidate overlapping scene intervals:

**Core Algorithm:**

- Input: List of intervals sorted by start time
- Process: Iterate through intervals, merging any that overlap
- Overlap Detection: Two intervals overlap if `end1 >= start2`
- Merging: When intervals overlap, extend the end time to `max(end1, end2)`
- Output: List of non-overlapping intervals covering the same time range

**Example:**

```
Input:  (0.0, 2.8), (2.8, 8.0), (7.5, 12.0), (15.0, 20.0)
Output: (0.0, 12.0), (15.0, 20.0)
```

**Properties:**

- **Time Complexity**: O(n) for merging (intervals already sorted by Redis)
- **Space Complexity**: O(n) for storing intervals
- **Preserves Order**: Maintains chronological sequence
- **Handles Multiple Overlaps**: Can merge chains of overlapping intervals
- **Produces Clean Timeline**: Results in non-overlapping intervals

#### Key Features

- **Redis Sorted Sets**: Efficient storage and retrieval of intervals sorted by start time
- **Overlap Merging**: Automatically combines overlapping scene intervals
- **Coverage Detection**: Identifies when entire videos are processed
- **Chronological Ordering**: Returns scenes sorted by start time
- **Efficient Storage**: Uses Redis pipelines for batch operations

### 3. Redis Storage System

The Redis storage uses sorted sets to organize scene data:

#### Data Structure

Redis uses sorted sets and hash structures to organize data:

```
scene-intervals:{video_id}:intervals
├── "0.000000:2.877398" → 0.0
├── "2.877398:8.003336" → 2.877398
└── ...

scene-intervals:{video_id}:intervals:{scene_id}
├── "0.000000:2.877398" → 0.0
└── ...

video-metadata:{video_id}
├── duration → "42.13"
├── video_id → "2946d2a0-0695-41b5-8da6-d68e04c4c289"
├── original_name → "pizza-conversation.mp4"
├── fps → "23.976023976023978"
└── uri → "s3://raw-videos/{video_id}/aa.mp4"
```

#### Key Operations

**Storage Operations:**

- Store scene intervals as encoded strings in Redis sorted sets
- Maintain video metadata in Redis hash structures
- Use Redis pipelines for efficient batch operations

**Interval Management:**

- Encode intervals as "start:end" strings with 6 decimal precision
- Use start time as the score for chronological sorting
- Merge overlapping intervals by extending end times
- Delete and recreate sorted sets after merging for consistency

## Data Flow

1. **Input**: Kafka message with scene interval data
2. **Parsing**: Extract scene_id, video_id, start_time, end_time
3. **Interval Creation**: Create Interval object from message data
4. **Scene Storage**: Store interval in Redis for specific scene
5. **Video Merging**: Merge with existing video-level intervals
6. **Coverage Check**: Verify if video is fully covered (0 to duration)
7. **Complete Sequence**: If fully covered, retrieve all scenes chronologically
8. **Output**: Publish complete scene sequence to Kafka
