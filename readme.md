# Movie insights pipeline

## What is included

### Docker Compose Setup (`docker-compose.yml`)

- **MinIO Server**: A local S3-compatible storage server that runs on your machine

  - **Bucket Initialization**: Automatically creates storage bucket when you start the services

  - **Web Interface**: A browser-based console to manage files and buckets

- **Kafka**: A message broker that allows you to send and receive messages between different services

  - **Topic Initialization**: Automatically creates Kafka topics when you start the services

  - **Kafka UI**: A web interface to manage Kafka topics and messages

- **Video Chunker**: A service that chunks videos into smaller chunks

## Quick Start

```bash
docker-compose up -d
```

### Upload a video

```bash
curl -i -X POST http://localhost:8000/chunk-video \
    -F "video=@$PWD/video-chunker/tests/fixtures/smallest.mp4;type=video/mp4"
```

### Stopping

```bash
docker-compose down
```

### To completely remove everything (including your data):

```bash
docker-compose down -v
```
