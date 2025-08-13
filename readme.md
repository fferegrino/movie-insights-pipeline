# Movie insights pipeline

## What is included

### Docker Compose Setup (`docker-compose.yml`)

- **MinIO Server**: A local S3-compatible storage server that runs on your machine

  - **Bucket Initialization**: Automatically creates storage bucket when you start the services

  - **Web Interface**: A browser-based console to manage files and buckets

## Quick Start

```bash
docker-compose up -d
```

### Stopping

```bash
docker-compose down
```

### To completely remove everything (including your data):

```bash
docker-compose down -v
```
