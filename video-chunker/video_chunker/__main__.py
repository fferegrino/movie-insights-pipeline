import tempfile
from pathlib import Path
from uuid import uuid4

import aiofiles
from fastapi import FastAPI, File, HTTPException, UploadFile
from moviepy import VideoFileClip

from video_chunker.chunking import chunk_video
from video_chunker.s3 import S3Client
from video_chunker.settings import ChunkerSettings, StorageSettings

app = FastAPI(title="Video Chunker", description="A simple app to chunk videos into smaller pieces")


CHUNK_DURATION = 5
OVERLAP = 1


def get_s3_client(storage_settings: StorageSettings) -> S3Client:
    """Get S3 client instance."""
    _storage_settings = {
        "aws_access_key_id": storage_settings.access_key_id,
        "aws_secret_access_key": storage_settings.secret_access_key,
    }
    if storage_settings.endpoint_url:
        _storage_settings["endpoint_url"] = storage_settings.endpoint_url

    return S3Client(**_storage_settings)


@app.post("/chunk-video")
async def post_chunk_video(
    video: UploadFile = File(...),  # noqa: B008
):
    """
    Chunk a video file and optionally upload chunks to S3.

    Args:
        video: The video file to chunk

    """
    if not video.content_type.startswith("video/"):
        raise HTTPException(status_code=400, detail="File must be a video")

    chunker_settings = ChunkerSettings()

    uploaded_path = Path(video.filename)

    video_id = str(uuid4())

    upload_metadata = {"original_name": uploaded_path.name, "video_id": video_id, "chunks": []}
    with tempfile.NamedTemporaryFile(suffix=uploaded_path.suffix) as temp_file:
        async with aiofiles.open(temp_file.name, "wb") as f:
            content = await video.read()
            await f.write(content)

        # Get S3 client if needed
        s3_client = get_s3_client(chunker_settings.storage)

        raw_video_metadata = s3_client.upload_video(
            bucket_name=chunker_settings.storage.raw_video_bucket,
            path=temp_file.name,
            metadata={"original_name": uploaded_path.name, "video_id": video_id},
            video_id=video_id,
        )

        upload_metadata["uri"] = raw_video_metadata["uri"]

        with VideoFileClip(temp_file.name) as full_video:
            upload_metadata["duration"] = full_video.duration
            upload_metadata["fps"] = full_video.fps

            chunk_count = 0
            for chunk_path, metadata in chunk_video(full_video, CHUNK_DURATION, OVERLAP):
                chunk_count += 1
                try:
                    # Upload chunk to S3
                    chunk_metadata = s3_client.upload_video(
                        bucket_name=chunker_settings.storage.chunked_video_bucket,
                        path=chunk_path,
                        metadata=metadata,
                        video_id=video_id,
                    )
                    upload_metadata["chunks"].append(chunk_metadata)
                except Exception as e:
                    # Log error but continue with other chunks
                    print(f"Failed to upload chunk to S3: {e}")

            upload_metadata["chunk_count"] = chunk_count

    return upload_metadata


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "message": "Video chunker is running"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
