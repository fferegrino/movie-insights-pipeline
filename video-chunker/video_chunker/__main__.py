import tempfile
from pathlib import Path
from uuid import uuid4

import aiofiles
from fastapi import FastAPI, File, HTTPException, UploadFile

from video_chunker.chunking import chunk_video
from video_chunker.s3 import S3Client
from video_chunker.settings import ChunkerSettings, StorageSettings

app = FastAPI(title="Video Chunker", description="A simple app to chunk videos into smaller pieces")


CHUNK_DURATION = 5
OVERLAP = 1


def get_s3_client(storage_settings: StorageSettings) -> S3Client:
    """Get S3 client instance."""
    _storage_settings = {
        "bucket_name": storage_settings.bucket,
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
    chunks = []

    video_id = str(uuid4())
    with tempfile.NamedTemporaryFile(suffix=uploaded_path.suffix) as temp_file:
        async with aiofiles.open(temp_file.name, "wb") as f:
            content = await video.read()
            await f.write(content)

        # Get S3 client if needed
        s3_client = get_s3_client(chunker_settings.storage)

        for metadata in chunk_video(str(temp_file.name), CHUNK_DURATION, OVERLAP):
            try:
                # Upload chunk to S3
                uploaded_metadata = s3_client.upload_video_chunk(
                    chunk_path=metadata["local_uri"],
                    chunk_metadata=metadata,
                    prefix=video_id,
                )
                chunks.append(uploaded_metadata)
            except Exception as e:
                # Log error but continue with other chunks
                print(f"Failed to upload chunk to S3: {e}")
                chunks.append(metadata)

    return {"chunks": chunks, "total_chunks": len(chunks)}


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "message": "Video chunker is running"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
