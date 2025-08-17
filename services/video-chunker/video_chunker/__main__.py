import tempfile
from pathlib import Path

import aiofiles
from fastapi import FastAPI, File, HTTPException, UploadFile

from video_chunker.main import process_video

app = FastAPI(title="Video Chunker", description="A simple app to chunk videos into smaller pieces")


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

    original_filename = Path(video.filename)

    with tempfile.NamedTemporaryFile(suffix=original_filename.suffix) as temp_file:
        async with aiofiles.open(temp_file.name, "wb") as f:
            content = await video.read()
            await f.write(content)

        video_path = Path(temp_file.name)

        return process_video(video_path, original_filename)


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "message": "Video chunker is running"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
