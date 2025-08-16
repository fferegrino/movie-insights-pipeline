from pathlib import Path
from typing import Any
from uuid import uuid4

from moviepy import VideoFileClip

from video_chunker.chunking import chunk_video
from video_chunker.s3 import S3Client
from video_chunker.settings import ChunkerSettings, StorageSettings

# Configuration constants
CHUNK_DURATION = 5
OVERLAP = 1


def get_s3_client(storage_settings: StorageSettings) -> S3Client:
    """
    Create and configure an S3 client instance.

    Args:
        storage_settings: Storage configuration containing AWS credentials and endpoint

    Returns:
        S3Client: Configured S3 client instance

    Note:
        Supports both AWS S3 and S3-compatible storage (like MinIO) via endpoint_url

    """
    _storage_settings = {
        "aws_access_key_id": storage_settings.access_key_id,
        "aws_secret_access_key": storage_settings.secret_access_key,
    }
    if storage_settings.endpoint_url:
        _storage_settings["endpoint_url"] = storage_settings.endpoint_url

    return S3Client(**_storage_settings)


def upload_raw_video(
    s3_client: S3Client, video_path: Path, metadata: dict[str, Any], video_id: str, bucket_name: str
) -> dict[str, Any]:
    """
    Upload the raw video file to S3 storage.

    Args:
        s3_client: Configured S3 client instance
        video_path: Path to the video file to upload
        metadata: Video metadata to associate with the upload
        video_id: Unique identifier for the video
        bucket_name: Name of the S3 bucket for raw videos

    Returns:
        Dict containing upload metadata including the S3 URI

    Raises:
        Exception: If upload fails

    """
    upload_metadata = s3_client.upload_video(
        bucket_name=bucket_name,
        path=video_path,
        metadata=metadata,
        video_id=video_id,
    )
    return upload_metadata


def upload_video_chunks(
    s3_client: S3Client, video_clip: VideoFileClip, video_id: str, bucket_name: str
) -> list[dict[str, Any]]:
    """
    Chunk the video and upload each chunk to S3 storage.

    Args:
        s3_client: Configured S3 client instance
        video_clip: The video clip to chunk and upload
        video_id: Unique identifier for the video
        bucket_name: Name of the S3 bucket for video chunks

    Returns:
        List of dictionaries containing metadata for each uploaded chunk

    Note:
        If a chunk upload fails, the error is logged but processing continues
        with the remaining chunks.

    """
    chunks = []

    for chunk_path, metadata in chunk_video(video_clip, CHUNK_DURATION, OVERLAP):
        try:
            chunk_metadata = s3_client.upload_video(
                bucket_name=bucket_name,
                path=chunk_path,
                metadata=metadata,
                video_id=video_id,
            )
            chunks.append(chunk_metadata)
        except Exception as e:
            # Log error but continue with other chunks
            print(f"Failed to upload chunk to S3: {e}")

    return chunks


def process_video(video_path: Path, original_filename: Path) -> dict[str, Any]:
    """
    Process a video file by uploading the raw video and creating/uploading chunks.

    This function orchestrates the complete video processing pipeline:
    1. Creates S3 client and video ID
    2. Extracts video metadata
    3. Uploads the raw video to S3
    4. Chunks the video and uploads each chunk to S3
    5. Returns comprehensive metadata including all chunk information

    Args:
        video_path: Path to the video file to process
        original_filename: Original filename of the video

    Returns:
        Dict containing complete video metadata including:
        - original_name: Original filename
        - video_id: Unique identifier
        - duration: Video duration in seconds
        - fps: Frames per second
        - uri: S3 URI of the raw video
        - chunks: List of chunk metadata

    Raises:
        Exception: If video processing fails (e.g., invalid video file)

    Example:
        >>> metadata = process_video(Path("video.mp4"), Path("original.mp4"))
        >>> print(f"Processed video {metadata['video_id']} with {len(metadata['chunks'])} chunks")

    """
    # Initialize settings and S3 client
    chunker_settings = ChunkerSettings()
    s3_client = get_s3_client(chunker_settings.storage)

    # Generate unique video ID
    video_id = str(uuid4())

    with VideoFileClip(video_path) as full_video:
        # Extract and prepare video metadata
        raw_video_metadata = {
            "original_name": original_filename.name,
            "video_id": video_id,
            "duration": full_video.duration,
            "fps": full_video.fps,
        }

        # Upload raw video to S3
        upload_metadata = upload_raw_video(
            s3_client=s3_client,
            video_path=video_path,
            metadata=raw_video_metadata,
            video_id=video_id,
            bucket_name=chunker_settings.storage.raw_video_bucket,
        )

        # Add S3 URI to metadata
        raw_video_metadata["uri"] = upload_metadata["uri"]

        # Upload video chunks
        chunks = upload_video_chunks(
            s3_client=s3_client,
            video_clip=full_video,
            video_id=video_id,
            bucket_name=chunker_settings.storage.chunked_video_bucket,
        )

        # Add chunks to final metadata
        raw_video_metadata["chunks"] = chunks

    return raw_video_metadata
