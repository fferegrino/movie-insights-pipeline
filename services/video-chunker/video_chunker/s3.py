import logging
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def flatten_dict(d: dict[str, Any], prefix: str = "", separator: str = ".") -> dict[str, Any]:
    """
    Flatten a nested dictionary into a single-level dictionary.

    This function recursively traverses a nested dictionary and creates a flattened
    version where nested keys are joined with a separator. This is useful for
    preparing metadata for S3 uploads, as S3 metadata must be flat.

    Args:
        d: The nested dictionary to flatten
        prefix: The prefix to prepend to keys (used internally for recursion)
        separator: The string to use when joining nested keys

    Returns:
        A flattened dictionary with single-level keys

    Example:
        >>> flatten_dict({"a": {"b": 1, "c": 2}, "d": 3})
        {'a.b': '1', 'a.c': '2', 'd': '3'}

    """
    new_dict = {}
    for key, value in d.items():
        if isinstance(value, dict):
            new_dict.update(flatten_dict(value, prefix=key))
        else:
            new_dict[f"{prefix}{separator}{key}" if prefix else key] = str(value)
    return new_dict


class S3Client:
    """
    A client for interacting with Amazon S3 using boto3.

    This class provides methods for uploading files to S3 with proper error handling
    and configuration management. It supports both synchronous and asynchronous operations.

    The client can be configured to work with:
    - Standard AWS S3
    - S3-compatible services (like MinIO) via custom endpoint URLs
    - Different AWS regions
    - Custom credentials

    Attributes:
        region_name: The AWS region name for S3 operations
        session: The boto3 session used for authentication
        client: The boto3 S3 client for performing operations

    """

    def __init__(  # noqa: PLR0913
        self,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        max_retries: int = 3,
        timeout: int = 60,
    ) -> None:
        """
        Initialize the S3 client with configuration options.

        Args:
            endpoint_url: Custom S3 endpoint URL (useful for S3-compatible services)
            region_name: AWS region name (e.g., 'us-east-1', 'eu-west-1')
            aws_access_key_id: AWS access key ID for authentication
            aws_secret_access_key: AWS secret access key for authentication
            max_retries: Maximum number of retry attempts for failed operations
            timeout: Timeout in seconds for connection and read operations

        Note:
            If credentials are not provided, the client will use the default
            AWS credential chain (environment variables, IAM roles, etc.)

        """
        self.region_name = region_name

        # Configure boto3 client
        config = Config(
            retries={"max_attempts": max_retries},
            connect_timeout=timeout,
            read_timeout=timeout,
        )

        # Initialize boto3 session and client
        session_kwargs = {}
        if aws_access_key_id:
            session_kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            session_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if self.region_name:
            session_kwargs["region_name"] = self.region_name

        self.session = boto3.Session(**session_kwargs)

        client_kwargs = {"config": config}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url

        self.client = self.session.client("s3", **client_kwargs)

    def upload_file(
        self,
        bucket_name: str,
        file_path: str,
        s3_key: str | None = None,
        metadata: dict[str, str] | None = None,
        content_type: str | None = None,
    ) -> str:
        """
        Upload a file to S3 with optional metadata and content type.

        This method uploads a local file to an S3 bucket. It includes comprehensive
        error handling and logging for monitoring upload progress and debugging
        issues.

        Args:
            bucket_name: The name of the S3 bucket to upload to
            file_path: Local path to the file to upload
            s3_key: The S3 key (path) for the uploaded file. If None, uses the filename
            metadata: Optional metadata to attach to the S3 object
            content_type: Optional MIME type for the uploaded file

        Returns:
            The S3 URI of the uploaded file (format: s3://bucket/key)

        Raises:
            FileNotFoundError: If the local file doesn't exist
            RuntimeError: If the upload fails due to S3 errors

        Example:
            >>> client = S3Client()
            >>> uri = client.upload_file(
            ...     bucket_name="my-bucket",
            ...     file_path="/path/to/video.mp4",
            ...     s3_key="videos/processed/video.mp4",
            ...     metadata={"processed": "true"},
            ...     content_type="video/mp4"
            ... )
            >>> print(uri)
            s3://my-bucket/videos/processed/video.mp4

        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Generate S3 key if not provided
        if s3_key is None:
            s3_key = file_path.name

        # Prepare upload parameters
        upload_kwargs = {
            "Filename": str(file_path),
            "Bucket": bucket_name,
            "Key": s3_key,
        }

        if metadata:
            upload_kwargs["ExtraArgs"] = {"Metadata": metadata}

        if content_type:
            if "ExtraArgs" not in upload_kwargs:
                upload_kwargs["ExtraArgs"] = {}
            upload_kwargs["ExtraArgs"]["ContentType"] = content_type

        try:
            logger.info(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}")
            self.client.upload_file(**upload_kwargs)

            # Generate S3 URL
            s3_url = f"s3://{bucket_name}/{s3_key}"
            logger.info(f"Successfully uploaded file to {s3_url}")
            return s3_url

        except ClientError as e:
            error_msg = f"Failed to upload {file_path} to S3: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def upload_video(
        self,
        bucket_name: str,
        path: str,
        metadata: dict[str, Any],
        video_id: str,
    ) -> dict[str, Any]:
        """
        Upload a video chunk to S3 with video-specific metadata.

        This method is specifically designed for uploading video chunks that are
        part of a larger video processing pipeline. It automatically organizes
        chunks by video ID and includes comprehensive metadata about the chunk.

        Args:
            bucket_name: The name of the S3 bucket to upload to
            path: Local path to the video chunk file
            metadata: Dictionary containing chunk metadata (timestamps, overlap, etc.)
            video_id: Unique identifier for the parent video

        Returns:
            Dictionary containing the original metadata plus S3-specific information:
            - uri: The S3 URI of the uploaded chunk
            - id: The S3 key of the uploaded chunk
            - video_id: The video ID
            - All original metadata fields

        Example:
            >>> client = S3Client()
            >>> result = client.upload_video(
            ...     bucket_name="video-chunks",
            ...     path="/tmp/chunk_000001_000010.mp4",
            ...     metadata={
            ...         "start_ts": 0.0,
            ...         "end_ts": 10.0,
            ...         "chunk_idx": 1,
            ...         "chunk_count": 10
            ...     },
            ...     video_id="video_123"
            ... )
            >>> print(result["uri"])
            s3://video-chunks/video_123/chunk_000001_000010.mp4

        """
        # Generate S3 key based on chunk metadata
        filename = Path(path).name
        s3_key = f"{video_id}/{filename}"

        # Prepare metadata for S3
        _metadata = {
            "video_id": video_id,
            **metadata,
        }

        # Upload the chunk
        s3_url = self.upload_file(
            bucket_name=bucket_name,
            file_path=path,
            s3_key=s3_key,
            metadata=flatten_dict(_metadata),
            content_type="video/mp4",
        )

        # Return updated metadata with S3 information
        return {
            **_metadata,
            "uri": s3_url,
            "id": s3_key,
        }
