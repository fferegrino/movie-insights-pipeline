import logging
import os
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError

logger = logging.getLogger(__name__)


class S3Client:
    """
    A client for interacting with Amazon S3 using boto3.

    This class provides methods for uploading files to S3 with proper error handling
    and configuration management. It supports both synchronous and asynchronous operations.
    """

    def __init__(  # noqa: PLR0913
        self,
        bucket_name: str,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        max_retries: int = 3,
        timeout: int = 60,
    ) -> None:
        """
        Initialize the S3 client.

        Args:
            bucket_name: The name of the S3 bucket to use
            endpoint_url: Custom S3 endpoint URL (for local development or other S3-compatible services)
            region_name: AWS region name (defaults to environment variable or us-east-1)
            aws_access_key_id: AWS access key ID (defaults to environment variable)
            aws_secret_access_key: AWS secret access key (defaults to environment variable)
            max_retries: Maximum number of retries for failed requests
            timeout: Request timeout in seconds

        """
        self.bucket_name = bucket_name
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

        # Verify bucket exists and is accessible
        self._verify_bucket_access()

    def _verify_bucket_access(self) -> None:
        """Verify that the bucket exists and is accessible."""
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully connected to S3 bucket: {self.bucket_name}")
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                raise ValueError(f"Bucket '{self.bucket_name}' does not exist") from e
            elif error_code == "403":
                raise PermissionError(f"Access denied to bucket '{self.bucket_name}'") from e
            else:
                raise RuntimeError(f"Failed to access bucket '{self.bucket_name}': {e}") from e
        except (NoCredentialsError, PartialCredentialsError) as e:
            raise RuntimeError(f"AWS credentials not found or invalid: {e}") from e

    def upload_file(
        self,
        file_path: str,
        s3_key: str | None = None,
        metadata: dict[str, str] | None = None,
        content_type: str | None = None,
    ) -> str:
        """
        Upload a file to S3.

        Args:
            file_path: Local path to the file to upload
            s3_key: S3 key (path) for the file. If None, uses the filename
            metadata: Optional metadata to attach to the S3 object
            content_type: Optional content type for the file

        Returns:
            The S3 URL of the uploaded file

        Raises:
            FileNotFoundError: If the local file doesn't exist
            RuntimeError: If the upload fails

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
            "Bucket": self.bucket_name,
            "Key": s3_key,
        }

        if metadata:
            upload_kwargs["ExtraArgs"] = {"Metadata": metadata}

        if content_type:
            if "ExtraArgs" not in upload_kwargs:
                upload_kwargs["ExtraArgs"] = {}
            upload_kwargs["ExtraArgs"]["ContentType"] = content_type

        try:
            logger.info(f"Uploading {file_path} to s3://{self.bucket_name}/{s3_key}")
            self.client.upload_file(**upload_kwargs)

            # Generate S3 URL
            s3_url = f"s3://{self.bucket_name}/{s3_key}"
            logger.info(f"Successfully uploaded file to {s3_url}")
            return s3_url

        except ClientError as e:
            error_msg = f"Failed to upload {file_path} to S3: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def upload_video_chunk(
        self,
        chunk_path: str,
        chunk_metadata: dict[str, Any],
        prefix: str,
    ) -> dict[str, Any]:
        """
        Upload a video chunk to S3 with metadata.

        Args:
            chunk_path: Local path to the video chunk file
            chunk_metadata: Metadata about the chunk (from chunk_video function)
            prefix: S3 key prefix for organizing chunks

        Returns:
            Dictionary containing the S3 URL and metadata

        """
        # Generate S3 key based on chunk metadata
        chunk_filename = Path(chunk_path).name
        s3_key = f"{prefix}/{chunk_filename}"

        # Prepare metadata for S3
        s3_metadata = {
            "start_time": str(chunk_metadata["start_time"]),
            "end_time": str(chunk_metadata["end_time"]),
            "fps": str(chunk_metadata["fps"]),
            "chunk_index": chunk_filename.split("_")[1],  # Extract chunk number
        }

        # Upload the chunk
        s3_url = self.upload_file(
            file_path=chunk_path,
            s3_key=s3_key,
            metadata=s3_metadata,
            content_type="video/mp4",
        )

        # Return updated metadata with S3 information
        return {
            **chunk_metadata,
            "s3_url": s3_url,
            "s3_key": s3_key,
        }

    def get_object_url(self, s3_key: str, expires_in: int = 3600) -> str:
        """
        Generate a presigned URL for an S3 object.

        Args:
            s3_key: The S3 key of the object
            expires_in: URL expiration time in seconds (default: 1 hour)

        Returns:
            Presigned URL for the object

        """
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket_name, "Key": s3_key},
                ExpiresIn=expires_in,
            )
            return url

        except ClientError as e:
            error_msg = f"Failed to generate presigned URL for s3://{self.bucket_name}/{s3_key}: {e}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e


# Convenience function for creating an S3 client with environment variables
def create_s3_client(
    bucket_name: str | None = None,
    region_name: str | None = None,
    endpoint_url: str | None = None,
) -> S3Client:
    """
    Create an S3 client using environment variables for configuration.

    Args:
        bucket_name: S3 bucket name (defaults to AWS_S3_BUCKET environment variable)
        region_name: AWS region (defaults to AWS_DEFAULT_REGION environment variable)
        endpoint_url: Custom S3 endpoint URL (for local development)

    Returns:
        Configured S3Client instance

    Raises:
        ValueError: If bucket_name is not provided and AWS_S3_BUCKET is not set

    """
    bucket = bucket_name or os.getenv("AWS_S3_BUCKET")
    if not bucket:
        raise ValueError("Bucket name must be provided either as parameter or via AWS_S3_BUCKET environment variable")

    region = region_name or os.getenv("AWS_DEFAULT_REGION")
    endpoint = endpoint_url or os.getenv("AWS_S3_ENDPOINT_URL")

    return S3Client(
        bucket_name=bucket,
        region_name=region,
        endpoint_url=endpoint,
    )
