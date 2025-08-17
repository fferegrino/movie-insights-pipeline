import pytest

from video_chunker.s3 import S3Client


@pytest.fixture
def raw_video_bucket(s3):
    bucket_name = "raw-videos"
    client = s3.get_client()
    client.make_bucket(bucket_name)
    yield bucket_name
    objects = client.list_objects(bucket_name=bucket_name)
    for obj in objects:
        client.remove_object(bucket_name=bucket_name, object_name=obj.object_name)
    client.remove_bucket(bucket_name)


def test_s3_client(s3, raw_video_bucket, fixture_path):
    s3_host = s3.get_container_host_ip()
    s3_port = s3.get_exposed_port(9000)

    minio_client = s3.get_client()
    s3_client = S3Client(
        endpoint_url=f"http://{s3_host}:{s3_port}",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )

    s3_client.upload_file(
        bucket_name=raw_video_bucket,
        file_path=fixture_path("smallest.mp4"),
        s3_key="smallest.mp4",
    )

    uploaded_object = minio_client.stat_object(bucket_name=raw_video_bucket, object_name="smallest.mp4")
    assert uploaded_object.object_name == "smallest.mp4"
