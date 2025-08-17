import json
import time
from unittest.mock import ANY
from urllib.parse import urlparse

import pytest
import requests
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage


@pytest.fixture
def raw_chunks_topic(kafka):
    t = "raw-chunks"
    client = AdminClient({"bootstrap.servers": kafka.get_bootstrap_server()})
    client.create_topics([NewTopic(t, 1, 1)])
    yield t
    client.delete_topics([t])


@pytest.fixture
def raw_video_bucket(s3):
    bucket_name = "raw-videos"
    client = s3.get_client()
    client.make_bucket(bucket_name)
    yield bucket_name
    objects = client.list_objects(bucket_name=bucket_name, recursive=True)

    for obj in objects:
        if not obj.is_dir:
            client.remove_object(bucket_name=bucket_name, object_name=obj.object_name)
    client.remove_bucket(bucket_name)


@pytest.fixture
def chunked_video_bucket(s3):
    bucket_name = "chunked-videos"
    client = s3.get_client()
    client.make_bucket(bucket_name)
    yield bucket_name
    objects = client.list_objects(bucket_name=bucket_name, recursive=True)

    for obj in objects:
        if not obj.is_dir:
            client.remove_object(bucket_name=bucket_name, object_name=obj.object_name)
    client.remove_bucket(bucket_name)


@pytest.fixture
def video_chunker_container(test_network):
    image = DockerImage(path=".", tag="video-chunker")
    image.build()
    container = DockerContainer(image.tag).with_exposed_ports(8000).with_network(test_network)
    return container


def read_messages(consumer, topic, count, timeout=10):
    consumer.subscribe([topic])
    messages = []
    start_time = time.time()
    while len(messages) < count and time.time() - start_time < timeout:
        message = consumer.poll(timeout=1.0)
        if message is None:
            continue
        if message.error():
            raise Exception(message.error())

        messages.append(json.loads(message.value().decode("utf-8")))
    return messages


def test_end_to_end(
    s3,
    chunked_video_bucket,
    video_chunker_container,
    fixture_path,
    raw_video_bucket,
    raw_chunks_topic,
    get_json_fixture,
    kafka,
):
    storage_client = s3.get_client()
    video_chunker_container.with_env("STORAGE__ENDPOINT_URL", "http://s3:9000")
    video_chunker_container.with_env("STORAGE__ACCESS_KEY_ID", "minioadmin")
    video_chunker_container.with_env("STORAGE__SECRET_ACCESS_KEY", "minioadmin")
    video_chunker_container.with_env("STORAGE__RAW_VIDEO_BUCKET", raw_video_bucket)
    video_chunker_container.with_env("STORAGE__CHUNKED_VIDEO_BUCKET", chunked_video_bucket)
    video_chunker_container.with_env("KAFKA__CHUNKS_TOPIC", raw_chunks_topic)
    video_chunker_container.with_env("KAFKA__BOOTSTRAP_SERVERS", "kafka:9092")
    video_chunker_container.with_env("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT")
    video_chunker_container.start()

    time.sleep(1)
    chunker_port = video_chunker_container.get_exposed_port(8000)
    chunker_host = video_chunker_container.get_container_host_ip()

    video_path = fixture_path("smallest.mp4")

    with open(video_path, "rb") as f:
        files = {"video": ("smallest.mp4", f, "video/mp4")}
        response = requests.post(f"http://{chunker_host}:{chunker_port}/chunk-video", files=files)

        try:
            result = response.json()
        except Exception as e:
            [stdout, stderr] = video_chunker_container.get_logs()
            assert False, f"Failed to chunk video: {e}\n{stdout.decode('utf-8')}\n{stderr.decode('utf-8')}"

    expected_response = get_json_fixture("smallest_upload_response.json")

    [stdout, stderr] = video_chunker_container.get_logs()

    video_id = result["video_id"]
    expected_response["video_id"] = video_id

    raw_video_uri = result.pop("uri")
    s3_key = urlparse(raw_video_uri).path.lstrip("/")
    s3_object = storage_client.stat_object(bucket_name=raw_video_bucket, object_name=s3_key)
    result["uri"] = ANY
    assert s3_object

    for chunk in result["chunks"]:
        chunk_uri = chunk.pop("uri")
        s3_key = urlparse(chunk_uri).path.lstrip("/")
        s3_object = storage_client.stat_object(bucket_name=chunked_video_bucket, object_name=s3_key)
        chunk["uri"] = ANY
        chunk["id"] = ANY
        assert chunk["video_id"] == video_id
        chunk["video_id"] = ANY
        assert s3_object

    assert result == expected_response

    consumer = Consumer(
        {
            "bootstrap.servers": kafka.get_bootstrap_server(),
            "group.id": "test-group-23132",
            "security.protocol": "PLAINTEXT",
            "auto.offset.reset": "earliest",
        }
    )

    messages = read_messages(consumer, raw_chunks_topic, 2)

    assert len(messages) == 2, f"Unable to read all messages\n{stdout.decode('utf-8')}\n{stderr.decode('utf-8')}"

    for message in messages:
        s3_key = urlparse(message["uri"]).path.lstrip("/")
        s3_object = storage_client.stat_object(bucket_name=chunked_video_bucket, object_name=s3_key)
        assert s3_object
