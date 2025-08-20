import json
import time

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage


@pytest.fixture
def scene_detector_container(test_network):
    image = DockerImage(path=".", tag="scene-detector")
    image.build()
    container = DockerContainer(image.tag).with_network(test_network)
    return container


@pytest.fixture
def raw_chunks_topic(kafka):
    t = "video-chunks"
    client = AdminClient({"bootstrap.servers": kafka.get_bootstrap_server()})
    client.create_topics([NewTopic(t, 1, 1)])
    yield t
    client.delete_topics([t])


@pytest.fixture
def scenes_topic(kafka):
    t = "scenes"
    client = AdminClient({"bootstrap.servers": kafka.get_bootstrap_server()})
    client.create_topics([NewTopic(t, 1, 1)])
    yield t
    client.delete_topics([t])


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


def test_end_to_end(
    s3, kafka, redis, scene_detector_container, raw_chunks_topic, scenes_topic, chunked_video_bucket, fixture_path
):
    s3_client = s3.get_client()
    scene_detector_container.with_env("PYTHONUNBUFFERED", "1")
    scene_detector_container.with_env("STORAGE__ENDPOINT_URL", "http://s3:9000")
    scene_detector_container.with_env("STORAGE__ACCESS_KEY_ID", "minioadmin")
    scene_detector_container.with_env("STORAGE__SECRET_ACCESS_KEY", "minioadmin")
    scene_detector_container.with_env("KAFKA__BOOTSTRAP_SERVERS", "kafka:9092")
    scene_detector_container.with_env("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT")
    scene_detector_container.with_env("KAFKA__CHUNKS_TOPIC", raw_chunks_topic)
    scene_detector_container.with_env("KAFKA__SCENES_TOPIC", scenes_topic)
    scene_detector_container.with_env("KAFKA__GROUP_ID", "test-group-23132")
    scene_detector_container.with_env("KAFKA__AUTO_OFFSET_RESET", "earliest")
    scene_detector_container.with_env("REDIS__HOST", "redis")
    scene_detector_container.with_env("REDIS__PORT", "6379")

    s3_client.fput_object(
        bucket_name=chunked_video_bucket,
        object_name="78eb6d79-81f7-4d1f-b2d0-d8574cd7de87/chunk_000002_000002.mp4",
        file_path=fixture_path("videos/big_buck_bunny_01@480p30.mp4"),
    )

    # Wait for the container to start
    time.sleep(10)

    # Send a message to the Kafka topic
    producer = Producer(
        {
            "bootstrap.servers": kafka.get_bootstrap_server(),
            "security.protocol": "PLAINTEXT",
        }
    )

    chunk_message = {
        "chunk_count": 2,
        "chunk_idx": 2,
        "end_ts": 10,
        "fps": 25.0,
        "id": "78eb6d79-81f7-4d1f-b2d0-d8574cd7de87/chunk_000002_000002.mp4",
        "overlap_left": 1,
        "overlap_right": 0.0,
        "settings": {"audio_codec": "aac", "codec": "libx264"},
        "start_ts": 5,
        "uri": "s3://chunked-videos/78eb6d79-81f7-4d1f-b2d0-d8574cd7de87/chunk_000002_000002.mp4",
        "video_id": "78eb6d79-81f7-4d1f-b2d0-d8574cd7de87",
    }

    producer.produce(raw_chunks_topic, json.dumps(chunk_message))
    producer.flush()

    consumer = Consumer(
        {
            "bootstrap.servers": kafka.get_bootstrap_server(),
            "group.id": "test-group-23132322",
            "security.protocol": "PLAINTEXT",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([raw_chunks_topic])
    time.sleep(1)
    message = consumer.poll(timeout=1.0)
    if message is None:
        raise Exception("No message received")
    if message.error():
        raise Exception(message.error())

    message_value = json.loads(message.value().decode("utf-8"))
    assert message_value == chunk_message

    scene_detector_container.start()

    time.sleep(10)

    consumer.unsubscribe()
    consumer.close()

    consumer = Consumer(
        {
            "bootstrap.servers": kafka.get_bootstrap_server(),
            "group.id": "test-group-23132322",
            "security.protocol": "PLAINTEXT",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([scenes_topic])
    time.sleep(1)
    message = consumer.poll(timeout=1.0)
    if message is None:
        raise Exception("No message received")
    if message.error():
        raise Exception(message.error())

    message_value = json.loads(message.value().decode("utf-8"))
