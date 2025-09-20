import json
import time
import uuid

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
    bucket_name = "chunked-data"
    client = s3.get_client()
    client.make_bucket(bucket_name)
    yield bucket_name
    objects = client.list_objects(bucket_name=bucket_name, recursive=True)

    for obj in objects:
        if not obj.is_dir:
            client.remove_object(bucket_name=bucket_name, object_name=obj.object_name)
    client.remove_bucket(bucket_name)


def test_end_to_end(
    s3,
    kafka,
    redis,
    scene_detector_container,
    raw_chunks_topic,
    scenes_topic,
    chunked_video_bucket,
    path_for_fixture,
    jsonl_fixture,
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

    all_videos = path_for_fixture("videos").glob("*.mp4")
    for video_path in all_videos:
        s3_client.fput_object(
            bucket_name=chunked_video_bucket,
            object_name=f"78eb6d79-81f7-4d1f-b2d0-d8574cd7de87/{video_path.stem}.mp4",
            file_path=video_path,
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

    video_chunks = jsonl_fixture("video_chunks.jsonl")
    for chunk_message in video_chunks:
        producer.produce(raw_chunks_topic, json.dumps(chunk_message))
    producer.flush()

    time.sleep(1)

    scene_detector_container.start()

    time.sleep(10)

    consumer = Consumer(
        {
            "bootstrap.servers": kafka.get_bootstrap_server(),
            "group.id": f"test-group-{uuid.uuid4()}",
            "security.protocol": "PLAINTEXT",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([scenes_topic])
    time.sleep(1)

    start_time = time.time()
    output_messages = []
    while time.time() - start_time < 10:
        message = consumer.poll(timeout=1.0)
        if message is None:
            continue
        if message.error():
            continue
        output_messages.append(json.loads(message.value().decode("utf-8")))

    with open("output_messages.jsonl", "w") as f:
        for message in output_messages:
            f.write(json.dumps(message) + "\n")

    assert len(output_messages) > 18
