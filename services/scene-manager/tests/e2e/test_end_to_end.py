import json
import time

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage


@pytest.fixture
def scene_manager_container(test_network):
    image = DockerImage(path=".", tag="scene-manager")
    image.build()
    container = DockerContainer(image.tag).with_network(test_network)
    return container


@pytest.fixture
def scenes_topic(kafka):
    t = "scenes"
    client = AdminClient({"bootstrap.servers": kafka.get_bootstrap_server()})
    client.create_topics([NewTopic(t, 1, 1)])
    yield t
    client.delete_topics([t])


@pytest.fixture
def merged_scenes_topic(kafka):
    t = "merged-scenes"
    client = AdminClient({"bootstrap.servers": kafka.get_bootstrap_server()})
    client.create_topics([NewTopic(t, 1, 1)])
    yield t
    client.delete_topics([t])


@pytest.fixture
def consumer(kafka):
    return Consumer(
        {
            "bootstrap.servers": kafka.get_bootstrap_server(),
            "group.id": "test-processor",
            "auto.offset.reset": "earliest",
        }
    )


@pytest.fixture
def producer(kafka):
    return Producer({"bootstrap.servers": kafka.get_bootstrap_server()})


@pytest.fixture
def scene_input_messages(read_jsonl_fixture):
    return read_jsonl_fixture("inputs/scene_messages.jsonl")


@pytest.fixture
def pattern_handlers():
    return {
        "uuid4": r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    }


@pytest.fixture
def scene_output_messages(read_jsonl_fixture):
    return list(read_jsonl_fixture("outputs/merged_scenes.jsonl"))


def test_end_to_end(
    kafka,
    redis,
    scene_manager_container,
    scenes_topic,
    merged_scenes_topic,
    scene_input_messages,
    scene_output_messages,
    producer,
    consumer,
    dict_match,
):
    video_id = "2946d2a0-0695-41b5-8da6-d68e04c4c289"

    redis_client = redis.get_client()

    video_metadata = {
        "duration": "42.13",
        "video_id": video_id,
        "original_name": "pizza-conversation.mp4",
        "fps": "23.976023976023978",
        "uri": f"s3://raw-videos/{video_id}/aa.mp4",
    }

    redis_client.hset(f"video-metadata:{video_id}", mapping=video_metadata)
    scene_manager_container.with_env("PYTHONUNBUFFERED", "1")
    scene_manager_container.with_env("KAFKA__BOOTSTRAP_SERVERS", "kafka:9092")
    scene_manager_container.with_env("KAFKA__SECURITY_PROTOCOL", "PLAINTEXT")
    scene_manager_container.with_env("KAFKA__SCENES_TOPIC", scenes_topic)
    scene_manager_container.with_env("KAFKA__MERGED_SCENES_TOPIC", merged_scenes_topic)
    scene_manager_container.with_env("KAFKA__GROUP_ID", "test-group-23132")
    scene_manager_container.with_env("KAFKA__AUTO_OFFSET_RESET", "earliest")
    scene_manager_container.with_env("REDIS__HOST", "redis")
    scene_manager_container.with_env("REDIS__PORT", "6379")

    # consumer.subscribe([scenes_topic])
    for message in scene_input_messages:
        producer.produce(scenes_topic, json.dumps(message).encode("utf-8"))
    producer.flush()

    scene_manager_container.start()

    consumer = Consumer(
        {
            "bootstrap.servers": kafka.get_bootstrap_server(),
            "group.id": "test-group-23132322",
            "security.protocol": "PLAINTEXT",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([merged_scenes_topic])

    merged_scene_topic_messages = []
    t0 = time.time()

    while time.time() - t0 < 30:
        message = consumer.poll(timeout=1.0)
        if message is None:
            continue
        if message.error():
            continue
        merged_scene_topic_messages.append(json.loads(message.value().decode("utf-8")))

    assert len(scene_output_messages) == len(merged_scene_topic_messages)

    for template, message in zip(scene_output_messages, merged_scene_topic_messages, strict=False):
        assert dict_match(template, message)
