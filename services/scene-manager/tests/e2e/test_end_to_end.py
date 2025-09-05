import json

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
def scene_messages(pytestconfig):
    with open(pytestconfig.rootdir / "tests/fixtures/scene_messages.jsonl") as f:
        return [json.loads(line) for line in f.readlines()]


def test_end_to_end(
    kafka, redis, scene_manager_container, scenes_topic, merged_scenes_topic, scene_messages, producer, consumer
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
    for message in scene_messages:
        producer.produce(scenes_topic, json.dumps(message).encode("utf-8"))
    producer.flush()

    scene_manager_container.start()

    # time.sleep(10)
    breakpoint()

    # # Wait for the container to start
    # time.sleep(10)

    # # Send a message to the Kafka topic
    # producer = Producer(
    #     {
    #         "bootstrap.servers": kafka.get_bootstrap_server(),
    #         "security.protocol": "PLAINTEXT",
    #     }
    # )

    # chunk_message = {
    #     "chunk_count": 2,
    #     "chunk_idx": 2,
    #     "end_ts": 10,
    #     "fps": 25.0,
    #     "id": "78eb6d79-81f7-4d1f-b2d0-d8574cd7de87/chunk_000002_000002.mp4",
    #     "overlap_left": 1,
    #     "overlap_right": 0.0,
    #     "settings": {"audio_codec": "aac", "codec": "libx264"},
    #     "start_ts": 5,
    #     "uri": "s3://chunked-videos/78eb6d79-81f7-4d1f-b2d0-d8574cd7de87/chunk_000002_000002.mp4",
    #     "video_id": "78eb6d79-81f7-4d1f-b2d0-d8574cd7de87",
    # }

    # producer.produce(raw_chunks_topic, json.dumps(chunk_message))
    # producer.flush()

    # consumer = Consumer(
    #     {
    #         "bootstrap.servers": kafka.get_bootstrap_server(),
    #         "group.id": "test-group-23132322",
    #         "security.protocol": "PLAINTEXT",
    #         "auto.offset.reset": "earliest",
    #     }
    # )
    # consumer.subscribe([raw_chunks_topic])
    # time.sleep(1)
    # message = consumer.poll(timeout=1.0)
    # if message is None:
    #     raise Exception("No message received")
    # if message.error():
    #     raise Exception(message.error())

    # message_value = json.loads(message.value().decode("utf-8"))
    # assert message_value == chunk_message

    # scene_detector_container.start()

    # time.sleep(10)

    # consumer.unsubscribe()
    # consumer.close()

    # consumer = Consumer(
    #     {
    #         "bootstrap.servers": kafka.get_bootstrap_server(),
    #         "group.id": "test-group-23132322",
    #         "security.protocol": "PLAINTEXT",
    #         "auto.offset.reset": "earliest",
    #     }
    # )
    # consumer.subscribe([scenes_topic])
    # time.sleep(1)
    # message = consumer.poll(timeout=1.0)
    # if message is None:
    #     raise Exception("No message received")
    # if message.error():
    #     raise Exception(message.error())

    # message_value = json.loads(message.value().decode("utf-8"))
