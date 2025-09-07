import json
import time
from pathlib import Path
from uuid import uuid4


import pytest
import requests
from confluent_kafka import Consumer


@pytest.fixture(scope="session")
def root_dir(pytestconfig):
    if pytestconfig.rootdir.basename == "tests":
        return Path(pytestconfig.rootdir).parent
    return Path(pytestconfig.rootdir)


@pytest.fixture(scope="session")
def docker_compose_file(root_dir):
    return [
        root_dir / "docker-compose.yml",
    ]


@pytest.fixture
def pattern_handlers():
    return {
        "uuid4": r"[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}",
    }


@pytest.fixture
def consume_messages(docker_services):
    kafka_port = docker_services.port_for("kafka", 9092)

    def _consume_messages(
        topic: str, timeout: int = 30, assert_message_number: int = -1
    ):
        consumer = Consumer(
            {
                "bootstrap.servers": f"localhost:{kafka_port}",
                "group.id": f"test-group-{uuid4()}",
                "auto.offset.reset": "earliest",
            }
        )

        consumer.subscribe([topic])

        start_time = time.time()
        messages = []
        while time.time() - start_time < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                raise Exception(msg.error())
            messages.append(json.loads(msg.value().decode("utf-8")))

        if assert_message_number != -1:
            assert len(messages) == assert_message_number, (
                f"For topic {topic}, expected {assert_message_number} but got {len(messages)}"
            )

        consumer.close()
        return messages

    return _consume_messages


@pytest.fixture
def upload_file(docker_services):
    chunker_port = docker_services.port_for("video-chunker", 8000)

    def _upload_file(files: dict):
        new_files = {}
        _files = []
        for key, value in files.items():
            file = open(value[0], "rb")
            new_files[key] = (str(value[0]), file, value[1])
        response = requests.post(
            f"http://localhost:{chunker_port}/chunk-video", files=new_files
        )
        assert response.status_code == 200
        return response.json()

    return _upload_file


@pytest.fixture
def wait_for_health(docker_services):
    chunker_port = docker_services.port_for("video-chunker", 8000)

    def _wait_for_health():
        t0 = time.time()
        while time.time() - t0 < 30:
            try:
                response = requests.get(f"http://localhost:{chunker_port}/health")
                if response.status_code == 200:
                    break
            except Exception:
                pass
            time.sleep(1)

    return _wait_for_health


@pytest.fixture
def load_messages_from_jsonl(root_dir):
    def _load_messages_from_jsonl(filename):
        with open(root_dir / "tests" / "fixtures" / filename, "r") as f:
            return [json.loads(line) for line in f]

    return _load_messages_from_jsonl


def test_full_system(
    consume_messages,
    root_dir,
    dict_matcher,
    upload_file,
    wait_for_health,
    load_messages_from_jsonl,
):
    video_path = root_dir / "movies" / "pizza-conversation.mp4"

    wait_for_health()

    upload_response = upload_file({"video": (video_path, "video/mp4")})

    scene_chunks = consume_messages("video-chunks")
    expected_scene_chunks = load_messages_from_jsonl("video_chunks.jsonl")

    for expected, actual in zip(expected_scene_chunks, scene_chunks):
        dict_matcher.match(expected, actual)

    scene_messages = consume_messages("scenes")
    expected_scene_messages = load_messages_from_jsonl("scene_messages.jsonl")

    for expected, actual in zip(expected_scene_messages, scene_messages):
        dict_matcher.match(expected, actual)


if __name__ == "__main__":
    pytest.main(["."])
