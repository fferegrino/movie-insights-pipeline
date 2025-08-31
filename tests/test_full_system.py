# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "confluent-kafka",
#     "pytest",
#     "pytest-docker",
#     "requests",
# ]
# ///
import json
import os
import time
from pathlib import Path
from uuid import uuid4

import pytest
import requests
from confluent_kafka import Consumer


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return [
        Path(pytestconfig.rootdir) / "docker-compose.yml",
    ]


@pytest.fixture
def consume_messages(docker_services):
    kafka_port = docker_services.port_for("kafka", 9092)

    def _consume_messages(topic: str, timeout: int = 30, assert_message_number: int = -1):
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
            assert len(messages) == assert_message_number, f"For topic {topic}, expected {assert_message_number} but got {len(messages)}"
        
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
            new_files[key] = (value[0], file, value[1])
        response = requests.post(f"http://localhost:{chunker_port}/chunk-video", files=new_files)
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

def test_full_system(consume_messages, upload_file, wait_for_health):

    video_path = os.path.join(os.getcwd(), "movies", "pizza-conversation.mp4")

    wait_for_health()

    upload_response = upload_file({"video": (video_path, "video/mp4")})

    scene_chunks = consume_messages("video-chunks")

    scene_messages = consume_messages("scenes")

    unique_scenes = {message["scene_id"] for message in scene_messages}
    breakpoint()
    assert len(unique_scenes) >= 8



if __name__ == "__main__":
    pytest.main(["tests/"])
