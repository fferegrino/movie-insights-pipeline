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


def test_full_system(docker_services):
    kafka_port = docker_services.port_for("kafka", 9092)

    consumer = Consumer(
        {
            "bootstrap.servers": f"localhost:{kafka_port}",
            "group.id": f"test-group-{uuid4()}",
            "auto.offset.reset": "latest",
        }
    )

    video_path = os.path.join(os.getcwd(), "movies", "pizza-conversation.mp4")

    while True:
        try:
            response = requests.get("http://localhost:8000/health")
            if response.status_code == 200:
                break
        except Exception:
            pass

    with open(video_path, "rb") as video_file:
        files = {"video": ("pizza-conversation.mp4", video_file, "video/mp4")}
        try:
            response = requests.post("http://localhost:8000/chunk-video", files=files)
            assert response.status_code == 200
        except Exception:
            breakpoint()

    consumer.subscribe(["scenes"])
    start_time = time.time()
    messages = []
    while time.time() - start_time < 30:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise Exception(msg.error())
        messages.append(json.loads(msg.value().decode("utf-8")))

    with open("messages.json", "w") as f:
        for message in messages:
            f.write(json.dumps(message) + "\n")

    unique_scenes = {message["scene_id"] for message in messages}
    assert len(unique_scenes) == 8
