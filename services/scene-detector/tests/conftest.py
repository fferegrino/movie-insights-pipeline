import json
import re
from unittest.mock import ANY

import numpy as np
import pytest
from PIL import Image
from testcontainers.core.network import Network
from testcontainers.kafka import KafkaContainer
from testcontainers.minio import MinioContainer
from testcontainers.redis import RedisContainer

from scene_detector.id_assigner import IdAssigner
from tests.in_memory_scene_index import InMemorySceneIndex

placeholder_regex = re.compile(r"\[(.*?)\]")


def replace_placeholders(json_data: dict, *, replacements: dict[str, str], replace_any: bool = True):
    for key, value in json_data.items():
        if isinstance(value, dict):
            json_data[key] = replace_placeholders(json_data[key], replacements, replace_any)
        elif isinstance(value, str):
            if replace_any and value == "ANY":
                json_data[key] = ANY
            elif match := placeholder_regex.match(value):
                json_data[key] = replacements.get(match.group(1), value)
            else:
                json_data[key] = value
        else:
            json_data[key] = value
    return json_data


@pytest.fixture
def jsonl_fixture(path_for_fixture):
    def _jsonl_fixture(filename):
        with open(path_for_fixture(filename)) as f:
            return [json.loads(line) for line in f]

    return _jsonl_fixture


@pytest.fixture
def get_json_fixture(path_for_fixture):
    def _get_json_fixture(filename, *, replacements: dict[str, str] = None, replace_any: bool = True):
        with open(path_for_fixture(filename)) as f:
            return replace_placeholders(json.load(f), replacements=replacements or {}, replace_any=replace_any)

    return _get_json_fixture


@pytest.fixture
def test_network():
    network = Network()
    try:
        with network:
            yield network
    except Exception:
        pass


@pytest.fixture
def s3(test_network):
    minio = MinioContainer()
    minio.with_env("MINIO_ACCESS_KEY", "minioadmin")
    minio.with_env("MINIO_SECRET_KEY", "minioadmin")
    minio.with_name("s3")
    minio.with_network(test_network)
    minio.with_exposed_ports(9000).with_exposed_ports(9001)
    with minio:
        yield minio


@pytest.fixture
def kafka(test_network):
    kafka = KafkaContainer()
    kafka.with_name("kafka")
    kafka.with_network(test_network)
    with kafka:
        yield kafka


@pytest.fixture
def redis(test_network):
    redis = RedisContainer()
    redis.with_name("redis")
    redis.with_network(test_network)
    with redis:
        yield redis


@pytest.fixture
def image_as_array(path_for_fixture):
    def _image_as_array(filename):
        return np.array(Image.open(path_for_fixture(f"images/{filename}")))

    return _image_as_array


@pytest.fixture
def in_memory_id_assigner():
    return IdAssigner(InMemorySceneIndex())
