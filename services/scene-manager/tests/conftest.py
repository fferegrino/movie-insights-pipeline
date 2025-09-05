from pathlib import Path

import pytest
from testcontainers.core.network import Network
from testcontainers.kafka import KafkaContainer
from testcontainers.redis import RedisContainer


@pytest.fixture
def fixture_path(pytestconfig):
    def _fixture_path(filename):
        return Path(pytestconfig.rootdir) / "tests" / "fixtures" / filename

    return _fixture_path


@pytest.fixture
def test_network():
    network = Network()
    try:
        with network:
            yield network
    except Exception:
        pass


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
