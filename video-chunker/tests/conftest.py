from pathlib import Path

import pytest
from testcontainers.core.network import Network
from testcontainers.minio import MinioContainer


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
def s3(test_network):
    minio = MinioContainer()
    minio.with_env("MINIO_ACCESS_KEY", "minioadmin")
    minio.with_env("MINIO_SECRET_KEY", "minioadmin")
    minio.with_name("s3")
    minio.with_network(test_network)
    with minio:
        yield minio
