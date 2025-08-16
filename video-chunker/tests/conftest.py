import json
import re
from pathlib import Path
from unittest.mock import ANY

import pytest
from testcontainers.core.network import Network
from testcontainers.minio import MinioContainer


@pytest.fixture
def fixture_path(pytestconfig):
    def _fixture_path(filename):
        return Path(pytestconfig.rootdir) / "tests" / "fixtures" / filename

    return _fixture_path


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
def get_json_fixture(fixture_path):
    def _get_json_fixture(filename, *, replacements: dict[str, str] = None, replace_any: bool = True):
        with open(fixture_path(filename)) as f:
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
    with minio:
        yield minio
