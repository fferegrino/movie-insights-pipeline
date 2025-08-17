import pytest
from testcontainers.redis import RedisContainer

from scene_detector.fingerprint import compute_fingerprint
from scene_detector.storage.redis_scene_index import RedisSceneIndex


@pytest.fixture
def redis_client():
    with RedisContainer() as redis:
        yield redis.get_client(decode_responses=True)


@pytest.fixture
def image_fingerprint(image_as_array):
    def _image_fingerprint(image_name: str):
        return compute_fingerprint(image_as_array(image_name))

    return _image_fingerprint


def test_redis_scene_index_add_one(redis_client, image_fingerprint):
    redis_scene_index = RedisSceneIndex(redis_client)

    redis_scene_index.add_scene_fingerprint("video_1", "scene_1", image_fingerprint("frame_0.png"))

    current_fingerprints = redis_client.hgetall("video:video_1:scenes")
    assert current_fingerprints == {"scene_1": image_fingerprint("frame_0.png")}


def test_redis_scene_index_find_match(redis_client, image_fingerprint):
    redis_scene_index = RedisSceneIndex(redis_client)

    redis_scene_index.add_scene_fingerprint("video_1", "scene_1", image_fingerprint("frame_0.png"))

    current_fingerprints = redis_client.hgetall("video:video_1:scenes")
    assert current_fingerprints == {"scene_1": image_fingerprint("frame_0.png")}

    assert redis_scene_index.find_match("video_1", image_fingerprint("frame_0.png")) == "scene_1"
