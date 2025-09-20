"""
Test for a very specific case.

There is a single scene that spans three clips because of the overlap threshold.
We want to make sure that the scene is assigned to the correct clip.

"""

import itertools

import pytest
from testcontainers.redis import RedisContainer

from scene_detector.id_assigner import IdAssigner
from scene_detector.scenes import detect_scenes
from scene_detector.storage.redis_scene_index import RedisSceneIndex


@pytest.fixture
def redis_client():
    with RedisContainer() as redis:
        yield redis.get_client(decode_responses=True)


@pytest.fixture
def redis_id_assigner(redis_client):
    redis_scene_index = RedisSceneIndex(redis_client, threshold=400)
    return IdAssigner(redis_scene_index)


def test_thing(path_for_fixture, redis_id_assigner):
    id_assigner = redis_id_assigner

    detected_scenes_01 = detect_scenes(
        video_path=str(path_for_fixture("videos/chunk_000001_000009.mp4")),
        chunk_relative_start_time=0,
    )

    detected_scenes_02 = detect_scenes(
        video_path=str(path_for_fixture("videos/chunk_000002_000009.mp4")),
        chunk_relative_start_time=4,
    )
    detected_scenes_03 = detect_scenes(
        video_path=str(path_for_fixture("videos/chunk_000003_000009.mp4")),
        chunk_relative_start_time=9,
    )

    detected_scenes_04 = detect_scenes(
        video_path=str(path_for_fixture("videos/chunk_000004_000009.mp4")),
        chunk_relative_start_time=14,
    )
    detected_scenes_05 = detect_scenes(
        video_path=str(path_for_fixture("videos/chunk_000005_000009.mp4")),
        chunk_relative_start_time=19,
    )

    for scene in itertools.chain(
        detected_scenes_01, detected_scenes_02, detected_scenes_03, detected_scenes_04, detected_scenes_05
    ):
        scene.video_id = "video_1"

    for scene in itertools.chain(detected_scenes_01, detected_scenes_02, detected_scenes_03):
        id_assigner.assign(scene)

    idx_scene_04 = []
    for scene in detected_scenes_04:
        idx_scene_04.append(id_assigner.assign(scene))

    thing_2 = id_assigner.assign(detected_scenes_05[0])
    assert idx_scene_04[-2] == thing_2
