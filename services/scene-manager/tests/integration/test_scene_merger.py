import random
from uuid import uuid4

import pytest
import redis
from testcontainers.redis import RedisContainer

from scene_manager.entities import Interval
from scene_manager.scene_merger import SceneMerger


@pytest.fixture
def video_id():
    return str(uuid4())


@pytest.fixture
def input_scenes(video_id):
    scene0id = "xyz"
    scene1id = "abc"
    scene2id = "def"

    scene_0_0 = Interval(scene_id=scene0id, video_id=video_id, start=0, end=1)
    scene_0_1 = Interval(scene_id=scene0id, video_id=video_id, start=1, end=2)
    scene_0_2 = Interval(scene_id=scene0id, video_id=video_id, start=2, end=3)
    scene_1_0 = Interval(scene_id=scene1id, video_id=video_id, start=3, end=4)
    scene_1_1 = Interval(scene_id=scene1id, video_id=video_id, start=4, end=5)
    scene_1_2 = Interval(scene_id=scene1id, video_id=video_id, start=5, end=6)
    scene_1_3 = Interval(scene_id=scene1id, video_id=video_id, start=6, end=7)
    scene_2_0 = Interval(scene_id=scene2id, video_id=video_id, start=7, end=10)
    return [scene_0_0, scene_0_1, scene_0_2, scene_1_0, scene_1_1, scene_1_2, scene_1_3, scene_2_0]


@pytest.fixture
def expected_scenes(video_id):
    return [
        Interval(scene_id="xyz", video_id=video_id, start=0, end=3),
        Interval(scene_id="abc", video_id=video_id, start=3, end=7),
        Interval(scene_id="def", video_id=video_id, start=7, end=10),
    ]


def test_functionality_different_event_order(input_scenes, expected_scenes, video_id):
    with RedisContainer() as redis_container:
        redis_client = redis_container.get_client()

        video_metadata = {
            "duration": "10.0",
            "video_id": video_id,
            "original_name": "smallest.mp4",
            "fps": "25.0",
            "uri": f"s3://raw-videos/{video_id}/aa.mp4",
        }
        redis_client.hset(f"video-metadata:{video_id}", mapping=video_metadata)

        for idx in range(10):
            random.shuffle(input_scenes)
            redis_client = redis.Redis(
                host=redis_container.get_container_host_ip(),
                port=redis_container.get_exposed_port(6379),
                db=0,
                decode_responses=True,
            )

            scene_manager = SceneMerger(redis_client)
            scene_manager.scene_intervals_namespace = f"test_{idx}"

            for event in input_scenes[:-1]:
                assert scene_manager.insert_scene(event) is None
            val = scene_manager.insert_scene(input_scenes[-1])

            assert val == expected_scenes
