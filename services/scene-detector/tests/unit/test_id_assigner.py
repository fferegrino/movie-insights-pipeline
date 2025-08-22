from collections import defaultdict

import numpy as np
import pytest

from scene_detector.entities import Scene
from scene_detector.fingerprint import compute_fingerprint, fingerprint_distance
from scene_detector.id_assigner import IdAssigner
from scene_detector.storage.scene_index import SceneIndex


class InMemorySceneIndex(SceneIndex):
    def __init__(self, threshold: int = 400):
        self.scene_fingerprints = defaultdict(dict)  # video_id -> scene_id -> fingerprint
        self.threshold = threshold

    def add_scene(self, scene: Scene):
        self.scene_fingerprints[scene.video_id][scene.scene_id] = scene.fingerprint

    def get_scene_fingerprint(self, video_id: str, scene_id: str) -> str:
        return self.scene_fingerprints[video_id][scene_id]

    def find_match(self, scene: Scene) -> str:
        video_scenes = self.scene_fingerprints[scene.video_id]
        for scene_id, stored_fp in video_scenes.items():
            dist = fingerprint_distance(scene.fingerprint, stored_fp)
            if dist <= self.threshold:
                return scene_id
        return None


@pytest.fixture
def id_assigner():
    return IdAssigner(InMemorySceneIndex())


def create_scene(video_id: str, image_array: np.ndarray):
    return Scene(
        video_id=video_id,
        frame_start=0,
        frame_end=10,
        chunk_start_time=0,
        chunk_end_time=10,
        video_start_time=0,
        video_end_time=10,
        keyframe=image_array,
        fingerprint=compute_fingerprint(image_array),
    )


def test_id_assigner_assign(id_assigner, image_as_array):
    scene = create_scene("video_1", image_as_array("frame_0.png"))

    assert scene.scene_id is None
    id_assigner.assign(scene)

    assert scene.scene_id is not None


def test_id_assigner_assign_same_frame(id_assigner, image_as_array):
    scene_one = create_scene("video_1", image_as_array("frame_0.png"))
    scene_two = create_scene("video_1", image_as_array("frame_0.png"))

    scene_one.scene_id = id_assigner.assign(scene_one)
    scene_two.scene_id = id_assigner.assign(scene_two)

    assert scene_one.scene_id == scene_two.scene_id


@pytest.mark.parametrize(
    "distinct_image",
    [
        "frame_561.png",
        "frame_580.png",
    ],
)
def test_id_assigner_assign_similar_frame(id_assigner, image_as_array, distinct_image):
    scene_one = create_scene("video_1", image_as_array("frame_560.png"))
    scene_two = create_scene("video_1", image_as_array(distinct_image))

    scene_one.scene_id = id_assigner.assign(scene_one)
    scene_two.scene_id = id_assigner.assign(scene_two)

    assert scene_one.scene_id == scene_two.scene_id


def test_id_assigner_assign_different(id_assigner, image_as_array):
    scene_one = create_scene("video_1", image_as_array("frame_0.png"))
    scene_two = create_scene("video_1", image_as_array("frame_10.png"))

    scene_one.scene_id = id_assigner.assign(scene_one)
    scene_two.scene_id = id_assigner.assign(scene_two)
    assert scene_one.scene_id != scene_two.scene_id
