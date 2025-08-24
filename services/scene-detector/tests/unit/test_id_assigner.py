from collections import defaultdict

import numpy as np
import pytest

from scene_detector.entities import Scene
from scene_detector.fingerprint import compute_fingerprint, fingerprint_distance
from scene_detector.id_assigner import IdAssigner
from scene_detector.storage.scene_index import SceneIndex, SceneMatch


class InMemorySceneIndex(SceneIndex):
    def __init__(self, threshold: int = 400, overlap_threshold_seconds: float = 0.1):
        self.scene_fingerprints = defaultdict(dict)  # video_id -> scene_id -> fingerprint
        self.scene_info = defaultdict(dict)  # video_id -> scene_id -> scene_info
        self.threshold = threshold
        self.overlap_threshold_seconds = overlap_threshold_seconds

    def add_scene(self, scene: Scene):
        self.scene_fingerprints[scene.video_id][scene.scene_id] = scene.fingerprint
        self.scene_info[scene.video_id][scene.scene_id] = {
            "video_start_time": scene.video_start_time,
            "video_end_time": scene.video_end_time,
        }

    def get_scene_fingerprint(self, video_id: str, scene_id: str) -> str:
        return self.scene_fingerprints[video_id][scene_id]

    def _overlap(self, scene_one: Scene, start_time: float, end_time: float) -> bool:
        """Check if a scene overlaps with another scene."""
        _start_time = start_time - self.overlap_threshold_seconds
        _end_time = end_time + self.overlap_threshold_seconds
        return (
            _start_time <= scene_one.video_start_time <= _end_time
            or _start_time <= scene_one.video_end_time <= _end_time
        )

    def find_match(self, scene: Scene) -> SceneMatch | None:
        video_scenes = self.scene_fingerprints[scene.video_id]
        for scene_id, stored_fp in video_scenes.items():
            dist = fingerprint_distance(scene.fingerprint, stored_fp)
            if dist <= self.threshold and self._overlap(
                scene,
                self.scene_info[scene.video_id][scene_id]["video_start_time"],
                self.scene_info[scene.video_id][scene_id]["video_end_time"],
            ):
                return SceneMatch(
                    scene_id=scene_id,
                    distance=dist,
                    video_id=scene.video_id,
                    video_start_time=self.scene_info[scene.video_id][scene_id]["video_start_time"],
                    video_end_time=self.scene_info[scene.video_id][scene_id]["video_end_time"],
                )
        return None

    def update_scene(self, scene: Scene):
        self.scene_info[scene.video_id][scene.scene_id] = {
            "video_start_time": scene.video_start_time,
            "video_end_time": scene.video_end_time,
        }


@pytest.fixture
def id_assigner():
    return IdAssigner(InMemorySceneIndex())


def create_scene(video_id: str, image_array: np.ndarray, video_start_time: float = 0, video_end_time: float = 10):
    return Scene(
        video_id=video_id,
        frame_start=0,
        frame_end=10,
        chunk_start_time=0,
        chunk_end_time=10,
        video_start_time=video_start_time,
        video_end_time=video_end_time,
        keyframe=image_array,
        fingerprint=compute_fingerprint(image_array),
    )


def test_id_assigner_assign_new_scene(id_assigner, image_as_array):
    scene = create_scene("video_1", image_as_array("frame_0.png"))

    assert scene.scene_id is None
    scene_id = id_assigner.assign(scene)

    assert scene.scene_id is not None
    assert scene.scene_id == scene_id


def test_id_assigner_assign_existing_scene(id_assigner, image_as_array):
    scene_one = create_scene("video_1", image_as_array("frame_0.png"))
    scene_two = create_scene("video_1", image_as_array("frame_0.png"))

    scene_one_id = id_assigner.assign(scene_one)
    scene_two_id = id_assigner.assign(scene_two)

    assert scene_one_id == scene_two_id


@pytest.mark.parametrize(
    "distinct_image, video_start_time, video_end_time",
    [
        ("frame_561.png", 0, 10),
        ("frame_580.png", 0, 10),
    ],
)
def test_id_assigner_assign_similar_frame_time_match(
    id_assigner, image_as_array, distinct_image, video_start_time, video_end_time
):
    scene_one = create_scene("video_1", image_as_array("frame_560.png"), 0, 10)
    scene_two = create_scene("video_1", image_as_array(distinct_image), video_start_time, video_end_time)

    scene_one_id = id_assigner.assign(scene_one)
    scene_two_id = id_assigner.assign(scene_two)

    assert scene_one_id == scene_two_id
    assert scene_one.scene_id is not None
    assert scene_two.scene_id is not None
    assert scene_one.scene_id == scene_two.scene_id


@pytest.mark.parametrize(
    "distinct_image, video_start_time, video_end_time",
    [
        ("frame_561.png", 1, 11),
        ("frame_580.png", 9, 13),
    ],
)
def test_id_assigner_assign_similar_frame_time_overlap(
    id_assigner, image_as_array, distinct_image, video_start_time, video_end_time
):
    scene_one = create_scene("video_1", image_as_array("frame_560.png"), 0, 10)
    scene_two = create_scene("video_1", image_as_array(distinct_image), video_start_time, video_end_time)

    scene_one_id = id_assigner.assign(scene_one)
    scene_two_id = id_assigner.assign(scene_two)

    assert scene_one_id == scene_two_id
    assert scene_one.scene_id is not None
    assert scene_two.scene_id is not None
    assert scene_one.scene_id == scene_two.scene_id


def test_id_assigner_assign_different_with_time_match(id_assigner, image_as_array):
    scene_one = create_scene("video_1", image_as_array("frame_0.png"))
    scene_two = create_scene("video_1", image_as_array("frame_10.png"))

    scene_one_id = id_assigner.assign(scene_one)
    scene_two_id = id_assigner.assign(scene_two)

    assert scene_one_id != scene_two_id
    assert scene_one.scene_id is not None
    assert scene_two.scene_id is not None
    assert scene_one.scene_id != scene_two.scene_id


@pytest.mark.parametrize(
    "distinct_image, video_start_time, video_end_time",
    [
        ("frame_560.png", 12, 13),
        ("frame_561.png", 12, 13),
        ("frame_580.png", 100, 101),
    ],
)
def test_id_assigner_assign_different_because_of_no_overlap(
    id_assigner, image_as_array, distinct_image, video_start_time, video_end_time
):
    scene_one = create_scene("video_1", image_as_array("frame_560.png"), 0, 10)
    scene_two = create_scene("video_1", image_as_array(distinct_image), video_start_time, video_end_time)

    scene_one_id = id_assigner.assign(scene_one)
    scene_two_id = id_assigner.assign(scene_two)

    assert scene_one_id != scene_two_id
    assert scene_one.scene_id is not None
    assert scene_two.scene_id is not None
    assert scene_one.scene_id != scene_two.scene_id
