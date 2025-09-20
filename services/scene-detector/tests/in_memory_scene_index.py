from collections import defaultdict

from scene_detector.entities import Scene
from scene_detector.fingerprint import fingerprint_distance
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
