import pytest

from scene_detector.scenes import detect_scenes


@pytest.mark.parametrize(
    "video, expected_scenes",
    [
        ("chunk_000009_000009", 1),
        ("chunk_000001_000009", 2),
    ],
)
def test_detect_scenes(fixture_path, video, expected_scenes):
    scenes = detect_scenes(str(fixture_path(f"videos/{video}.mp4")), chunk_relative_start_time=0)
    assert len(scenes) == expected_scenes
