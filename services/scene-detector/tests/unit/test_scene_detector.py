from scene_detector.scenes import detect_scenes


def test_detect_scenes():
    scenes = detect_scenes("tests/fixtures/videos/big_buck_bunny_01@480p30.mp4", chunk_relative_start_time=0)
    assert len(scenes) == 4
