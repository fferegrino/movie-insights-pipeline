import pytest

from scene_detector.fingerprint import compute_fingerprint, fingerprint_distance


def test_compute_fingerprint(image_as_array):
    frame = image_as_array("frame_560.png")
    fingerprint = compute_fingerprint(frame)
    assert fingerprint == "c4694538a522b592f294d32b9bd9cdf4d9b8e8cd9c5ab8a9d404da2ed216f546"


@pytest.mark.parametrize(
    "image_one, image_two, expected_distance",
    [
        ("frame_560.png", "frame_560.png", 0),
        ("frame_560.png", "frame_561.png", 2),
        ("frame_0.png", "frame_10.png", 128),
        ("frame_0.png", "frame_1000.png", 128),
    ],
)
def test_fingerprint_distance_exact_match(image_as_array, image_one, image_two, expected_distance):
    fingerprint1 = compute_fingerprint(image_as_array(image_one))
    fingerprint2 = compute_fingerprint(image_as_array(image_two))
    assert fingerprint_distance(fingerprint1, fingerprint2) == expected_distance
