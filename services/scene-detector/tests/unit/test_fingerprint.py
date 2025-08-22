import pytest

from scene_detector.fingerprint import compute_fingerprint, fingerprint_distance


def test_compute_fingerprint(image_as_array):
    frame = image_as_array("frame_560.png")
    fingerprint = compute_fingerprint(frame)
    assert fingerprint == (
        "fff807fffff803ff01fc00ff00fc00f0287e00f0003e0070001f8000023f8000"
        "03ffe00007fff80001fffc000efffc000ffffe000ffffe003fffff008c7fff00"
        "6b39fdc0403bffc0000fdf98000fff8800ff1fbc208f9ff401f9d7e201f87773"
        "07fc247117ce08780bfe00fc1ffe18163ffe1807bffe0c07bffed8011f7e7a09"
    )


@pytest.mark.parametrize(
    "image_one, image_two, expected_distance",
    [
        ("frame_560.png", "frame_560.png", 0),
        ("frame_560.png", "frame_561.png", 4),
        ("frame_0.png", "frame_10.png", 486),
        ("frame_0.png", "frame_1000.png", 511),
    ],
)
def test_fingerprint_distance_exact_match(image_as_array, image_one, image_two, expected_distance):
    fingerprint1 = compute_fingerprint(image_as_array(image_one))
    fingerprint2 = compute_fingerprint(image_as_array(image_two))
    assert fingerprint_distance(fingerprint1, fingerprint2) == expected_distance
