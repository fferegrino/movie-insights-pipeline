"""
Fingerprint computation and comparison utilities for video scene detection.

This module provides functions for generating perceptual hashes from video frames
and comparing them using Hamming distance. Perceptual hashing allows for efficient
similarity detection between frames, making it useful for identifying duplicate
or similar scenes in video content.

The module uses the imagehash library to compute perceptual hashes (pHash) which
are robust to minor variations in lighting, compression, and small transformations
while still being sensitive enough to detect scene changes.

Example:
    >>> import numpy as np
    >>> from scene_detector.fingerprint import compute_fingerprint, fingerprint_distance
    >>>
    >>> # Generate a sample frame (in practice, this would be from a video)
    >>> frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
    >>>
    >>> # Compute fingerprint for the frame
    >>> fingerprint = compute_fingerprint(frame, hash_size=16)
    >>> print(f"Frame fingerprint: {fingerprint}")
    >>>
    >>> # Compare two fingerprints
    >>> fingerprint2 = compute_fingerprint(frame, hash_size=16)
    >>> distance = fingerprint_distance(fingerprint, fingerprint2)
    >>> print(f"Distance between fingerprints: {distance}")

"""

import imagehash
import numpy as np
from PIL import Image

_EXPECTED_NUM_CHANNELS = 3


def compute_fingerprint(frame: np.ndarray, hash_size: int = 16) -> str:
    """
    Compute a perceptual hash (pHash) for a video frame.

    Converts the input frame to a perceptual hash that can be used for
    similarity comparison. The perceptual hash is robust to minor variations
    in lighting, compression artifacts, and small geometric transformations,
    making it ideal for detecting similar scenes in video content.

    Args:
        frame (np.ndarray): Input video frame as a numpy array with shape (height, width, channels).
                           Expected to be in RGB format with uint8 data type.
        hash_size (int, optional): Size of the hash to generate. Larger values provide
                                 more precision but require more computation. Must be a
                                 power of 2. Defaults to 16.

    Returns:
        str: Hexadecimal string representation of the perceptual hash

    Raises:
        ValueError: If hash_size is not a power of 2 or is less than 1
        TypeError: If frame is not a numpy array
        ValueError: If frame has invalid shape or data type

    Example:
        >>> import numpy as np
        >>> from scene_detector.fingerprint import compute_fingerprint
        >>>
        >>> # Create a sample frame (in practice, this would be from a video file)
        >>> frame = np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8)
        >>>
        >>> # Compute fingerprint with default hash size
        >>> fingerprint = compute_fingerprint(frame)
        >>> print(f"Fingerprint: {fingerprint}")
        >>>
        >>> # Compute fingerprint with larger hash size for more precision
        >>> fingerprint_32 = compute_fingerprint(frame, hash_size=32)
        >>> print(f"32-bit fingerprint: {fingerprint_32}")

    """
    if not isinstance(frame, np.ndarray):
        raise TypeError("frame must be a numpy array")

    if frame.ndim != _EXPECTED_NUM_CHANNELS or frame.shape[2] not in [1, 3, 4]:
        raise ValueError("frame must be a 3D array with 1, 3, or 4 channels")

    if frame.dtype != np.uint8:
        raise ValueError("frame must have uint8 data type")

    if hash_size < 1 or (hash_size & (hash_size - 1)) != 0:
        raise ValueError("hash_size must be a positive power of 2")

    pil_img = Image.fromarray(frame)

    hash_obj = imagehash.phash(pil_img, hash_size=hash_size)

    return str(hash_obj)


def fingerprint_distance(hash1: str, hash2: str) -> int:
    """
    Calculate the Hamming distance between two perceptual hashes.

    The Hamming distance represents the number of bits that differ between
    two hashes. A distance of 0 means the hashes are identical, while larger
    distances indicate greater differences. This is used to determine if two
    frames represent the same or similar scenes.

    Args:
        hash1 (str): First perceptual hash as a hexadecimal string
        hash2 (str): Second perceptual hash as a hexadecimal string

    Returns:
        int: Hamming distance between the two hashes (0 to hash bit length)

    Raises:
        ValueError: If either hash is not a valid hexadecimal string
        ValueError: If the hashes have different bit lengths

    Example:
        >>> from scene_detector.fingerprint import fingerprint_distance
        >>>
        >>> # Compare two similar fingerprints
        >>> hash1 = "a1b2c3d4e5f67890"
        >>> hash2 = "a1b2c3d4e5f67891"
        >>> distance = fingerprint_distance(hash1, hash2)
        >>> print(f"Distance: {distance}")
        >>>
        >>> # Determine if frames are similar (distance <= threshold)
        >>> threshold = 10
        >>> if distance <= threshold:
        ...     print("Frames are similar")
        ... else:
        ...     print("Frames are different")

    Note:
        The threshold for considering two fingerprints similar depends on the
        hash size used. For a 16-bit hash, typical thresholds range from 5-15.
        For larger hash sizes, the threshold should be scaled proportionally.

    """
    if not isinstance(hash1, str) or not isinstance(hash2, str):
        raise TypeError("Both hash1 and hash2 must be strings")

    try:
        hash_obj1 = imagehash.hex_to_hash(hash1)
        hash_obj2 = imagehash.hex_to_hash(hash2)

        return hash_obj1 - hash_obj2

    except ValueError as e:
        raise ValueError(f"Invalid hash format: {e}") from e
    except Exception as e:
        raise ValueError(f"Error computing hash distance: {e}") from e
