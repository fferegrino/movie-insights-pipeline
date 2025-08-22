"""
Data entities for video scene detection.

This module defines the Scene dataclass which represents a detected video scene
with its metadata and visual data.
"""

from dataclasses import dataclass

import numpy as np


@dataclass
class Scene:
    """
    Represents a detected scene in a video.

    Contains temporal boundaries, visual data (keyframe), and identification
    information for scene deduplication and matching.

    Example:
        >>> scene = Scene(
        ...     video_id="movie.mp4",
        ...     frame_start=100,
        ...     frame_end=150,
        ...     chunk_start_time=4.0,
        ...     chunk_end_time=6.0,
        ...     keyframe=np.zeros((480, 640, 3), dtype=np.uint8)
        ... )

    """

    video_id: str
    """Source video identifier."""

    frame_start: int
    """Starting frame number (0-based)."""

    frame_end: int
    """Ending frame number (inclusive)."""

    chunk_start_time: float
    """Scene start time in seconds relative to the chunk."""

    chunk_end_time: float
    """Scene end time in seconds relative to the chunk."""

    video_start_time: float
    """Scene start time in seconds relative to the video."""

    video_end_time: float
    """Scene end time in seconds relative to the video."""

    keyframe: np.ndarray
    """Representative frame as numpy array (BGR format)."""

    fingerprint: str = None
    """Perceptual hash for similarity matching."""

    scene_id: str = None
    """Unique scene identifier for deduplication."""
