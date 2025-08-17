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
        ...     start_time=4.0,
        ...     end_time=6.0,
        ...     keyframe=np.zeros((480, 640, 3), dtype=np.uint8)
        ... )

    """

    video_id: str
    """Source video identifier."""

    frame_start: int
    """Starting frame number (0-based)."""

    frame_end: int
    """Ending frame number (inclusive)."""

    start_time: float
    """Start time in seconds."""

    end_time: float
    """End time in seconds."""

    keyframe: np.ndarray
    """Representative frame as numpy array (BGR format)."""

    fingerprint: str = None
    """Perceptual hash for similarity matching."""

    scene_id: str = None
    """Unique scene identifier for deduplication."""
