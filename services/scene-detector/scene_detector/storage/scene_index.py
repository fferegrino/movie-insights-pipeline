"""
Abstract base class for scene fingerprint storage backends.

This module defines the SceneIndex interface that all scene storage implementations
must follow. It provides a common contract for storing and retrieving scene
fingerprints with similarity matching capabilities.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from scene_detector.entities import Scene


@dataclass
class SceneMatch:
    """Represents a match between a scene and a stored scene."""

    scene_id: str
    """The ID of the matching scene."""

    video_id: str
    """The ID of the video containing the matching scene."""

    distance: float
    """The distance between the scene and the matching scene."""

    video_start_time: float
    """Scene start time in seconds relative to the video."""

    video_end_time: float
    """Scene end time in seconds relative to the video."""


class SceneIndex(ABC):
    """
    Abstract base class for scene fingerprint storage and retrieval.

    Defines the interface that all scene storage backends must implement.
    Provides methods for storing scene fingerprints, retrieving them, and
    finding similar scenes based on fingerprint matching.

    Implementations should provide efficient storage and retrieval of scene
    fingerprints, with support for similarity-based matching using appropriate
    distance metrics.
    """

    @abstractmethod
    def add_scene(self, scene: Scene):
        """
        Add a scene fingerprint to the storage backend.

        Args:
            scene: Scene object to add to the storage backend, it should contain
                all the information to identify the scene, including a `scene_id`.

        """

    @abstractmethod
    def get_scene_fingerprint(self, video_id: str, scene_id: str) -> str:
        """
        Retrieve a scene fingerprint from the storage backend.

        Args:
            video_id: Unique identifier for the video
            scene_id: Unique identifier for the scene within the video

        Returns:
            The fingerprint data for the scene, or None if not found

        """

    @abstractmethod
    def find_match(self, scene: Scene) -> SceneMatch | None:
        """
        Find a matching scene in the index.

        Args:
            scene: Scene object to search for in the index.

        Returns:
            SceneMatch object, or None if no match found

        """
