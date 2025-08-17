"""
Abstract base class for scene fingerprint storage backends.

This module defines the SceneIndex interface that all scene storage implementations
must follow. It provides a common contract for storing and retrieving scene
fingerprints with similarity matching capabilities.
"""

from abc import ABC, abstractmethod


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
    def add_scene_fingerprint(self, video_id: str, scene_id: str, fingerprint: str):
        """
        Add a scene fingerprint to the storage backend.

        Args:
            video_id: Unique identifier for the video
            scene_id: Unique identifier for the scene within the video
            fingerprint: Fingerprint data for the scene

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
    def find_match(self, video_id: str, fingerprint: str) -> str:
        """
        Find a matching scene fingerprint within the similarity threshold.

        Args:
            video_id: Unique identifier for the video to search within
            fingerprint: Fingerprint data to match against

        Returns:
            Scene ID of the first matching scene, or None if no match found

        """
