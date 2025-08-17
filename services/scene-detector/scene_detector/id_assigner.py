"""
Scene ID assignment and deduplication logic.

This module provides the IdAssigner class which handles the assignment of unique
identifiers to video scenes while preventing duplicates. It uses fingerprint-based
matching to identify similar scenes and assign consistent IDs across different
video processing runs.
"""

import uuid

from scene_detector.entities import Scene
from scene_detector.storage.scene_index import SceneIndex


class IdAssigner:
    """
    Assigns unique identifiers to video scenes with deduplication support.

    The IdAssigner is responsible for managing scene identifiers in a way that
    ensures similar scenes (based on their fingerprints) receive the same ID,
    while new scenes get unique identifiers. This enables consistent scene
    tracking across multiple video processing sessions and prevents duplicate
    scene entries in the database.

    The assignment process works as follows:
    1. Check if a similar scene already exists using fingerprint matching
    2. If a match is found, reuse the existing scene ID
    3. If no match is found, generate a new UUID and store the fingerprint

    Attributes:
        index (SceneIndex): Storage backend for scene fingerprints and IDs

    Example:
        >>> from scene_detector.storage.redis_scene_index import RedisSceneIndex
        >>> from scene_detector.entities import Scene
        >>> import redis
        >>> import numpy as np
        >>>
        >>> # Initialize storage and assigner
        >>> redis_client = redis.Redis(host='localhost', port=6379, db=0)
        >>> scene_index = RedisSceneIndex(redis_client, threshold=10)
        >>> assigner = IdAssigner(scene_index)
        >>>
        >>> # Create a scene
        >>> scene = Scene(
        ...     video_id="movie_123",
        ...     frame_start=100,
        ...     frame_end=150,
        ...     start_time=4.0,
        ...     end_time=6.0,
        ...     keyframe=np.random.randint(0, 255, (480, 640, 3), dtype=np.uint8),
        ...     fingerprint="abc123def456"
        ... )
        >>>
        >>> # Assign ID to the scene
        >>> scene_id = assigner.assign(scene)
        >>> print(f"Assigned scene ID: {scene_id}")
        >>> print(f"Scene object updated: {scene.scene_id}")

    """

    def __init__(self, index: SceneIndex):
        """
        Initialize the IdAssigner with a scene storage backend.

        Args:
            index (SceneIndex): Storage backend that implements the SceneIndex interface.
                              Used for fingerprint storage and similarity matching.

        Example:
            >>> from scene_detector.storage.redis_scene_index import RedisSceneIndex
            >>> import redis
            >>>
            >>> redis_client = redis.Redis(host='localhost', port=6379, db=0)
            >>> scene_index = RedisSceneIndex(redis_client)
            >>> assigner = IdAssigner(scene_index)

        """
        self.index = index

    def assign(self, scene: Scene) -> str:
        """
        Assign a unique identifier to a scene with deduplication.

        This method performs fingerprint-based matching to determine if the scene
        already exists in the storage backend. If a similar scene is found (within
        the configured similarity threshold), the existing scene ID is reused.
        Otherwise, a new UUID is generated and the scene fingerprint is stored.

        The scene object is modified in-place to set the scene_id attribute.

        Args:
            scene (Scene): Scene object containing video metadata and fingerprint.
                         Must have video_id and fingerprint attributes set.
                         The scene_id will be set by this method.

        Returns:
            str: The assigned scene ID (either existing or newly generated)

        Raises:
            ValueError: If scene.video_id or scene.fingerprint is None
            AttributeError: If scene object is missing required attributes

        Example:
            >>> from scene_detector.entities import Scene
            >>> import numpy as np
            >>>
            >>> # Create a scene with fingerprint
            >>> scene = Scene(
            ...     video_id="video_123",
            ...     frame_start=0,
            ...     frame_end=30,
            ...     start_time=0.0,
            ...     end_time=1.0,
            ...     keyframe=np.zeros((480, 640, 3), dtype=np.uint8),
            ...     fingerprint="unique_fingerprint_123"
            ... )
            >>>
            >>> # Assign ID (this will generate a new UUID)
            >>> scene_id = assigner.assign(scene)
            >>> print(f"New scene ID: {scene_id}")
            >>> assert scene.scene_id == scene_id
            >>>
            >>> # Create another scene with same fingerprint
            >>> duplicate_scene = Scene(
            ...     video_id="video_123",
            ...     frame_start=60,
            ...     frame_end=90,
            ...     start_time=2.0,
            ...     end_time=3.0,
            ...     keyframe=np.zeros((480, 640, 3), dtype=np.uint8),
            ...     fingerprint="unique_fingerprint_123"  # Same fingerprint
            ... )
            >>>
            >>> # Assign ID (this should reuse the existing ID)
            >>> duplicate_id = assigner.assign(duplicate_scene)
            >>> print(f"Duplicate scene ID: {duplicate_id}")
            >>> assert duplicate_id == scene_id  # Same ID for same fingerprint

        """
        if not scene.video_id:
            raise ValueError("scene.video_id cannot be None or empty")

        if not scene.fingerprint:
            raise ValueError("scene.fingerprint cannot be None or empty")

        match_id = self.index.find_match(scene.video_id, scene.fingerprint)

        if match_id:
            scene.scene_id = match_id
        else:
            new_id = str(uuid.uuid4())
            self.index.add_scene_fingerprint(scene.video_id, new_id, scene.fingerprint)
            scene.scene_id = new_id

        return scene.scene_id
