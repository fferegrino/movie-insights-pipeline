import redis

from scene_detector.fingerprint import fingerprint_distance
from scene_detector.storage.scene_index import SceneIndex


class RedisSceneIndex(SceneIndex):
    """
    Redis-based implementation of scene fingerprint storage and retrieval.

    This class provides a Redis-backed storage solution for scene fingerprints,
    allowing efficient storage, retrieval, and similarity matching of video scene
    fingerprints. It uses Redis hash structures to organize fingerprints by video ID
    and scene ID.

    Attributes:
        redis_client (redis.Redis): The Redis client instance for database operations
        threshold (int): Maximum fingerprint distance for considering a match (default: 10)

    Example:
        >>> import redis
        >>> from scene_detector.storage.redis_scene_index import RedisSceneIndex
        >>>
        >>> # Initialize Redis client
        >>> redis_client = redis.Redis(host='localhost', port=6379, db=0)
        >>>
        >>> # Create scene index with custom threshold
        >>> scene_index = RedisSceneIndex(redis_client, threshold=15)
        >>>
        >>> # Add a scene fingerprint
        >>> scene_index.add_scene_fingerprint("video_123", "scene_1", "fingerprint_data")
        >>>
        >>> # Find matching scenes
        >>> match = scene_index.find_match("video_123", "similar_fingerprint")

    """

    def __init__(self, redis_client: redis.Redis, threshold: int = 10):
        """
        Initialize the RedisSceneIndex.

        Args:
            redis_client (redis.Redis): Redis client instance for database operations
            threshold (int, optional): Maximum fingerprint distance for considering a match.
                                     Lower values are more strict, higher values allow more
                                     variation. Defaults to 10.

        Raises:
            TypeError: If redis_client is not a valid Redis client instance
            ValueError: If threshold is negative

        """
        if not isinstance(redis_client, redis.Redis):
            raise TypeError("redis_client must be a redis.Redis instance")
        if threshold < 0:
            raise ValueError("threshold must be non-negative")

        self.redis_client = redis_client
        self.threshold = threshold

    def _key(self, video_id: str) -> str:
        """
        Generate Redis key for storing scene fingerprints of a specific video.

        Args:
            video_id (str): Unique identifier for the video

        Returns:
            str: Redis key in format "video:{video_id}:scenes"

        Example:
            >>> index = RedisSceneIndex(redis_client)
            >>> index._key("movie_123")
            'video:movie_123:scenes'

        """
        return f"video:{video_id}:scenes"

    def add_scene_fingerprint(self, video_id: str, scene_id: str, fingerprint: str):
        """
        Add a scene fingerprint to the Redis index.

        Stores the fingerprint in a Redis hash where the key is the video ID
        and the field is the scene ID. If a fingerprint already exists for the
        given scene, it will be overwritten.

        Args:
            video_id (str): Unique identifier for the video
            scene_id (str): Unique identifier for the scene within the video
            fingerprint (str): Fingerprint data for the scene

        Example:
            >>> scene_index.add_scene_fingerprint("video_123", "scene_1", "abc123...")

        """
        self.redis_client.hset(self._key(video_id), scene_id, fingerprint)

    def get_scene_fingerprint(self, video_id: str, scene_id: str) -> str:
        """
        Retrieve a scene fingerprint from the Redis index.

        Args:
            video_id (str): Unique identifier for the video
            scene_id (str): Unique identifier for the scene within the video

        Returns:
            str: The fingerprint data for the scene, or None if not found

        Example:
            >>> fingerprint = scene_index.get_scene_fingerprint("video_123", "scene_1")
            >>> if fingerprint:
            ...     print(f"Found fingerprint: {fingerprint}")

        """
        return self.redis_client.hget(self._key(video_id), scene_id)

    def find_match(self, video_id: str, fingerprint: str) -> str:
        """
        Find a matching scene fingerprint within the specified threshold.

        Compares the provided fingerprint against all stored fingerprints for the
        given video using the fingerprint_distance function. Returns the first
        scene ID whose fingerprint distance is less than or equal to the threshold.

        Args:
            video_id (str): Unique identifier for the video to search within
            fingerprint (str): Fingerprint data to match against

        Returns:
            str: Scene ID of the first matching scene, or None if no match found

        Example:
            >>> match_scene = scene_index.find_match("video_123", "query_fingerprint")
            >>> if match_scene:
            ...     print(f"Found matching scene: {match_scene}")
            >>> else:
            ...     print("No matching scene found")

        """
        fingerprints = self.redis_client.hgetall(self._key(video_id))
        for scene_id, stored_fp in fingerprints.items():
            dist = fingerprint_distance(fingerprint, stored_fp)
            if dist <= self.threshold:
                return scene_id
        return None
