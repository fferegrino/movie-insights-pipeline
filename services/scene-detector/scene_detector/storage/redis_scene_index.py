import redis

from scene_detector.entities import Scene
from scene_detector.fingerprint import fingerprint_distance
from scene_detector.storage.scene_index import SceneIndex, SceneMatch


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

    def __init__(self, redis_client: redis.Redis, threshold: int = 10, overlap_threshold: float = 0.1):
        """
        Initialize the RedisSceneIndex.

        Args:
            redis_client (redis.Redis): Redis client instance for database operations
            threshold (int, optional): Maximum fingerprint distance for considering a match.
                                     Lower values are more strict, higher values allow more
                                     variation. Defaults to 10.
            overlap_threshold (float, optional): Maximum overlap between scenes in seconds.
                                                Defaults to 0.1.

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
        self.overlap_threshold = overlap_threshold

    def _scenes_key(self, video_id: str) -> str:
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

    def _scene_info_key(self, video_id: str, scene_id: str) -> str:
        """
        Generate Redis key for storing scene info of a specific scene.

        Args:
            video_id (str): Unique identifier for the video
            scene_id (str): Unique identifier for the scene within the video

        Returns:
            str: Redis key in format "video:{video_id}:scene_info:{scene_id}"

        Example:
            >>> index = RedisSceneIndex(redis_client)
            >>> index._scene_info_key("movie_123", "scene_1")
            'video:movie_123:scene_info:scene_1'

        """
        return f"video:{video_id}:scene_info:{scene_id}"

    def add_scene(self, scene: Scene):
        """
        Add a scene fingerprint to the Redis index.

        Stores the fingerprint in a Redis hash where the key is the video ID
        and the field is the scene ID. If a fingerprint already exists for the
        given scene, it will be overwritten.

        Args:
            scene: Scene object to add to the storage backend, it should contain
                all the information to identify the scene, including a `scene_id`
                and a `fingerprint`.

        Example:
            >>> scene_index.add_scene(Scene(video_id="video_123", scene_id="scene_1", fingerprint="abc123..."))

        """
        self.redis_client.hset(self._scenes_key(scene.video_id), scene.scene_id, scene.fingerprint)
        self._insert_scene_info(scene)

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
        return self.redis_client.hget(self._scenes_key(video_id), scene_id)

    def _overlap(self, scene_one: Scene, start_time: float, end_time: float) -> bool:
        """Check if a scene overlaps with another scene."""
        _start_time = start_time - self.overlap_threshold
        _end_time = end_time + self.overlap_threshold
        return (
            _start_time <= scene_one.video_start_time <= _end_time
            or _start_time <= scene_one.video_end_time <= _end_time
        )

    def find_match(self, scene: Scene) -> str:
        """
        Find a matching scene fingerprint within the specified threshold.

        Compares the provided fingerprint against all stored fingerprints for the
        given video using the fingerprint_distance function. Returns the first
        scene ID whose fingerprint distance is less than or equal to the threshold.

        Args:
            scene: Scene object to search for similarity in the index.

        Returns:
            str: Scene ID of the first matching scene, or None if no match found

        Example:
            >>> match_scene = scene_index.find_match(Scene(video_id="video_123", fingerprint="query_fingerprint"))
            >>> if match_scene:
            ...     print(f"Found matching scene: {match_scene}")
            >>> else:
            ...     print("No matching scene found")

        """
        fingerprints = self.redis_client.hgetall(self._scenes_key(scene.video_id))
        for scene_id, stored_fp in fingerprints.items():
            dist = fingerprint_distance(scene.fingerprint, stored_fp)
            if dist <= self.threshold:
                scene_info = self.redis_client.hgetall(self._scene_info_key(scene.video_id, scene_id))
                if self._overlap(scene, float(scene_info["video_start_time"]), float(scene_info["video_end_time"])):
                    return SceneMatch(
                        scene_id=scene_id,
                        video_id=scene.video_id,
                        distance=dist,
                        video_start_time=float(scene_info["video_start_time"]),
                        video_end_time=float(scene_info["video_end_time"]),
                    )
        return None

    def _insert_scene_info(self, scene: Scene):
        """Insert scene info into the Redis index."""
        self.redis_client.hset(
            self._scene_info_key(scene.video_id, scene.scene_id),
            mapping={
                "video_start_time": scene.video_start_time,
                "video_end_time": scene.video_end_time,
            },
        )

    def update_scene(self, scene: Scene):
        """
        Update the info of a scene in the Redis index.

        Args:
            scene: Scene object to update in the index.

        Example:
            >>> scene_index.update_scene(
            ...     Scene(video_id="video_123", scene_id="scene_1", video_start_time=10, video_end_time=20)
            ... )

        """
        self.redis_client.hset(
            self._scene_info_key(scene.video_id, scene.scene_id),
            mapping={
                "video_start_time": scene.video_start_time,
                "video_end_time": scene.video_end_time,
            },
        )
