import redis

from .entities import Interval, VideoMetadata


class SceneMerger:
    """
    A Redis-based scene merger that manages video scene intervals and merges overlapping scenes.

    This class provides functionality to:
    - Store and manage video scene intervals in Redis using sorted sets
    - Merge overlapping intervals to create consolidated scene timelines
    - Track scene coverage across entire videos
    - Return complete scene sequences when a video is fully covered

    The class uses Redis sorted sets to maintain intervals sorted by start time,
    enabling efficient merging and retrieval of scene data.

    Attributes:
        redis (redis.Redis): Redis client for data storage and retrieval
        scene_intervals_namespace (str): Namespace prefix for the scene intervals

    """

    def __init__(self, redis_client: redis.Redis):
        """
        Initialize the SceneMerger with Redis client and namespace.

        Args:
            redis_client (redis.Redis): Redis client instance for data operations

        """
        self.redis = redis_client
        self.scene_intervals_namespace = "scene-intervals"

    def _video_interval_key(self, key_type: str, video_id: str) -> str:
        """
        Generate a Redis key for video-related data.

        Args:
            key_type (str): Type of data (e.g., 'duration', 'intervals')
            video_id (str): Unique identifier for the video

        Returns:
            str: Formatted Redis key in the format '{scene_intervals_namespace}:{video_id}:{key_type}'

        """
        return f"{self.scene_intervals_namespace}:{video_id}:{key_type}"

    def _scene_interval_key(self, key_type: str, video_id: str, scene_id: str) -> str:
        """
        Generate a Redis key for scene interval data.

        Args:
            key_type (str): Type of data (e.g., 'intervals')
            video_id (str): Unique identifier for the video
            scene_id (str): Unique identifier for the scene

        Returns:
            str: Formatted Redis key in the format '{scene_intervals_namespace}:{video_id}:{key_type}:{scene_id}'

        """
        return f"{self._video_interval_key(key_type, video_id)}:{scene_id}"

    def _encode_interval(self, start: float, end: float) -> str:
        """
        Encode a time interval as a string for Redis storage.

        Args:
            start (float): Start time of the interval in seconds
            end (float): End time of the interval in seconds

        Returns:
            str: Encoded interval string in format '{start:.6f}:{end:.6f}'

        """
        return f"{start:.6f}:{end:.6f}"

    def _decode_interval(self, encoded: str) -> tuple[float, float]:
        """
        Decode a string interval back to start and end times.

        Args:
            encoded (str): Encoded interval string in format '{start}:{end}'

        Returns:
            tuple[float, float]: Tuple of (start_time, end_time) in seconds

        """
        return tuple(map(float, encoded.split(":")))

    def _get_interval_score(self, start: float, end: float) -> float:
        """
        Get the score for an interval to be used in Redis sorted set.

        The score determines the sort order in Redis. Using start time ensures
        intervals are sorted chronologically.

        Args:
            start (float): Start time of the interval in seconds
            end (float): End time of the interval in seconds (unused but kept for consistency)

        Returns:
            float: Score value (start time) for Redis sorted set

        """
        return start

    def _video_metadata_key(self, video_id: str) -> str:
        """
        Generate a Redis key for video metadata.

        Args:
            key_type (str): Type of data (e.g., 'duration')
            video_id (str): Unique identifier for the video

        Returns:
            str: Formatted Redis key in the format '{video_metadata_namespace}:{video_id}:{key_type}'

        """
        return f"video-metadata:{video_id}"

    def _get_video_metadata(self, video_id: str) -> VideoMetadata | None:
        """
        Get video metadata from Redis.

        Args:
            video_id (str): Unique identifier for the video

        """
        metadata = self.redis.hgetall(self._video_metadata_key(video_id))
        if not metadata:
            return None
        return VideoMetadata.from_dict(metadata)

    def _overlap(self, interval1: tuple[float, float], interval2: tuple[float, float]) -> bool:
        """
        Check if two intervals overlap.

        Two intervals overlap if the end of the first interval is greater than or equal
        to the start of the second interval. This assumes intervals are sorted by start time.

        Args:
            interval1 (tuple[float, float]): First interval as (start, end)
            interval2 (tuple[float, float]): Second interval as (start, end)

        Returns:
            bool: True if intervals overlap, False otherwise

        """
        return interval1[1] >= interval2[0]

    def _merge_intervals(self, intervals: list[tuple[float, float]]) -> list[tuple[float, float]]:
        """
        Merge overlapping intervals into a list of non-overlapping intervals.

        Takes a list of intervals sorted by start time and merges any overlapping
        intervals by extending the end time to cover the maximum range.

        Args:
            intervals (list[tuple[float, float]]): List of intervals as (start, end) tuples,
                                                  must be sorted by start time

        Returns:
            list[tuple[float, float]]: List of merged non-overlapping intervals

        """
        if not intervals:
            return []

        # Intervals are already sorted by start time from Redis
        merged = []
        current_start, current_end = intervals[0]

        for i in range(1, len(intervals)):
            next_start, next_end = intervals[i]
            if self._overlap((current_start, current_end), (next_start, next_end)):
                # Merge by taking the maximum end time
                current_end = max(current_end, next_end)
            else:
                # No overlap, add current interval and start new one
                merged.append((current_start, current_end))
                current_start, current_end = next_start, next_end

        # Add the last interval
        merged.append((current_start, current_end))
        return merged

    def _insert_intervals_for_identifier(
        self, identifier: str, *intervals: tuple[float, float]
    ) -> list[tuple[float, float]]:
        """
        Insert intervals into a Redis sorted set and return merged intervals.

        This method adds new intervals to a Redis sorted set, retrieves all existing
        intervals, merges overlapping ones, and stores the merged result back to Redis.

        Args:
            identifier (str): Redis key identifier for the sorted set
            *intervals (tuple[float, float]): Variable number of intervals to insert

        Returns:
            list[tuple[float, float]]: List of merged non-overlapping intervals

        """
        pipeline = self.redis.pipeline()

        for start, end in intervals:
            incoming_encoded = self._encode_interval(start, end)
            incoming_score = self._get_interval_score(start, end)
            pipeline.zadd(identifier, {incoming_encoded: incoming_score})
        pipeline.zrange(identifier, 0, -1)

        stored_intervals = pipeline.execute()[-1]
        stored_intervals = [self._decode_interval(interval) for interval in stored_intervals]
        merged_intervals = self._merge_intervals(stored_intervals)

        pipeline = self.redis.pipeline()
        pipeline.delete(identifier)

        for start, end in merged_intervals:
            interval_encoded = self._encode_interval(start, end)
            score = self._get_interval_score(start, end)
            pipeline.zadd(identifier, {interval_encoded: score})

        pipeline.execute()

        return merged_intervals

    def insert_scene(self, scene: Interval) -> list[Interval] | None:
        """
        Insert a scene interval and return complete scene sequence if video is fully covered.

        This is the main public method that processes a scene interval. It:
        1. Stores the scene interval in Redis for the specific scene
        2. Merges it with existing video-level intervals
        3. Checks if the video is now fully covered (from 0 to duration)
        4. If fully covered, returns all scenes sorted chronologically

        Args:
            scene (Interval): Scene interval to insert containing scene_id, video_id,
                            duration, start, and end times

        Returns:
            list[Interval] | None: If video is fully covered, returns list of all scenes
                                 sorted by start time. Otherwise returns None.

        """
        video_metadata = self._get_video_metadata(scene.video_id)
        if video_metadata is None:
            raise ValueError(f"Video metadata not found for video_id: {scene.video_id}")

        scene_intervals_key = self._scene_interval_key("intervals", scene.video_id, scene.scene_id)
        video_intervals_key = self._video_interval_key("intervals", scene.video_id)
        scene_intervals = self._insert_intervals_for_identifier(scene_intervals_key, (scene.start, scene.end))
        video_intervals = self._insert_intervals_for_identifier(video_intervals_key, *scene_intervals)
        print("Incoming interval:", scene.start, scene.end)
        print(video_intervals_key, video_intervals)

        if len(video_intervals) == 1 and (
            video_intervals[0][0] == 0 and video_intervals[0][1] >= video_metadata.duration
        ):
            all_scene = self._scene_interval_key("intervals", scene.video_id, "*")
            all_scene_keys = self.redis.keys(all_scene)
            pipe = self.redis.pipeline()
            for key in all_scene_keys:
                pipe.zrange(key, 0, -1)
            all_scene_intervals = pipe.execute()
            all_scene_intervals = [self._decode_interval(interval[0]) for interval in all_scene_intervals]
            scene_ids = [name.rpartition(":")[-1] for name in all_scene_keys]
            sorted_scene_intervals = sorted(
                zip(scene_ids, all_scene_intervals, strict=False), key=lambda entry: entry[1][0]
            )
            return [
                Interval(
                    scene_id=scene_id,
                    video_id=scene.video_id,
                    start=start,
                    end=end,
                )
                for scene_id, (start, end) in sorted_scene_intervals
            ]

        return None
