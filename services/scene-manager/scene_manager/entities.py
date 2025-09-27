"""
Data entities for the scene manager service.

This module defines the core data structures used throughout the scene manager
for representing video metadata and scene intervals.
"""

from dataclasses import dataclass


@dataclass
class VideoMetadata:
    """
    Metadata information for a video file.

    This class encapsulates essential video properties that are used by the
    scene manager to track video duration, frame rate, and storage location.

    Attributes:
        duration (float): Total duration of the video in seconds
        video_id (str): Unique identifier for the video
        original_name (str): Original filename of the video
        fps (float): Frames per second of the video
        uri (str): URI or path to the video file location

    """

    duration: float
    video_id: str
    original_name: str
    fps: float
    uri: str

    @classmethod
    def from_dict(cls, data: dict) -> "VideoMetadata":
        """
        Create a VideoMetadata instance from a dictionary.

        This class method provides a convenient way to construct VideoMetadata
        objects from dictionary data, typically loaded from Redis or JSON.

        Args:
            data (dict): Dictionary containing video metadata with keys:
                - duration: Video duration (will be converted to float)
                - video_id: Unique video identifier
                - original_name: Original filename
                - fps: Frames per second (will be converted to float)
                - uri: Video file URI/path

        Returns:
            VideoMetadata: New instance with data from the dictionary

        Raises:
            KeyError: If required keys are missing from the data dictionary
            ValueError: If duration or fps cannot be converted to float

        """
        return cls(
            duration=float(data["duration"]),
            video_id=data["video_id"],
            original_name=data["original_name"],
            fps=float(data["fps"]),
            uri=data["uri"],
        )


@dataclass
class Interval:
    """
    Represents a time interval for a scene within a video.

    This class defines a time segment (start to end) for a specific scene
    within a video. It's used throughout the scene manager to track scene
    boundaries and merge overlapping intervals.

    Attributes:
        scene_id (str): Unique identifier for the scene
        video_id (str): Unique identifier for the video containing this scene
        start (float): Start time of the interval in seconds
        end (float): End time of the interval in seconds

    Note:
        The start and end times are relative to the beginning of the video.
        The end time should be greater than the start time for valid intervals.

    """

    scene_id: str
    video_id: str
    start: float
    end: float
