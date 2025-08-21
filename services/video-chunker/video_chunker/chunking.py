"""
Video chunking utilities for processing large video files.

This module provides functionality to split large video files into smaller chunks
with configurable overlap. This is useful for processing videos that are too large
to process as a single unit, or for parallel processing of video segments.

The chunking process preserves video quality and includes overlap regions to ensure
smooth transitions between chunks and maintain context for analysis.
"""

import tempfile
from collections.abc import Generator
from dataclasses import dataclass

from moviepy import VideoFileClip


@dataclass
class ChunkBoundary:
    """
    Represents the temporal boundaries of a video chunk.

    This class defines the start and end timestamps of a chunk, along with
    overlap regions on both sides. The overlap regions are used to maintain
    context and ensure smooth transitions between chunks.

    Attributes:
        start_ts: The start timestamp of the chunk (in seconds)
        end_ts: The end timestamp of the chunk (in seconds)
        frame_start: The start frame of the chunk relative to the original video (in frames)
        frame_end: The end frame of the chunk relative to the original video (in frames)
        overlap_left: Overlap duration extending before the start timestamp
        overlap_right: Overlap duration extending after the end timestamp

    Properties:
        left: The actual start time including left overlap
        right: The actual end time including right overlap

    """

    start_ts: float
    end_ts: float
    frame_start: int
    frame_end: int
    overlap_left: float
    overlap_right: float

    @property
    def left(self) -> float:
        """
        Get the actual start time including left overlap.

        Returns:
            The start time minus the left overlap duration

        """
        return self.start_ts - self.overlap_left

    @property
    def right(self) -> float:
        """
        Get the actual end time including right overlap.

        Returns:
            The end time plus the right overlap duration

        """
        return self.end_ts + self.overlap_right


def calculate_chunk_boundaries(
    duration: float, chunk_duration: float, overlap: float, fps: float
) -> list[ChunkBoundary]:
    """
    Calculate the boundaries for video chunks based on duration and overlap settings.

    This function divides a video into chunks of specified duration, with optional
    overlap between chunks. The overlap helps maintain context and ensures smooth
    transitions between chunks.

    Args:
        duration: Total duration of the video in seconds
        chunk_duration: Target duration for each chunk in seconds
        overlap: Overlap duration between consecutive chunks in seconds
        fps: Frame rate of the video

    Returns:
        List of ChunkBoundary objects defining each chunk's temporal boundaries

    Raises:
        ValueError: If any of the input parameters are invalid

    Example:
        >>> boundaries = calculate_chunk_boundaries(100.0, 30.0, 5.0)
        >>> len(boundaries)
        4
        >>> boundaries[0].start_ts
        0.0
        >>> boundaries[0].end_ts
        30.0
        >>> boundaries[0].overlap_right
        5.0

    """
    # Validate inputs
    if duration < 0:
        raise ValueError("Duration must be non-negative")
    if chunk_duration <= 0:
        raise ValueError("Chunk duration must be positive")
    if overlap < 0:
        raise ValueError("Overlap must be non-negative")
    if overlap >= chunk_duration:
        raise ValueError("Overlap must be less than chunk duration")

    num_chunks = int(duration // chunk_duration) + (1 if duration % chunk_duration > 0 else 0)

    chunks = []
    for i in range(num_chunks):
        start_time = i * chunk_duration
        end_time = min((i + 1) * chunk_duration, duration)

        overlap_left = 0.0
        overlap_right = 0.0

        if i != 0:
            overlap_left = overlap

        if i != num_chunks - 1:
            overlap_right = overlap

        frame_start = int(start_time * fps)
        frame_end = int(end_time * fps)

        chunks.append(ChunkBoundary(start_time, end_time, frame_start, frame_end, overlap_left, overlap_right))

    return chunks


def chunk_video(
    full_video: VideoFileClip, chunk_duration: float, overlap: float
) -> Generator[tuple[str, dict], None, None]:
    """
    Split a video into chunks with specified duration and overlap.

    This function processes a video file and yields chunks as temporary files
    along with their metadata. Each chunk is written to a temporary directory
    and includes overlap regions to maintain context between chunks.

    The function uses MoviePy for video processing and automatically handles
    video codec settings for optimal quality and compatibility.

    Args:
        full_video: The MoviePy VideoFileClip object to chunk
        chunk_duration: Target duration for each chunk in seconds
        overlap: Overlap duration between consecutive chunks in seconds

    Yields:
        Tuples containing:
        - chunk_path: Path to the temporary chunk file
        - metadata: Dictionary containing chunk metadata including:
            - start_ts: Start timestamp of the chunk
            - end_ts: End timestamp of the chunk
            - overlap_left: Left overlap duration
            - overlap_right: Right overlap duration
            - chunk_count: Total number of chunks
            - chunk_idx: Index of this chunk (1-based)
            - fps: Video frame rate
            - settings: Video encoding settings used

    Note:
        The temporary files are automatically cleaned up when the generator
        is exhausted or when the temporary directory context exits.

    Example:
        >>> from moviepy import VideoFileClip
        >>> video = VideoFileClip("large_video.mp4")
        >>> for chunk_path, metadata in chunk_video(video, 30.0, 5.0):
        ...     print(f"Chunk {metadata['chunk_idx']}: {chunk_path}")
        ...     # Process the chunk...
        ...     # The file will be automatically cleaned up

    """
    duration = full_video.duration
    fps = full_video.fps
    chunks = calculate_chunk_boundaries(duration, chunk_duration, overlap, fps)
    num_chunks = len(chunks)
    chunk_settings = {
        "codec": "libx264",
        "audio_codec": "aac",
    }
    _chunk_pointers = []
    with tempfile.TemporaryDirectory() as temp_dir:
        for chunk_index, chunk_boundary in enumerate(chunks, start=1):
            chunk_clip = full_video.subclipped(chunk_boundary.left, chunk_boundary.right)
            chunk_filename = f"chunk_{chunk_index:06d}_{num_chunks:06d}.mp4"
            chunk_path = f"{temp_dir}/{chunk_filename}"
            chunk_clip.write_videofile(chunk_path, **chunk_settings)

            metadata = {
                "start_ts": chunk_boundary.start_ts,
                "end_ts": chunk_boundary.end_ts,
                "overlap_left": chunk_boundary.overlap_left,
                "overlap_right": chunk_boundary.overlap_right,
                "frame_start": chunk_boundary.frame_start,
                "frame_end": chunk_boundary.frame_end,
                "chunk_count": num_chunks,
                "chunk_idx": chunk_index,
                "fps": fps,
                "settings": chunk_settings,
            }
            yield chunk_path, metadata
            _chunk_pointers.append(chunk_clip)

        for chunk_pointer in _chunk_pointers:
            chunk_pointer.close()
