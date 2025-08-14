import tempfile
from collections.abc import Generator

from moviepy import VideoFileClip


def calculate_chunk_boundaries(duration: float, chunk_duration: float, overlap: float) -> list[tuple[float, float]]:
    """
    Calculate time boundaries for video chunks with overlap.

    This function divides a video duration into chunks of specified length, with optional
    overlap between consecutive chunks. The overlap helps ensure smooth transitions
    and prevents cutting important content at chunk boundaries.

    Args:
        duration (float): Total duration of the video in seconds.
        chunk_duration (float): Duration of each chunk in seconds (excluding overlap).
        overlap (float): Overlap duration in seconds between consecutive chunks.

    Returns:
        list[tuple[float, float]]: List of (start_time, end_time) tuples for each chunk.
            Each tuple contains the start and end times in seconds for that chunk.
            The end_time includes the overlap with the next chunk (except for the last chunk).

    Examples:
        >>> calculate_chunk_boundaries(10.0, 3.0, 0.5)
        [(0.0, 3.5), (2.5, 6.5), (5.5, 9.5), (8.5, 10.0)]

        >>> calculate_chunk_boundaries(5.0, 2.0, 0.0)
        [(0.0, 2.0), (2.0, 4.0), (4.0, 5.0)]

        >>> calculate_chunk_boundaries(3.0, 5.0, 1.0)
        [(0.0, 3.0)]

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

        if i != 0:
            start_time = start_time - overlap

        if i != num_chunks - 1:
            end_time = end_time + overlap

        chunks.append((start_time, end_time))

    return chunks


def chunk_video(video_path: str, chunk_duration: float, overlap: float) -> Generator[dict, None, None]:
    """
    Chunk a video into smaller pieces.

    Note that the chunk file is deleted after the generator is exhausted.

    Args:
        video_path: The path to the video file.
        chunk_duration: The duration of each chunk in seconds.
        overlap: The overlap between chunks in seconds.

    Returns:
        A generator of dictionaries, each containing the metadata for a chunk.

        The metadata contains the following keys:
        - start_time: The start time of the chunk in seconds.
        - end_time: The end time of the chunk in seconds.
        - fps: The frames per second of the video.
        - local_uri: The path to the chunk file.
        - settings: The settings used to create the chunk.

    """
    with VideoFileClip(video_path) as clip:
        duration = clip.duration
        fps = clip.fps
        chunks = calculate_chunk_boundaries(duration, chunk_duration, overlap)
        num_chunks = len(chunks)
        chunk_settings = {
            "codec": "libx264",
            "audio_codec": "aac",
        }
        _chunk_pointers = []
        with tempfile.TemporaryDirectory() as temp_dir:
            for chunk_index, (start_time, end_time) in enumerate(chunks, start=1):
                chunk_clip = clip.subclipped(start_time, end_time)
                chunk_filename = f"chunk_{chunk_index:06d}_{num_chunks:06d}.mp4"
                chunk_path = f"{temp_dir}/{chunk_filename}"
                chunk_clip.write_videofile(chunk_path, **chunk_settings)

                metadata = {
                    "start_time": start_time,
                    "end_time": end_time,
                    "fps": fps,
                    "local_uri": chunk_path,
                    "settings": chunk_settings,
                }
                yield metadata
                _chunk_pointers.append(chunk_clip)

            for chunk_pointer in _chunk_pointers:
                chunk_pointer.close()
