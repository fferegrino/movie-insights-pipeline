import cv2
from scenedetect import SceneManager, open_video
from scenedetect.detectors import ContentDetector

from scene_detector.entities import Scene


def detect_scenes(video_path, threshold=30.0, min_scene_len=2):
    """
    Detect scenes in a video using PySceneDetect.

    Args:
        video_path (str): Path to video chunk.
        threshold (float): Content detector threshold (lower = more sensitive).
        min_scene_len (int): Minimum scene length in frames.

    Returns:
        List[Scene]: List of Scene objects with start/end times + keyframe.

    """
    # Open video and initialize scene manager
    video = open_video(video_path)
    scene_manager = SceneManager()
    scene_manager.add_detector(ContentDetector(threshold=threshold, min_scene_len=min_scene_len))

    # Perform scene detection
    scene_manager.detect_scenes(video)

    # Get list of scene boundaries (list of (start_time, end_time))
    scene_list = scene_manager.get_scene_list()

    results = []
    cap = cv2.VideoCapture(video_path)

    for start, end in scene_list:
        # Convert to seconds
        start_sec = start.get_seconds()
        end_sec = end.get_seconds()

        # Seek to midpoint frame of the scene to extract keyframe
        mid_sec = (start_sec + end_sec) / 2
        cap.set(cv2.CAP_PROP_POS_MSEC, mid_sec * 1000)
        success, frame = cap.read()

        if success:
            scene = Scene(
                video_id=video_path.split("/")[0],
                frame_start=start.get_frames(),
                frame_end=end.get_frames(),
                start_time=start_sec,
                end_time=end_sec,
                keyframe=frame,
            )
            results.append(scene)

    cap.release()
    return results
