import json
import tempfile
from pathlib import Path

from confluent_kafka import Consumer, Producer

from scene_detector.fingerprint import compute_fingerprint
from scene_detector.id_assigner import IdAssigner
from scene_detector.s3 import S3Client
from scene_detector.scenes import detect_scenes
from scene_detector.settings import SceneDetectorSettings
from scene_detector.telemetry import (
    detected_scenes,
    video_chunks_processed,
)


class SceneDetector:
    """
    A service that processes video chunks to detect scenes and extract keyframes.

    The SceneDetector consumes video chunk messages from Kafka, downloads the video
    files from S3, detects scenes within each chunk, computes fingerprints for
    keyframes, and publishes scene information back to Kafka.

    Attributes:
        settings (SceneDetectorSettings): Configuration settings for the detector
        s3 (S3Client): Client for downloading video files from S3
        id_assigner (IdAssigner): Service for assigning unique IDs to detected scenes
        consumer (Consumer): Kafka consumer for receiving video chunk messages
        producer (Producer): Kafka producer for publishing scene messages

    """

    def __init__(
        self,
        settings: SceneDetectorSettings,
        s3: S3Client,
        id_assigner: IdAssigner,
        consumer: Consumer,
        producer: Producer,
    ):
        """
        Initialize the SceneDetector with required dependencies.

        Args:
            settings: Configuration settings including Kafka topics and other parameters
            s3: S3 client for downloading video files
            id_assigner: Service for assigning unique scene IDs
            consumer: Kafka consumer for receiving video chunk messages
            producer: Kafka producer for publishing scene messages

        """
        self.settings = settings
        self.s3 = s3
        self.id_assigner = id_assigner
        self.consumer = consumer
        self.producer = producer

    def process_chunk(self, video_id: str, chunk_id: str, temp_file_path: str, video_start_time: float) -> list[dict]:
        """
        Process a video chunk to detect scenes and extract keyframes.

        This method analyzes a video file to identify scene boundaries, computes
        fingerprints for keyframes, assigns unique scene IDs, and tracks telemetry
        metrics for detected scenes.

        Args:
            video_id: Unique identifier for the video
            chunk_id: Unique identifier for the video chunk being processed
            temp_file_path: Path to the temporary video file to process
            video_start_time: Start time of the chunk relative to the original video

        Returns:
            List of dictionaries containing scene information including:
            - video_id: The video identifier
            - scene_id: Unique scene identifier
            - frame_start: Starting frame number of the scene
            - frame_end: Ending frame number of the scene
            - start_time: Start time of the scene in seconds
            - end_time: End time of the scene in seconds

        """
        scenes = detect_scenes(temp_file_path, chunk_relative_start_time=video_start_time)

        chunks = []
        for scene in scenes:
            scene.fingerprint = compute_fingerprint(scene.keyframe)
            scene.video_id = video_id
            self.id_assigner.assign(scene)

            detected_scenes.add(1, {"video_id": video_id, "chunk_id": chunk_id, "scene_id": scene.scene_id})

            info = {
                "chunk_id": chunk_id,
                "video_id": video_id,
                "scene_id": scene.scene_id,
                "frame_start": scene.frame_start,
                "frame_end": scene.frame_end,
                "video_start_time": scene.video_start_time,
                "video_end_time": scene.video_end_time,
                "start_time": scene.chunk_start_time,
                "end_time": scene.chunk_end_time,
            }
            chunks.append(info)
        return chunks

    def run(self) -> None:
        """
        Start the main processing loop for the scene detector.

        This method runs indefinitely, continuously:
        1. Polling for video chunk messages from Kafka
        2. Downloading video files from S3 to temporary storage
        3. Processing chunks to detect scenes
        4. Publishing scene information back to Kafka
        5. Tracking telemetry metrics

        The method handles Kafka message errors gracefully and ensures proper
        cleanup of temporary files.
        """
        self.consumer.subscribe([self.settings.kafka.chunks_topic])

        while True:
            message = self.consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                print(message.error())
                continue

            chunk_message_value = json.loads(message.value().decode("utf-8"))

            uri = chunk_message_value["uri"]
            video_id = chunk_message_value["video_id"]
            chunk_id = chunk_message_value["id"]
            chunk_video_start_time = chunk_message_value["start_ts"] - chunk_message_value["overlap_left"]

            video_chunks_processed.add(1, {"video_id": video_id, "chunk_id": chunk_id})

            with tempfile.NamedTemporaryFile(prefix=f"{video_id}-", suffix=".mp4") as temp_file:
                self.s3.download_file(uri, Path(temp_file.name))

                chunks = self.process_chunk(video_id, chunk_id, temp_file.name, video_start_time=chunk_video_start_time)

                for chunk in chunks:
                    self.producer.produce(self.settings.kafka.scenes_topic, json.dumps(chunk).encode("utf-8"))

            self.producer.flush()
