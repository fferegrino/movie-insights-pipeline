"""
Scene processor for handling Kafka messages and scene merging.

This module contains the SceneProcessor class that orchestrates the processing
of scene messages from Kafka, merges them using the SceneMerger, and publishes
complete scene sequences back to Kafka.
"""

import json

from confluent_kafka import Consumer, Producer

from scene_manager.entities import Interval
from scene_manager.scene_merger import SceneMerger


class SceneProcessor:
    """
    Processes scene messages from Kafka and manages scene merging workflow.

    This class acts as the main orchestrator for the scene management pipeline.
    It consumes partial scene data from Kafka, processes it through the SceneMerger
    to detect complete video coverage, and publishes merged scene sequences when
    a video is fully processed.

    The processor runs continuously, polling for new messages and handling them
    one at a time. When a video is completely covered by scenes, it publishes
    the complete scene sequence to the output topic.

    Attributes:
        scene_merger (SceneMerger): Instance responsible for merging scene intervals
        consumer (Consumer): Kafka consumer for receiving scene messages
        producer (Producer): Kafka producer for publishing merged scenes
        merged_scenes_topic (str): Kafka topic name for publishing complete scenes

    """

    def __init__(self, scene_merger: SceneMerger, consumer: Consumer, producer: Producer, merged_scenes_topic: str):
        """
        Initialize the SceneProcessor with required dependencies.

        Args:
            scene_merger (SceneMerger): Scene merger instance for processing intervals
            consumer (Consumer): Kafka consumer configured for scene messages
            producer (Producer): Kafka producer for publishing results
            merged_scenes_topic (str): Name of the Kafka topic for complete scenes

        """
        self.scene_merger = scene_merger
        self.consumer = consumer
        self.producer = producer
        self.merged_scenes_topic = merged_scenes_topic

    def run(self) -> None:
        """
        Process partial scene messages and publish complete scene sequences.

        This method runs indefinitely, polling for messages from the Kafka consumer.
        For each valid message received:
        1. Parses the JSON message to extract scene data
        2. Creates an Interval object from the message data
        3. Inserts the scene into the merger
        4. If the video is now fully covered, publishes all scenes to the output topic

        The method handles Kafka errors gracefully by logging them and continuing
        to process subsequent messages.

        Expected message format:
        {
            "scene_id": "unique_scene_identifier",
            "video_id": "unique_video_identifier",
            "video_start_time": 0.0,
            "video_end_time": 10.5
        }

        Published message format:
        {
            "scene_id": "unique_scene_identifier",
            "video_id": "unique_video_identifier",
            "start": 0.0,
            "end": 10.5
        }

        Note:
            This method runs indefinitely and should be called from the main
            application entry point. It will only exit on unhandled exceptions
            or when the process is terminated.

        """
        while True:
            message = self.consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                print(message.error())
                continue

            partial_scene = json.loads(message.value().decode("utf-8"))

            interval = Interval(
                scene_id=partial_scene["scene_id"],
                video_id=partial_scene["video_id"],
                start=partial_scene["video_start_time"],
                end=partial_scene["video_end_time"],
            )
            result = self.scene_merger.insert_scene(interval)
            if result is not None:
                for scene in result:
                    interval_as_dict = {
                        "scene_id": scene.scene_id,
                        "video_id": scene.video_id,
                        "start": scene.start,
                        "end": scene.end,
                    }
                    self.producer.produce(self.merged_scenes_topic, json.dumps(interval_as_dict).encode("utf-8"))
                self.producer.flush()
