import json

from confluent_kafka import Consumer, Producer

from scene_manager.entities import Interval
from scene_manager.scene_merger import SceneMerger


class SceneProcessor:
    def __init__(self, scene_merger: SceneMerger, consumer: Consumer, producer: Producer):
        self.scene_merger = scene_merger
        self.consumer = consumer
        self.producer = producer

    def run(self):
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
                print(result)
                for scene in result:
                    self.producer.produce(self.producer.merged_scenes_topic, json.dumps(scene).encode("utf-8"))
                self.producer.flush()
