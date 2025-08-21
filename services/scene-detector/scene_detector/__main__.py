import json
import tempfile
import time
from pathlib import Path

from confluent_kafka import Consumer, Producer
from redis import Redis

from scene_detector.fingerprint import compute_fingerprint
from scene_detector.id_assigner import IdAssigner
from scene_detector.s3 import S3Client
from scene_detector.scenes import detect_scenes
from scene_detector.settings import SceneDetectorSettings
from scene_detector.storage.redis_scene_index import RedisSceneIndex
from scene_detector.telemetry import (
    detected_scenes,
    start_metrics_server,
    video_chunks_processed,
    video_chunks_processed_duration,
)

settings = SceneDetectorSettings()

s3_client = S3Client(
    endpoint_url=settings.storage.endpoint_url,
    aws_access_key_id=settings.storage.access_key_id,
    aws_secret_access_key=settings.storage.secret_access_key,
)


consumer = Consumer(
    {
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "group.id": settings.kafka.group_id,
        "security.protocol": settings.kafka.security_protocol,
        "auto.offset.reset": settings.kafka.auto_offset_reset,
    }
)


producer = Producer(
    {
        "bootstrap.servers": settings.kafka.bootstrap_servers,
        "security.protocol": settings.kafka.security_protocol,
    }
)

consumer.subscribe([settings.kafka.chunks_topic])

redis_client = Redis(host=settings.redis.host, port=settings.redis.port, decode_responses=True)
index = RedisSceneIndex(redis_client)
assigner = IdAssigner(index)

start_metrics_server()

while True:
    message = consumer.poll(timeout=1.0)
    if message is None:
        continue
    if message.error():
        print(message.error())
        continue
    message_value = json.loads(message.value().decode("utf-8"))

    uri = message_value["uri"]
    video_id = message_value["video_id"]

    video_chunks_processed.add(1, {"video_id": video_id})

    start_time = time.time()

    with tempfile.NamedTemporaryFile(prefix=f"{video_id}-", suffix=".mp4") as temp_file:
        s3_client.download_file(uri, Path(temp_file.name))

        scenes = detect_scenes(temp_file.name)

        print("Downloaded file to", temp_file.name)

        for scene in scenes:
            scene.fingerprint = compute_fingerprint(scene.keyframe)
            scene.video_id = video_id
            assigner.assign(scene)

            detected_scenes.add(1, {"video_id": video_id})
            print(f"Producing scene {scene.scene_id} for video {video_id} to topic {settings.kafka.scenes_topic}")

            producer.produce(
                settings.kafka.scenes_topic,
                json.dumps(
                    {
                        "video_id": video_id,
                        "scene_id": scene.scene_id,
                        "frame_start": scene.frame_start,
                        "frame_end": scene.frame_end,
                        "start_time": scene.start_time,
                        "end_time": scene.end_time,
                    }
                ).encode("utf-8"),
            )

        video_chunks_processed_duration.record(time.time() - start_time)

        producer.flush()
