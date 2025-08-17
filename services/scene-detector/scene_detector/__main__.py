import json
import tempfile
from pathlib import Path

from confluent_kafka import Consumer
from redis import Redis

from scene_detector.fingerprint import compute_fingerprint
from scene_detector.id_assigner import IdAssigner
from scene_detector.s3 import S3Client
from scene_detector.scenes import detect_scenes
from scene_detector.settings import SceneDetectorSettings
from scene_detector.storage.redis_scene_index import RedisSceneIndex

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


consumer.subscribe([settings.kafka.chunks_topic])


redis_client = Redis(host=settings.redis.host, port=settings.redis.port, decode_responses=True)
index = RedisSceneIndex(redis_client)
assigner = IdAssigner(index)


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

    with tempfile.NamedTemporaryFile(prefix=f"{video_id}-", suffix=".mp4") as temp_file:
        s3_client.download_file(uri, Path(temp_file.name))

        scenes = detect_scenes(temp_file.name)

        print("Downloaded file to", temp_file.name)

        for scene in scenes:
            scene.fingerprint = compute_fingerprint(scene.keyframe)
            scene.video_id = video_id
            assigner.assign(scene)
