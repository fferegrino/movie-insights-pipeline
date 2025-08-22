from confluent_kafka import Consumer, Producer
from redis import Redis

from scene_detector.id_assigner import IdAssigner
from scene_detector.main import SceneDetector
from scene_detector.s3 import S3Client
from scene_detector.settings import SceneDetectorSettings
from scene_detector.storage.redis_scene_index import RedisSceneIndex
from scene_detector.telemetry import (
    start_metrics_server,
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


redis_client = Redis(host=settings.redis.host, port=settings.redis.port, decode_responses=True)
index = RedisSceneIndex(redis_client, threshold=400)
assigner = IdAssigner(index)

start_metrics_server()

scene_detector = SceneDetector(
    settings,
    s3=s3_client,
    id_assigner=assigner,
    consumer=consumer,
    producer=producer,
)

scene_detector.run()
