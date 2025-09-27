import redis
from confluent_kafka import Consumer, Producer

from scene_manager.processor import SceneProcessor
from scene_manager.scene_merger import SceneMerger
from scene_manager.settings import SceneManagerSettings

settings = SceneManagerSettings()

redis_client = redis.Redis(
    host=settings.redis.host,
    port=settings.redis.port,
    decode_responses=True,
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

scene_merger = SceneMerger(
    redis_client,
)


consumer.subscribe([settings.kafka.scenes_topic])
processor = SceneProcessor(scene_merger, consumer, producer, merged_scenes_topic=settings.kafka.merged_scenes_topic)
processor.run()
