from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    host: str
    port: int


class KafkaSettings(BaseSettings):
    group_id: str
    bootstrap_servers: str
    security_protocol: str
    scenes_topic: str
    merged_scenes_topic: str
    auto_offset_reset: str


class SceneManagerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")

    kafka: KafkaSettings
    redis: RedisSettings
