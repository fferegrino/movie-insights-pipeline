from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    host: str
    port: int


class StorageSettings(BaseSettings):
    access_key_id: str
    secret_access_key: str
    endpoint_url: str | None = None


class KafkaSettings(BaseSettings):
    group_id: str
    bootstrap_servers: str
    security_protocol: str
    chunks_topic: str
    auto_offset_reset: str
    scenes_topic: str


class SceneDetectorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")

    storage: StorageSettings
    kafka: KafkaSettings
    redis: RedisSettings
