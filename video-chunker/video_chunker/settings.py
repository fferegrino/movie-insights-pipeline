from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageSettings(BaseSettings):
    raw_video_bucket: str
    chunked_video_bucket: str
    access_key_id: str
    secret_access_key: str
    endpoint_url: str | None = None


class KafkaSettings(BaseSettings):
    bootstrap_servers: str
    security_protocol: str
    chunks_topic: str


class ChunkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")

    storage: StorageSettings
    kafka: KafkaSettings
