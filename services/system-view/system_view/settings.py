from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    group_id: str = "system-view"
    bootstrap_servers: str = "localhost:9092"
    security_protocol: str = "PLAINTEXT"
    chunks_topic: str = "video-chunks"
    auto_offset_reset: str = "earliest"
    scenes_topic: str = "scenes"
    max_messages_per_topic: int = 100


class SystemViewSettings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")
    kafka: KafkaSettings = KafkaSettings()
