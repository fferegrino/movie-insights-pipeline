from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageSettings(BaseSettings):
    bucket: str
    access_key_id: str
    secret_access_key: str
    endpoint_url: str | None = None


class ChunkerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_nested_delimiter="__")

    storage: StorageSettings
