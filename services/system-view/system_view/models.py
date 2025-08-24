from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class KafkaMessage(BaseModel):
    """Model for a Kafka message."""

    topic: str
    partition: int
    offset: int
    timestamp: datetime
    key: str | None = None
    value: Any
    headers: dict[str, str] | None = None


class TopicInfo(BaseModel):
    """Model for topic information."""

    name: str
    partition_count: int
    message_count: int
    latest_messages: list[KafkaMessage] = Field(default_factory=list)


class WebSocketMessage(BaseModel):
    """Model for WebSocket messages."""

    type: str  # "status", "message", "error"
    data: Any
    timestamp: datetime = Field(default_factory=datetime.now)
