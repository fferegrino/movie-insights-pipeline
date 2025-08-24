from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class WebSocketMessage(BaseModel):
    """Model for WebSocket messages."""

    type: str  # "status", "message", "error"
    data: Any
    timestamp: datetime = Field(default_factory=datetime.now)
