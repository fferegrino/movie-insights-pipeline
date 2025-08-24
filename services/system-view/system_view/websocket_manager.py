"""WebSocket manager for real-time communication with clients."""

import asyncio
import logging

from fastapi import WebSocket

from system_view.kafka_connections import KafkaConnections
from system_view.models import WebSocketMessage

logger = logging.getLogger(__name__)


class WebSocketManager:
    """Manages WebSocket connections and broadcasts messages to clients."""

    def __init__(self, kafka_connections: KafkaConnections):
        """Initialize the WebSocket manager."""
        self.active_connections: set[WebSocket] = set()
        self._broadcast_task: asyncio.Task = None
        self.kafka_connections = kafka_connections

    async def connect(self, websocket: WebSocket) -> None:
        """Accept a new WebSocket connection."""
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")

        # Send initial status
        await self.send_status_update(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection."""
        self.active_connections.discard(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")

    async def send_personal_message(self, message: WebSocketMessage, websocket: WebSocket) -> None:
        """Send a message to a specific WebSocket connection."""
        try:
            await websocket.send_text(message.json())
        except Exception as e:
            logger.error(f"Error sending personal message: {e}")
            self.disconnect(websocket)

    async def broadcast(self, message: WebSocketMessage) -> None:
        """Broadcast a message to all connected WebSocket clients."""
        if not self.active_connections:
            return

        disconnected = set()
        for connection in self.active_connections:
            try:
                await connection.send_text(message.json())
            except Exception as e:
                logger.error(f"Error broadcasting message: {e}")
                disconnected.add(connection)

        # Remove disconnected connections
        for connection in disconnected:
            self.disconnect(connection)

    async def send_status_update(self, websocket: WebSocket) -> None:
        """Send current system status to a specific client."""
        try:
            status = await self.kafka_connections.get_system_status()
            message = WebSocketMessage(
                type="status",
                data={
                    "kafka": status,
                },
            )
            await self.send_personal_message(message, websocket)
        except Exception as e:
            logger.error(f"Error sending status update: {e}")

    async def broadcast_status_update(self) -> None:
        """Broadcast system status to all connected clients."""
        try:
            status = await self.kafka_connections.get_system_status()
            message = WebSocketMessage(
                type="status",
                data={
                    "kafka": status,
                },
            )
            await self.broadcast(message)
        except Exception as e:
            logger.error(f"Error broadcasting status update: {e}")

    async def broadcast_error(self, error: str) -> None:
        """Broadcast an error message to all connected clients."""
        try:
            message = WebSocketMessage(type="error", data={"error": error})
            await self.broadcast(message)
        except Exception as e:
            logger.error(f"Error broadcasting error message: {e}")

    async def start_status_broadcast(self) -> None:
        """Start periodic status broadcasting."""
        while True:
            try:
                await asyncio.sleep(5)  # Broadcast every 5 seconds
                await self.broadcast_status_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in status broadcast loop: {e}")

    def start(self) -> None:
        """Start the WebSocket manager."""
        if self._broadcast_task is None or self._broadcast_task.done():
            self._broadcast_task = asyncio.create_task(self.start_status_broadcast())
            logger.info("WebSocket manager started")

    def stop(self) -> None:
        """Stop the WebSocket manager."""
        if self._broadcast_task and not self._broadcast_task.done():
            self._broadcast_task.cancel()
            logger.info("WebSocket manager stopped")
