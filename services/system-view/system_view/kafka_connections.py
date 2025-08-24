import asyncio
import json
import logging
from datetime import datetime
from typing import Any

from aiokafka import AIOKafkaConsumer

from system_view.models import KafkaMessage, TopicInfo
from system_view.settings import SystemViewSettings

logger = logging.getLogger(__name__)


class KafkaConnections:
    """Kafka client for consuming data from Kafka."""

    def __init__(self, settings: SystemViewSettings):
        """Initialize the Kafka connections."""
        self.kafka_settings = settings.kafka
        self.known_topics = [
            self.kafka_settings.chunks_topic,
            self.kafka_settings.scenes_topic,
        ]
        self.consumers: dict[str, AIOKafkaConsumer] = {}
        self.messages: dict[str, list[KafkaMessage]] = {}
        self.topic_info: dict[str, TopicInfo] = {}
        self.connected = False
        self._consumption_tasks: dict[str, asyncio.Task] = {}
        self._message_callback = None

    async def connect(self) -> bool:
        """Connect to Kafka and initialize consumers for known topics."""
        try:
            for topic in self.known_topics:
                await self._create_consumer(topic)

            self.connected = True
            logger.info("Successfully connected to Kafka")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.connected = False
            return False

    async def _create_consumer(self, topic: str) -> None:
        """Create a consumer for a specific topic."""
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=self.kafka_settings.bootstrap_servers,
                security_protocol=self.kafka_settings.security_protocol,
                group_id=self.kafka_settings.group_id,
                auto_offset_reset=self.kafka_settings.auto_offset_reset,
                enable_auto_commit=True,
                value_deserializer=lambda m: self._deserialize_message(m),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
            )

            await consumer.start()
            self.consumers[topic] = consumer
            self.messages[topic] = []

            # Get topic metadata
            partitions = consumer.assignment()
            self.topic_info[topic] = TopicInfo(
                name=topic, partition_count=len(partitions) if partitions else 0, message_count=0, latest_messages=[]
            )

            # Start consumption task
            task = asyncio.create_task(self._consume_messages(topic))
            self._consumption_tasks[topic] = task

            logger.info(f"Created consumer for topic: {topic}")

        except Exception as e:
            logger.error(f"Failed to create consumer for topic {topic}: {e}")
            raise

    async def _consume_messages(self, topic: str) -> None:
        """Consume messages from a topic."""
        consumer = self.consumers[topic]
        topic_info = self.topic_info[topic]

        try:
            async for message in consumer:
                kafka_message = KafkaMessage(
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                    timestamp=datetime.fromtimestamp(message.timestamp / 1000),
                    key=message.key,
                    value=message.value,
                    headers=dict(message.headers) if message.headers else None,
                )

                # Add message to storage
                self.messages[topic].append(kafka_message)
                topic_info.latest_messages.append(kafka_message)

                # Limit the number of stored messages
                if len(self.messages[topic]) > self.kafka_settings.max_messages_per_topic:
                    self.messages[topic].pop(0)

                if len(topic_info.latest_messages) > self.kafka_settings.max_messages_per_topic:
                    topic_info.latest_messages.pop(0)

                # Update message count
                topic_info.message_count = len(self.messages[topic])

                # Call message callback if set
                if self._message_callback:
                    try:
                        await self._message_callback(topic, kafka_message.dict())
                    except Exception as e:
                        logger.error(f"Error in message callback: {e}")

                logger.debug(f"Received message from {topic}: {kafka_message.offset}")

        except Exception as e:
            logger.error(f"Error consuming messages from {topic}: {e}")
            self.connected = False

    def _deserialize_message(self, message: bytes) -> Any:
        """Deserialize a Kafka message."""
        if not message:
            return None

        try:
            # Try to parse as JSON first
            return json.loads(message.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            # If not JSON, return as string
            try:
                return message.decode("utf-8")
            except UnicodeDecodeError:
                # If not UTF-8, return as base64 or hex
                return message.hex()

    async def get_topics(self) -> list[TopicInfo]:
        """Get information about all monitored topics."""
        return list(self.topic_info.values())

    async def get_messages(self, topic: str, limit: int | None = None) -> list[KafkaMessage]:
        """Get messages for a specific topic."""
        messages = self.messages.get(topic, [])
        if limit:
            return messages[-limit:]
        return messages

    def set_message_callback(self, callback):
        """Set a callback function to be called when new messages are received."""
        self._message_callback = callback

    async def get_system_status(self) -> dict:
        """Get current system status."""
        topics = await self.get_topics()
        return {
            "kafka_connected": self.connected,
            "topics": [topic.dict() for topic in topics],
            "last_updated": datetime.now().isoformat(),
        }

    async def disconnect(self) -> None:
        """Disconnect from Kafka and clean up resources."""
        # Cancel consumption tasks
        for task in self._consumption_tasks.values():
            task.cancel()

        # Stop consumers
        for consumer in self.consumers.values():
            await consumer.stop()

        self.consumers.clear()
        self._consumption_tasks.clear()
        self.connected = False
        logger.info("Disconnected from Kafka")
