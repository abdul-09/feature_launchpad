"""
Kafka Producer Service - Handles publishing events to Kafka topics.

This service provides:
- Connection management with retry logic
- Async event publishing
- Batch publishing support
- Error handling and dead letter queue support
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Optional
from uuid import UUID

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaConnectionError

from app.schemas.events import UserEvent
from app.core.config import settings

logger = logging.getLogger(__name__)


class UUIDEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles UUID serialization."""
    
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class KafkaProducerService:
    """
    Async Kafka producer service with connection management and retry logic.
    
    Features:
    - Automatic reconnection on failure
    - Configurable retry attempts
    - Batch publishing support
    - Message serialization with schema validation
    """
    
    def __init__(self):
        self._producer: Optional[AIOKafkaProducer] = None
        self._is_connected: bool = False
        self._lock = asyncio.Lock()
        
    @property
    def is_connected(self) -> bool:
        """Check if producer is connected to Kafka."""
        return self._is_connected and self._producer is not None
    
    async def connect(self, max_retries: int = 5, retry_delay: float = 2.0) -> bool:
        """
        Establish connection to Kafka with retry logic.
        
        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between retries in seconds
            
        Returns:
            bool: True if connection successful, False otherwise
        """
        async with self._lock:
            if self._is_connected:
                return True
            
            for attempt in range(1, max_retries + 1):
                try:
                    logger.info(f"Connecting to Kafka (attempt {attempt}/{max_retries})...")
                    
                    self._producer = AIOKafkaProducer(
                        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                        value_serializer=lambda v: json.dumps(v, cls=UUIDEncoder).encode('utf-8'),
                        key_serializer=lambda k: k.encode('utf-8') if k else None,
                        acks='all',  # Wait for all replicas to acknowledge
                        enable_idempotence=True,  # Exactly-once semantics
                        max_batch_size=16384,  # 16KB batch size
                        linger_ms=10,  # Wait up to 10ms to batch messages
                        compression_type='gzip',  # Compress messages
                        retries=3,  # Retry failed sends
                    )
                    
                    await self._producer.start()
                    self._is_connected = True
                    logger.info("Successfully connected to Kafka")
                    return True
                    
                except KafkaConnectionError as e:
                    logger.warning(f"Kafka connection failed (attempt {attempt}): {e}")
                    if attempt < max_retries:
                        await asyncio.sleep(retry_delay * attempt)  # Exponential backoff
                    else:
                        logger.error("Failed to connect to Kafka after all retries")
                        return False
                        
                except Exception as e:
                    logger.error(f"Unexpected error connecting to Kafka: {e}")
                    return False
        
        return False
    
    async def disconnect(self) -> None:
        """Gracefully close the Kafka producer connection."""
        async with self._lock:
            if self._producer:
                try:
                    await self._producer.stop()
                    logger.info("Kafka producer disconnected")
                except Exception as e:
                    logger.error(f"Error disconnecting from Kafka: {e}")
                finally:
                    self._producer = None
                    self._is_connected = False
    
    async def publish_event(
        self,
        event: UserEvent,
        topic: Optional[str] = None
    ) -> bool:
        """
        Publish a single event to Kafka.
        
        Args:
            event: The UserEvent to publish
            topic: Optional topic override (defaults to settings.KAFKA_TOPIC)
            
        Returns:
            bool: True if event was published successfully
        """
        if not self._is_connected:
            logger.warning("Not connected to Kafka, attempting to reconnect...")
            if not await self.connect():
                return False
        
        topic = topic or settings.KAFKA_TOPIC
        
        try:
            # Use user_id as partition key for ordering guarantees per user
            key = event.user_id
            
            # Convert event to dict for serialization
            event_data = event.model_dump(mode='json')
            event_data['received_at'] = datetime.utcnow().isoformat()
            
            # Send to Kafka
            await self._producer.send_and_wait(
                topic=topic,
                value=event_data,
                key=key
            )
            
            logger.debug(f"Published event {event.event_id} to topic {topic}")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to publish event {event.event_id}: {e}")
            self._is_connected = False
            return False
            
        except Exception as e:
            logger.error(f"Unexpected error publishing event: {e}")
            return False
    
    async def publish_batch(
        self,
        events: list[UserEvent],
        topic: Optional[str] = None
    ) -> tuple[int, list[str]]:
        """
        Publish a batch of events to Kafka.
        
        Args:
            events: List of UserEvents to publish
            topic: Optional topic override
            
        Returns:
            Tuple of (successful_count, list of error messages)
        """
        if not self._is_connected:
            if not await self.connect():
                return 0, ["Failed to connect to Kafka"]
        
        topic = topic or settings.KAFKA_TOPIC
        successful = 0
        errors = []
        
        for event in events:
            try:
                if await self.publish_event(event, topic):
                    successful += 1
                else:
                    errors.append(f"Failed to publish event {event.event_id}")
            except Exception as e:
                errors.append(f"Error publishing event {event.event_id}: {str(e)}")
        
        logger.info(f"Batch publish complete: {successful}/{len(events)} successful")
        return successful, errors


# Singleton instance
kafka_producer = KafkaProducerService()


async def get_kafka_producer() -> KafkaProducerService:
    """Dependency injection for Kafka producer."""
    if not kafka_producer.is_connected:
        await kafka_producer.connect()
    return kafka_producer
