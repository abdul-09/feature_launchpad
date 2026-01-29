"""
Events API Router - Endpoints for event ingestion and tracking.

This module provides REST endpoints for:
- Single event ingestion
- Batch event ingestion
- Event validation
- Health checks
"""

from datetime import datetime
from typing import Annotated
import logging

from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import JSONResponse

from app.schemas.events import (
    UserEvent,
    EventBatch,
    EventResponse,
    BatchEventResponse,
    HealthResponse,
)
from app.services.kafka_producer import KafkaProducerService, get_kafka_producer

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/events", tags=["events"])


@router.post(
    "/track",
    response_model=EventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Track a single user event",
    description="Ingests a single user interaction event and publishes it to the event stream."
)
async def track_event(
    event: UserEvent,
    request: Request,
    kafka: Annotated[KafkaProducerService, Depends(get_kafka_producer)]
) -> EventResponse:
    """
    Track a single user event.
    
    The event is validated against the schema and published to Kafka
    for downstream processing.
    
    Args:
        event: The user event to track
        request: FastAPI request object
        kafka: Kafka producer service
        
    Returns:
        EventResponse with event_id for tracking
        
    Raises:
        HTTPException: If event publishing fails
    """
    # Enrich event with server-side data
    event.received_at = datetime.utcnow()
    
    # Extract additional context from request
    if event.context.user_agent is None:
        event.context.user_agent = request.headers.get("user-agent")
    
    # Publish to Kafka
    success = await kafka.publish_event(event)
    
    if not success:
        logger.error(f"Failed to publish event {event.event_id}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event publishing service temporarily unavailable"
        )
    
    logger.info(f"Event tracked: {event.event_type} for user {event.user_id[:8]}...")
    
    return EventResponse(
        success=True,
        event_id=event.event_id,
        message="Event accepted for processing"
    )


@router.post(
    "/track/batch",
    response_model=BatchEventResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Track multiple events in batch",
    description="Ingests multiple events in a single request for efficiency."
)
async def track_events_batch(
    batch: EventBatch,
    request: Request,
    kafka: Annotated[KafkaProducerService, Depends(get_kafka_producer)]
) -> BatchEventResponse:
    """
    Track multiple events in a single batch request.
    
    More efficient for high-volume event tracking as it reduces
    HTTP overhead.
    
    Args:
        batch: Batch of events to track
        request: FastAPI request object
        kafka: Kafka producer service
        
    Returns:
        BatchEventResponse with success/failure counts
    """
    received_at = datetime.utcnow()
    user_agent = request.headers.get("user-agent")
    
    # Enrich all events
    for event in batch.events:
        event.received_at = received_at
        if event.context.user_agent is None:
            event.context.user_agent = user_agent
    
    # Publish batch
    successful, errors = await kafka.publish_batch(batch.events)
    
    event_ids = [e.event_id for e in batch.events[:successful]]
    
    logger.info(f"Batch processed: {successful}/{len(batch.events)} events accepted")
    
    return BatchEventResponse(
        success=successful > 0,
        total_events=len(batch.events),
        accepted_events=successful,
        rejected_events=len(batch.events) - successful,
        event_ids=event_ids,
        errors=errors
    )


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check endpoint",
    description="Returns the health status of the event ingestion service."
)
async def health_check(
    kafka: Annotated[KafkaProducerService, Depends(get_kafka_producer)]
) -> HealthResponse:
    """
    Check the health of the event service.
    
    Verifies connectivity to Kafka and returns service status.
    """
    return HealthResponse(
        status="healthy" if kafka.is_connected else "degraded",
        kafka_connected=kafka.is_connected,
        timestamp=datetime.utcnow()
    )


@router.options("/track")
async def track_options():
    """Handle CORS preflight requests."""
    return JSONResponse(
        content={},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        }
    )
