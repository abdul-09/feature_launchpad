"""
Event Schemas - Pydantic models for event validation and serialization.

This module defines the canonical event schema used throughout the pipeline.
All events flowing through the system must conform to these schemas.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID, uuid4
from enum import Enum
from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    """Enumeration of all valid event types in the system."""
    
    # Page/View Events
    PAGE_VIEW = "page_view"
    FEATURE_VIEW = "feature_view"
    
    # Configurator Events
    QUESTION_VIEWED = "question_viewed"
    SLIDER_ADJUSTED = "slider_adjusted"
    OPTION_SELECTED = "option_selected"
    OPTION_DESELECTED = "option_deselected"
    
    # Quiz Flow Events
    QUIZ_STARTED = "quiz_started"
    QUIZ_STEP_COMPLETED = "quiz_step_completed"
    QUIZ_COMPLETED = "quiz_completed"
    QUIZ_ABANDONED = "quiz_abandoned"
    
    # Result Events
    RESULT_VIEWED = "result_viewed"
    RESULT_SHARED = "result_shared"
    RESULT_SAVED = "result_saved"
    
    # Engagement Events
    CTA_CLICKED = "cta_clicked"
    LINK_CLICKED = "link_clicked"
    SESSION_START = "session_start"
    SESSION_END = "session_end"


class DeviceType(str, Enum):
    """Device type classification."""
    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"
    UNKNOWN = "unknown"


class EventContext(BaseModel):
    """Contextual information about the event environment."""
    
    page_url: Optional[str] = Field(None, description="Full URL where event occurred")
    page_path: Optional[str] = Field(None, description="URL path without domain")
    referrer: Optional[str] = Field(None, description="Referring URL")
    user_agent: Optional[str] = Field(None, description="Browser user agent string")
    device_type: DeviceType = Field(DeviceType.UNKNOWN, description="Device classification")
    screen_width: Optional[int] = Field(None, ge=0, description="Screen width in pixels")
    screen_height: Optional[int] = Field(None, ge=0, description="Screen height in pixels")
    viewport_width: Optional[int] = Field(None, ge=0, description="Viewport width in pixels")
    viewport_height: Optional[int] = Field(None, ge=0, description="Viewport height in pixels")
    timezone: Optional[str] = Field(None, description="User's timezone (e.g., 'America/New_York')")
    locale: Optional[str] = Field(None, description="Browser locale (e.g., 'en-US')")


class EventProperties(BaseModel):
    """
    Flexible event properties container.
    
    Different event types will have different properties:
    - slider_adjusted: question_id, slider_value, previous_value
    - option_selected: question_id, option_id, option_label
    - quiz_completed: total_time_seconds, questions_answered, score
    """
    
    # Question/Step identifiers
    question_id: Optional[int] = Field(None, description="ID of the question/step")
    step_number: Optional[int] = Field(None, ge=1, description="Current step in flow")
    total_steps: Optional[int] = Field(None, ge=1, description="Total steps in flow")
    
    # Value changes
    slider_value: Optional[float] = Field(None, ge=0, le=100, description="Current slider value")
    previous_value: Optional[float] = Field(None, ge=0, le=100, description="Previous slider value")
    
    # Selection data
    option_id: Optional[str] = Field(None, description="Selected option identifier")
    option_label: Optional[str] = Field(None, description="Human-readable option label")
    selected_options: Optional[list[str]] = Field(None, description="List of selected option IDs")
    
    # Result/Completion data
    recommendation_id: Optional[str] = Field(None, description="ID of recommendation shown")
    recommendation_name: Optional[str] = Field(None, description="Name of recommendation")
    match_score: Optional[float] = Field(None, ge=0, le=100, description="Match score percentage")
    
    # Timing
    time_on_step_ms: Optional[int] = Field(None, ge=0, description="Time spent on step in ms")
    total_time_ms: Optional[int] = Field(None, ge=0, description="Total time in flow in ms")
    
    # Engagement
    share_platform: Optional[str] = Field(None, description="Platform shared to")
    cta_id: Optional[str] = Field(None, description="CTA button identifier")
    cta_label: Optional[str] = Field(None, description="CTA button text")
    
    # Catch-all for additional properties
    extra: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional custom properties")


class UserEvent(BaseModel):
    """
    The canonical user event schema.
    
    This is the primary event model that flows through the entire pipeline:
    Frontend SDK → Backend API → Kafka → Spark → DuckDB
    """
    
    event_id: UUID = Field(default_factory=uuid4, description="Unique event identifier")
    user_id: str = Field(..., min_length=1, description="Anonymous or hashed user identifier")
    session_id: UUID = Field(..., description="Session identifier for grouping events")
    event_type: EventType = Field(..., description="Type of event")
    event_properties: EventProperties = Field(default_factory=EventProperties, description="Event-specific properties")
    feature_name: str = Field(default="product_configurator", description="Name of the feature")
    context: EventContext = Field(default_factory=EventContext, description="Event context")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp (UTC)")
    
    # Pipeline metadata (added during processing)
    received_at: Optional[datetime] = Field(None, description="Server receive timestamp")
    processed_at: Optional[datetime] = Field(None, description="Processing timestamp")
    
    @field_validator('user_id')
    @classmethod
    def validate_user_id(cls, v: str) -> str:
        """Ensure user_id is not empty and strip whitespace."""
        v = v.strip()
        if not v:
            raise ValueError('user_id cannot be empty')
        return v

    class Config:
        json_schema_extra = {
            "example": {
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "user_id": "anon_abc123",
                "session_id": "660e8400-e29b-41d4-a716-446655440001",
                "event_type": "slider_adjusted",
                "event_properties": {
                    "question_id": 5,
                    "slider_value": 80,
                    "previous_value": 50,
                    "time_on_step_ms": 3500
                },
                "feature_name": "product_configurator",
                "context": {
                    "page_url": "https://example.com/configurator",
                    "device_type": "desktop",
                    "timezone": "America/New_York"
                },
                "timestamp": "2024-01-15T10:30:00Z"
            }
        }


class EventBatch(BaseModel):
    """Batch of events for bulk ingestion."""
    
    events: list[UserEvent] = Field(..., min_length=1, max_length=1000, description="List of events")
    
    class Config:
        json_schema_extra = {
            "example": {
                "events": [
                    {
                        "user_id": "anon_abc123",
                        "session_id": "660e8400-e29b-41d4-a716-446655440001",
                        "event_type": "quiz_started",
                        "feature_name": "product_configurator"
                    }
                ]
            }
        }


class EventResponse(BaseModel):
    """Response model for event ingestion."""
    
    success: bool = Field(..., description="Whether the event was accepted")
    event_id: UUID = Field(..., description="Event ID for tracking")
    message: str = Field(default="Event received", description="Status message")


class BatchEventResponse(BaseModel):
    """Response model for batch event ingestion."""
    
    success: bool = Field(..., description="Whether the batch was accepted")
    total_events: int = Field(..., description="Total events in batch")
    accepted_events: int = Field(..., description="Number of events accepted")
    rejected_events: int = Field(default=0, description="Number of events rejected")
    event_ids: list[UUID] = Field(default_factory=list, description="IDs of accepted events")
    errors: list[str] = Field(default_factory=list, description="Error messages for rejected events")


class HealthResponse(BaseModel):
    """Health check response."""
    
    status: str = Field(..., description="Service status")
    kafka_connected: bool = Field(..., description="Kafka connection status")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Check timestamp")
    version: str = Field(default="1.0.0", description="API version")
