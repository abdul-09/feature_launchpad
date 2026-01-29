"""
Feature Launchpad - Event Ingestion API

A high-performance event tracking service that collects user interactions
and publishes them to Kafka for real-time and batch processing.

Architecture:
    Frontend SDK → This API → Kafka → Spark Streaming → DuckDB → Dashboard
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
import time

from app.api.events import router as events_router
from app.services.kafka_producer import kafka_producer
from app.core.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO if not settings.DEBUG else logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Prometheus metrics
REQUEST_COUNT = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status']
)
REQUEST_LATENCY = Histogram(
    'http_request_duration_seconds',
    'HTTP request latency',
    ['method', 'endpoint']
)
EVENTS_INGESTED = Counter(
    'events_ingested_total',
    'Total events ingested',
    ['event_type']
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager for startup/shutdown events."""
    # Startup
    logger.info("Starting Feature Launchpad Event API...")
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    logger.info(f"Kafka servers: {settings.KAFKA_BOOTSTRAP_SERVERS}")
    
    # Connect to Kafka
    connected = await kafka_producer.connect()
    if connected:
        logger.info("✓ Kafka connection established")
    else:
        logger.warning("⚠ Kafka connection failed - events will be queued")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Event API...")
    await kafka_producer.disconnect()
    logger.info("✓ Cleanup complete")


# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    description="""
    ## Feature Launchpad Event Ingestion API
    
    High-performance event tracking service for user interaction analytics.
    
    ### Features
    - Real-time event ingestion via REST API
    - Schema validation with Pydantic
    - Kafka integration for stream processing
    - Batch event support for efficiency
    - Prometheus metrics for monitoring
    
    ### Event Types
    - `page_view` - Page/screen views
    - `quiz_started`, `quiz_completed` - Quiz flow events
    - `slider_adjusted`, `option_selected` - Interaction events
    - `result_viewed`, `result_shared` - Outcome events
    """,
    version=settings.APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request timing middleware
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Record request metrics for Prometheus."""
    start_time = time.time()
    
    response = await call_next(request)
    
    # Record metrics
    duration = time.time() - start_time
    endpoint = request.url.path
    method = request.method
    status = response.status_code
    
    REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=status).inc()
    REQUEST_LATENCY.labels(method=method, endpoint=endpoint).observe(duration)
    
    # Add timing header
    response.headers["X-Process-Time"] = str(duration)
    
    return response


# Include routers
app.include_router(events_router, prefix=settings.API_PREFIX)


# Root endpoint
@app.get("/", tags=["root"])
async def root():
    """API root endpoint with service information."""
    return {
        "service": settings.APP_NAME,
        "version": settings.APP_VERSION,
        "environment": settings.ENVIRONMENT,
        "docs": "/docs",
        "health": f"{settings.API_PREFIX}/events/health"
    }


# Prometheus metrics endpoint
@app.get("/metrics", tags=["monitoring"])
async def metrics():
    """Expose Prometheus metrics."""
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions gracefully."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "type": type(exc).__name__
        }
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG
    )
