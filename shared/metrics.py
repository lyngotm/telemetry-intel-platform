"""Shared Prometheus metrics definitions and instrumentation helpers."""

import time
from prometheus_client import (
    Counter,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
    REGISTRY,
)
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


# ─── HTTP Metrics (API Gateway) ──────────────────────────────────

REQUEST_COUNT = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status_code"],
)

REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# ─── Business Metrics (Ingestion Consumer) ───────────────────────

EVENTS_INGESTED = Counter(
    "telemetry_events_ingested_total",
    "Total telemetry events successfully persisted and published to enriched topic",
)

EVENTS_DLQ = Counter(
    "telemetry_events_dlq_total",
    "Total telemetry events routed to dead-letter queue",
    ["reason"],
)

# ─── Business Metrics (Anomaly Detection) ────────────────────────

EVENTS_ANALYZED = Counter(
    "telemetry_events_analyzed_total",
    "Total enriched events processed by anomaly detection",
)

ANOMALIES_DETECTED = Counter(
    "anomalies_detected_total",
    "Total anomalies detected",
    ["severity"],
)

# ─── Business Metrics (API Gateway Kafka) ────────────────────────

KAFKA_PUBLISH_ERRORS = Counter(
    "kafka_publish_errors_total",
    "Total Kafka publish failures from API Gateway",
    ["error_type"],
)

# ─── Business Metrics (Diagnosis Service) ────────────────────────

DIAGNOSES_GENERATED = Counter(
    "diagnoses_generated_total",
    "Total RAG diagnoses successfully generated and persisted",
    ["severity"],
)

DIAGNOSES_FAILED = Counter(
    "diagnoses_failed_total",
    "Total diagnosis generation failures",
    ["phase"],  # context, retrieval, generation, persistence
)

DIAGNOSIS_GENERATION_SECONDS = Histogram(
    "diagnosis_generation_seconds",
    "End-to-end diagnosis pipeline duration in seconds",
    buckets=(1.0, 2.5, 5.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0, 90.0, 120.0),
)

# ─── Prometheus Middleware (API Gateway only) ─────────────────────

class PrometheusMiddleware(BaseHTTPMiddleware):
    """Records request count and latency for all HTTP endpoints."""

    async def dispatch(self, request: Request, call_next):
        # Skip instrumenting the /metrics endpoint itself
        if request.url.path == "/metrics":
            return await call_next(request)

        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start

        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=request.url.path,
            status_code=str(response.status_code),
        ).inc()

        REQUEST_LATENCY.labels(
            method=request.method,
            endpoint=request.url.path,
        ).observe(duration)

        return response


# ─── /metrics response helper ─────────────────────────────────────

def metrics_response() -> Response:
    """Generate Prometheus text-format metrics response."""
    return Response(
        content=generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST,
    )

