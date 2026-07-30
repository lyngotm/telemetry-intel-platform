"""
Prometheus metrics for the Alert Receiver Service.
Exposed on a separate port (9097) for Prometheus to scrape.
"""

from prometheus_client import Counter, Gauge, Histogram


# --- Alert ingestion metrics ----------------------------------

ALERT_EVENTS_RECEIVED = Counter(
    "alert_events_received_total",
    "Total alert events received from Alertmanager",
    ["status", "severity"],
)

ALERT_WEBHOOK_REQUESTS = Counter(
    "alert_webhook_requests_total",
    "Total webhook POST requests from Alertmanager",
    ["status_code"],
)

ALERT_PROCESSING_ERRORS = Counter(
    "alert_processing_errors_total",
    "Total errors processing alert events",
    ["error_type"],
)

# --- Alert state metrics --------------------------------------

ALERTS_CURRENTLY_FIRING = Gauge(
    "alerts_currently_firing",
    "Number of alerts currently in firing state",
)

ALERTS_FIRING_BY_SEVERITY = Gauge(
    "alerts_firing_by_severity",
    "Currently firing alerts broken down by severity",
    ["severity"],
)

ALERTS_FIRING_BY_PIPELINE = Gauge(
    "alerts_firing_by_pipeline",
    "Currently firing alerts broken down by pipeline",
    ["pipeline"],
)

# --- Webhook delivery metrics ---------------------------------

WEBHOOK_DELIVERY_LATENCY = Histogram(
    "alert_webhook_delivery_seconds",
    "Time from alert startsAt to webhook receipt (delivery latency)",
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120, 300),
)

# --- AI Triage metrics ---------------------------------------

TRIAGE_INVOCATIONS = Counter(
    "alert_triage_invocations_total",
    "Total AI triage invocations attempted",
    ["result"],  # success, failure, skipped
)

TRIAGE_LATENCY = Histogram(
    "alert_triage_duration_seconds",
    "Time to generate AI triage summary",
    buckets=(1, 2.5, 5, 10, 15, 20, 30, 45, 60),
)
