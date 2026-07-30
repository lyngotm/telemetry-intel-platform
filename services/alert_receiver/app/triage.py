"""
AI-Assisted Alert Triage — generates contextual triage for critical alerts.

Strategy (lightweight, avoids redundancy with Diagnosis Service):
1. For PRODUCT alerts (anomaly-triggered): Query existing diagnoses from
   PostgreSQL — the Diagnosis Service already ran. Summarize what it found.
2. For PLATFORM alerts (ServiceDown, PipelineStalled, CascadingFailure):
   No diagnosis exists. Invoke LLM fresh with alert context.

This avoids duplicate LLM calls while still providing triage for alerts
that the Diagnosis Service never sees.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

import asyncpg
import boto3

from services.alert_receiver.app.config import settings
from services.alert_receiver.app.database import update_triage_summary
from services.alert_receiver.app.metrics import TRIAGE_INVOCATIONS, TRIAGE_LATENCY
from services.alert_receiver.app.models import AlertmanagerAlert

logger = logging.getLogger("alert_receiver.triage")

# Alerts where the Diagnosis Service has no output — these need fresh LLM triage
PLATFORM_ALERTS = {
    "ServiceDown",
    "IngestionPipelineStalled",
    "DiagnosisPipelineDown",
    "AlertReceiverDown",
    "CascadingFailure",
    "FullPipelineStall",
    "KafkaIngestionFailure",
    "PipelineDegradation",
}

# Alerts triggered by anomaly detection — Diagnosis Service may already have context
PRODUCT_ALERTS = {
    "CriticalAnomalyBurst",
    "DetectionWithoutDiagnosis",
}


async def generate_triage(
    pool: asyncpg.Pool,
    alert_id: str,
    alert: AlertmanagerAlert,
) -> None:
    """
    Generate triage summary for a critical alert.

    Decision tree:
    - Product alert → check for existing diagnoses → summarize them
    - Platform alert → no diagnoses exist → invoke LLM (or fallback)
    - Unknown → try diagnoses first, fall back to LLM/template

    This is called as a background task — errors are logged but don't
    propagate to the webhook response.
    """
    alert_name = alert.labels.get("alertname", "unknown")
    pipeline = alert.labels.get("pipeline", "unknown")
    tier = alert.labels.get("tier", "unknown")

    logger.info(f"Triage: evaluating {alert_name} (tier={tier}, pipeline={pipeline})")
    start_time = time.perf_counter()

    try:
        if alert_name in PRODUCT_ALERTS or tier == "product":
            triage_summary = await _triage_from_existing_diagnoses(pool, alert)

            if not triage_summary:
                triage_summary = await _triage_via_llm_or_fallback(alert)

        elif alert_name in PLATFORM_ALERTS or tier == "platform":
            triage_summary = await _triage_via_llm_or_fallback(alert)

        else:
            triage_summary = await _triage_from_existing_diagnoses(pool, alert)
            if not triage_summary:
                triage_summary = await _triage_via_llm_or_fallback(alert)

        # Persist the triage summary
        await update_triage_summary(pool, alert_id, triage_summary)

        duration = time.perf_counter() - start_time
        TRIAGE_LATENCY.observe(duration)
        TRIAGE_INVOCATIONS.labels(result="success").inc()

        logger.info(f"Triage generated for {alert_name} in {duration:.1f}s (id={alert_id[:8]})")

    except Exception as e:
        duration = time.perf_counter() - start_time
        TRIAGE_INVOCATIONS.labels(result="failure").inc()
        logger.error(
            f"Triage failed for {alert_name} after {duration:.1f}s: {e}",
            exc_info=True,
        )


# ============================================================
# Strategy 1: Summarize existing diagnoses (for product alerts)
# ============================================================


async def _triage_from_existing_diagnoses(
    pool: asyncpg.Pool,
    alert: AlertmanagerAlert,
) -> str | None:
    """
    Query recent diagnoses related to this alert's pipeline/context.
    If found, summarize them into a triage summary without invoking the LLM.

    Returns None if no relevant diagnoses exist.
    """
    lookback = datetime.now(timezone.utc) - timedelta(minutes=30)

    async with pool.acquire() as conn:
        # Find recent critical/high anomaly diagnoses in the last 30 minutes
        rows = await conn.fetch(
            """
            SELECT
                d.root_cause_summary,
                d.confidence_score,
                d.recommended_actions,
                a.metric_type,
                a.severity,
                a.device_id,
                a.detected_at
            FROM diagnoses d
            JOIN anomalies a ON d.anomaly_id = a.anomaly_id
            WHERE a.severity IN ('critical', 'high')
              AND a.detected_at > $1
            ORDER BY a.detected_at DESC
            LIMIT 10
            """,
            lookback,
        )

    if not rows:
        logger.debug("No recent diagnoses found for product alert triage")
        return None

    # Summarize existing diagnoses into triage format
    alert_name = alert.labels.get("alertname", "unknown")
    summary_parts = [
        f"TRIAGE SUMMARY (from {len(rows)} recent diagnosis(es))",
        f"Alert: {alert_name}",
        "Source: Existing Diagnosis Service output (last 30 minutes)",
        "",
        "─── Root Causes Identified ───",
    ]

    seen_causes = set()
    devices_affected = set()
    all_actions = []

    for row in rows:
        cause = row["root_cause_summary"]
        if cause not in seen_causes:
            seen_causes.add(cause)
            confidence = row["confidence_score"]
            summary_parts.append(
                f"• [{row['severity'].upper()}] {cause} (confidence: {confidence:.0%})"
            )

        devices_affected.add(str(row["device_id"]))

        actions = row["recommended_actions"]
        if isinstance(actions, list):
            all_actions.extend(actions)

    summary_parts.append("")
    summary_parts.append("─── Blast Radius ───")
    summary_parts.append(f"• Devices affected: {len(devices_affected)}")
    summary_parts.append(f"• Anomalies in last 30min: {len(rows)}")

    if all_actions:
        # Deduplicate and take top 5
        unique_actions = list(dict.fromkeys(all_actions))[:5]
        summary_parts.append("")
        summary_parts.append("─── Recommended Actions (from diagnoses) ───")
        for i, action in enumerate(unique_actions, 1):
            summary_parts.append(f"{i}. {action}")

    return "\n".join(summary_parts)


# ============================================================
# Strategy 2: Fresh LLM triage (for platform alerts)
# ============================================================


async def _triage_via_llm_or_fallback(alert: AlertmanagerAlert) -> str:
    """
    Generate fresh triage via LLM. Falls back to rule-based template
    if LLM is unavailable or not configured.
    """
    if not settings.claude_model_id:
        logger.debug("No CLAUDE_MODEL_ID — using rule-based triage")
        TRIAGE_INVOCATIONS.labels(result="skipped").inc()
        return _generate_rule_based_triage(alert)

    # Try LLM
    try:
        context = _build_llm_prompt(alert)
        return await _invoke_llm(context)
    except Exception as e:
        logger.warning(f"LLM triage failed, using rule-based fallback: {e}")
        return _generate_rule_based_triage(alert)


def _build_llm_prompt(alert: AlertmanagerAlert) -> str:
    """Build a focused triage prompt for platform alerts."""
    alert_name = alert.labels.get("alertname", "unknown")
    severity = alert.labels.get("severity", "unknown")
    pipeline = alert.labels.get("pipeline", "unknown")
    tier = alert.labels.get("tier", "unknown")
    summary = alert.annotations.get("summary", "No summary")
    description = alert.annotations.get("description", "No description")
    runbook = alert.annotations.get("runbook_url", "N/A")

    return f"""You are an SRE triage assistant for the Telemetry Intelligence Platform.
A critical platform alert has fired. Provide a concise triage summary.

ALERT DETAILS:
- Name: {alert_name}
- Severity: {severity}
- Pipeline: {pipeline}
- Tier: {tier}
- Started: {alert.startsAt}
- Summary: {summary}

DESCRIPTION:
{description}

RUNBOOK: {runbook}

SYSTEM CONTEXT:
- This is a telemetry ingestion + anomaly detection + AI diagnosis platform
- Pipeline: API Gateway → Kafka → Ingestion Consumer → Anomaly Detection → Diagnosis Service
- Data stores: PostgreSQL, Redis (cache/windows), ChromaDB (vectors)

Provide exactly:
1. LIKELY ROOT CAUSE (2-3 sentences, most probable explanation)
2. INVESTIGATION STEPS (3-5 prioritized steps, most impactful first)
3. BLAST RADIUS (what's affected downstream)
4. IMMEDIATE ACTIONS (what to do right now to mitigate)

Be specific to this platform's architecture. No generic advice."""


async def _invoke_llm(prompt: str) -> str:
    """
    Invoke LLM for triage generation.
    Currently support AWS Bedrock Converse API. Can be swapped for OpenAI,
    Anthropic direct, or other providers by modifying this function.
    """
    def _call_llm():
        client = boto3.client("bedrock-runtime", region_name=settings.aws_region)
        response = client.converse(
            modelId=settings.claude_model_id,
            messages=[
                {
                    "role": "user",
                    "content": [{"text": prompt}],
                }
            ],
            inferenceConfig={
                "maxTokens": 1024,
                "temperature": 0.2,
            },
        )
        return response["output"]["message"]["content"][0]["text"]

    return await asyncio.get_event_loop().run_in_executor(None, _call_llm)


# ============================================================
# Fallback: Rule-based triage (no LLM needed)
# ============================================================


def _generate_rule_based_triage(alert: AlertmanagerAlert) -> str:
    """
    Generate a structured triage summary using predefined templates.
    Used when LLM is unavailable or not configured.
    """
    alert_name = alert.labels.get("alertname", "unknown")
    pipeline = alert.labels.get("pipeline", "unknown")
    summary = alert.annotations.get("summary", "No summary available")

    templates = {
        "ServiceDown": {
            "root_cause": (
                f"The {pipeline} service process has stopped responding. "
                "Common causes: OOM kill, unhandled exception, or dependency failure "
                "(PostgreSQL/Redis/Kafka connection timeout)."
            ),
            "investigation": [
                f"Check container/pod status: docker ps | grep {pipeline}",
                "Review service logs for crash stacktrace: docker logs tip-{pipeline}",
                "Check system resources: docker stats (memory, CPU)",
                "Verify dependency connectivity (PostgreSQL, Redis, Kafka)",
            ],
            "blast_radius": (
                f"All downstream services depending on {pipeline} are affected. "
                "If ingestion-consumer: no new telemetry stored. "
                "If anomaly-detection: no anomalies detected. "
                "If diagnosis-service: no root-cause analysis generated."
            ),
            "actions": [
                "Restart the service: docker compose restart {pipeline}",
                "If OOM: increase memory limits in docker-compose.yml",
                "If dependency failure: check upstream services first",
            ],
        },
        "IngestionPipelineStalled": {
            "root_cause": (
                "Zero telemetry events ingested in 5+ minutes. The Kafka consumer "
                "in the ingestion service is likely disconnected or the API gateway "
                "cannot publish to Kafka."
            ),
            "investigation": [
                "Check Kafka broker health: docker exec tip-kafka kafka-broker-api-versions --bootstrap-server localhost:9092",
                "Check consumer group lag: docker exec tip-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group ingestion-consumer-group --describe",
                "Verify telemetry.raw topic exists and has recent offsets",
                "Check API gateway logs for Kafka publish errors",
                "Verify the simulator or producers are actually sending data",
            ],
            "blast_radius": (
                "Complete data pipeline halt. No new telemetry stored in PostgreSQL, "
                "no events flowing to anomaly detection, no diagnoses generated. "
                "Historical data is still queryable but stale."
            ),
            "actions": [
                "Restart ingestion consumer: docker compose restart ingestion-consumer",
                "If Kafka is down: docker compose restart kafka",
                "Check if topic was accidentally deleted: recreate via kafka-init",
            ],
        },
        "CascadingFailure": {
            "root_cause": (
                "A backend service failure is propagating errors to the API gateway. "
                "The root cause is the backend service (ingestion-consumer or anomaly-detection) "
                "being down, which causes the API to fail on dependent operations."
            ),
            "investigation": [
                "Identify which backend service is DOWN (check 'up' metric)",
                "Check if the failure started with a dependency (PostgreSQL, Redis, Kafka)",
                "Review API gateway error logs for the specific failing endpoint",
                "Check if the issue is network-related (container DNS resolution)",
            ],
            "blast_radius": (
                "User-facing API is returning 5xx errors. Read operations may still work "
                "(cache-aside from Redis) but write operations are failing. "
                "Downstream: anomaly detection and diagnosis are also impacted."
            ),
            "actions": [
                "Restart the DOWN backend service first",
                "Monitor API error rate — it should self-recover once backend is up",
                "If persists: restart the entire pipeline in dependency order",
            ],
        },
        "FullPipelineStall": {
            "root_cause": (
                "The API gateway is healthy but zero data is flowing through the pipeline. "
                "Most likely: Kafka is unreachable or all consumer groups are stuck. "
                "The API can accept requests but they dead-end at Kafka publish."
            ),
            "investigation": [
                "Check Kafka container: docker ps | grep kafka",
                "Check Zookeeper: docker ps | grep zookeeper",
                "Verify Kafka publish from API: check kafka_publish_errors_total metric",
                "Check all consumer group states",
            ],
            "blast_radius": (
                "Total pipeline failure. API accepts requests (appears healthy from outside) "
                "but no data reaches storage or detection. Silent failure — most dangerous kind."
            ),
            "actions": [
                "Restart Kafka + Zookeeper: docker compose restart zookeeper kafka",
                "Wait 30s for Kafka to stabilize, then restart consumers",
                "Verify topic recreation if needed: docker compose restart kafka-init",
            ],
        },
        "DiagnosisPipelineDown": {
            "root_cause": (
                "The RAG diagnosis pipeline is failing >90% of the time. "
                "Common causes: LLM API rate limiting, expired AWS credentials, "
                "ChromaDB connection failure, or embedding model unavailability."
            ),
            "investigation": [
                "Check diagnosis service logs: docker logs tip-diagnosis-service",
                "Verify AWS credentials are valid and not expired",
                "Check ChromaDB health: curl http://localhost:8100/api/v1/heartbeat",
                "Check diagnoses_failed_total metric by phase (context/retrieval/generation)",
            ],
            "blast_radius": (
                "Anomalies are being detected but no root-cause analysis is generated. "
                "Operators must triage anomalies manually without AI assistance. "
                "The anomaly detection pipeline itself is unaffected."
            ),
            "actions": [
                "If AWS credentials expired: refresh them and restart diagnosis service",
                "If ChromaDB is down: docker compose restart chromadb",
                "If rate limited: check AWS Bedrock quotas and reduce concurrency",
            ],
        },
        "PipelineDegradation": {
            "root_cause": (
                "Ingestion has dropped >50% AND detection has dropped >80% compared to "
                "15 minutes ago. This indicates a pipeline break between ingestion and "
                "anomaly detection — not simply fewer incoming events."
            ),
            "investigation": [
                "Check telemetry.enriched topic — are events being published?",
                "Check ingestion consumer: is it consuming but failing to enrich?",
                "Verify Redis connectivity (enrichment step uses Redis HGETALL)",
                "Compare consumer group lag on telemetry.raw vs telemetry.enriched",
            ],
            "blast_radius": (
                "Partial data loss. Some events may be ingested but not reaching detection. "
                "Anomaly detection is essentially blind. Diagnoses cannot be generated "
                "for anomalies that are never detected."
            ),
            "actions": [
                "Check Redis health: docker exec tip-redis redis-cli ping",
                "Restart ingestion consumer to reset consumer state",
                "If Redis is down: restart Redis, then restart all consumers",
            ],
        },
        "KafkaIngestionFailure": {
            "root_cause": (
                "Kafka publish errors are occurring simultaneously with ingestion rate drop. "
                "The API gateway cannot write to Kafka, so events are being lost."
            ),
            "investigation": [
                "Check Kafka broker status and disk space",
                "Verify network connectivity between API gateway and Kafka",
                "Check kafka_publish_errors_total breakdown by error_type",
                "Review Kafka broker logs for partition errors",
            ],
            "blast_radius": (
                "Incoming telemetry events are being dropped. The API returns 202 "
                "(accepted) but events never reach storage. Silent data loss."
            ),
            "actions": [
                "Restart Kafka broker if unresponsive",
                "Check and increase disk space if Kafka partitions are full",
                "Restart API gateway to reset Kafka producer connections",
            ],
        },
    }

    template = templates.get(alert_name)

    if template:
        parts = [
            f"TRIAGE: {alert_name}",
            f"Alert: {summary}",
            "",
            "─── Likely Root Cause ───",
            template["root_cause"],
            "",
            "─── Investigation Steps ───",
        ]
        for i, step in enumerate(template["investigation"], 1):
            parts.append(f"{i}. {step}")
        parts.append("")
        parts.append("─── Blast Radius ───")
        parts.append(template["blast_radius"])
        parts.append("")
        parts.append("─── Immediate Actions ───")
        for i, action in enumerate(template["actions"], 1):
            parts.append(f"{i}. {action}")
        return "\n".join(parts)

    # Generic fallback for unknown alerts
    return (
        f"TRIAGE: {alert_name}\n"
        f"Alert: {summary}\n"
        f"Pipeline: {pipeline}\n\n"
        "─── Likely Root Cause ───\n"
        f"Critical condition detected on {pipeline} pipeline. "
        "Review the alert description and annotations for specific context.\n\n"
        "─── Investigation Steps ───\n"
        f"1. Check {pipeline} service logs\n"
        "2. Verify dependency connectivity (PostgreSQL, Redis, Kafka)\n"
        "3. Check related metrics in Grafana dashboards\n"
        "4. Review recent deployments or configuration changes\n\n"
        "─── Blast Radius ───\n"
        "Depends on pipeline scope. Check downstream service health.\n\n"
        "─── Immediate Actions ───\n"
        "1. Follow runbook link in alert annotations\n"
        f"2. Check service status: docker compose ps\n"
        "3. Escalate if not resolved within 15 minutes"
    )
