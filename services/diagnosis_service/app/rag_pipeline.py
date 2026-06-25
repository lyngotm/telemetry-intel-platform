"""
RAG Pipeline for the Diagnosis Service.

Three-phase pipeline:
1. Context Construction — gather recent telemetry + device metadata
2. Retrieval — embed context, query ChromaDB for similar historical incidents
3. Generation — build prompt, call Claude Haiku 4.5, parse structured response

All synchronous I/O (boto3, ChromaDB HttpClient) is wrapped in asyncio.to_thread()
to avoid blocking the event loop.
"""

import asyncio
import json
import logging
import time
from datetime import timedelta

import asyncpg
import boto3
import chromadb
import redis.asyncio as aioredis

from services.diagnosis_service.app.config import settings
from services.diagnosis_service.app.prompt import (
    SYSTEM_PROMPT,
    USER_PROMPT_TEMPLATE,
    format_recent_telemetry,
    format_retrieved_incidents,
)
from shared.models.models import AnomalyDetectedMessage

logger = logging.getLogger("diagnosis_service")


# ─── Phase 1: Context Construction ────────────────────────────────


async def build_context(
    anomaly: AnomalyDetectedMessage,
    db_pool: asyncpg.Pool,
    redis_client: aioredis.Redis,
) -> dict:
    """
    Gather contextual information for the anomaly:
    - Recent telemetry history from PostgreSQL (last N minutes for device)
    - Device metadata from Redis (with PostgreSQL fallback)

    Returns a dict with 'recent_events', 'device_metadata', ready for prompt construction.
    """
    # --- Recent telemetry from PostgreSQL ---
    cutoff = anomaly.detected_at - timedelta(minutes=settings.context_window_minutes)

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT metric_type, value, timestamp
            FROM telemetry_events
            WHERE device_id = $1 AND timestamp >= $2
            ORDER BY timestamp ASC
            """,
            anomaly.device_id,
            cutoff,
        )

    recent_events = [
        {
            "metric_type": row["metric_type"],
            "value": float(row["value"]),
            "timestamp": row["timestamp"].isoformat(),
        }
        for row in rows
    ]

    # --- Device metadata from Redis (cache-aside pattern) ---
    device_metadata = {}
    try:
        cached = await redis_client.hgetall(f"device:{anomaly.device_id}")
        if cached:
            device_metadata = cached
        else:
            # Fallback to PostgreSQL
            async with db_pool.acquire() as conn:
                device_row = await conn.fetchrow(
                    "SELECT device_type, location, firmware_version FROM devices WHERE device_id = $1",
                    anomaly.device_id,
                )
                if device_row:
                    device_metadata = {
                        "device_type": device_row["device_type"],
                        "location": device_row["location"] or "unknown",
                        "firmware_version": device_row["firmware_version"] or "unknown",
                    }
    except Exception as e:
        logger.warning(f"Failed to fetch device metadata: {e}")

    logger.info(
        f"Context built: {len(recent_events)} recent events, "
        f"device_metadata={'populated' if device_metadata else 'empty'}"
    )

    return {
        "recent_events": recent_events,
        "device_metadata": device_metadata,
    }


# ─── Phase 2: Retrieval ──────────────────────────────────────────


def _generate_query_embedding(text: str) -> list[float]:
    """
    Generate embedding for a query string using Cohere Embed v4 via Bedrock.
    Uses input_type="search_query" for optimal retrieval (asymmetric search).
    """
    client = boto3.client("bedrock-runtime", region_name=settings.aws_region)

    response = client.invoke_model(
        modelId=settings.bedrock_embedding_model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({
            "texts": [text],
            "input_type": "search_query",
        }),
    )

    response_body = json.loads(response["body"].read())
    return response_body["embeddings"]["float"][0]


def _query_chromadb(
    query_embedding: list[float],
    chroma_client: chromadb.ClientAPI,
    top_k: int,
) -> list[dict]:
    """
    Query ChromaDB for the most similar incident chunks.
    Returns a list of dicts with 'document', 'metadata', 'distance' keys.
    """
    collection = chroma_client.get_or_create_collection(
        name="incident_embeddings",
        metadata={"hnsw:space": "cosine"},
    )

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
        include=["documents", "metadatas", "distances"],
    )

    # Flatten ChromaDB's nested list structure
    chunks = []
    if results["documents"] and results["documents"][0]:
        for i in range(len(results["documents"][0])):
            chunks.append({
                "document": results["documents"][0][i],
                "metadata": results["metadatas"][0][i],
                "distance": results["distances"][0][i],
            })

    return chunks


async def retrieve_similar_incidents(
    anomaly: AnomalyDetectedMessage,
    context: dict,
    chroma_client: chromadb.ClientAPI,
) -> list[dict]:
    """
    Phase 2: Embed the anomaly context and retrieve similar incidents from ChromaDB.

    Constructs a query string from anomaly details + device context,
    generates an embedding, and queries for top-k similar chunks.
    """
    # Build query text combining anomaly details with device context
    device_type = context["device_metadata"].get("device_type", anomaly.device_type or "unknown")
    location = context["device_metadata"].get("location", anomaly.device_location or "unknown")

    query_text = (
        f"Anomaly detected: {anomaly.metric_type} reading of {anomaly.observed_value} "
        f"with z-score {anomaly.z_score:.2f} ({anomaly.severity} severity) "
        f"on device type '{device_type}' at location '{location}'. "
        f"Expected range: mean {anomaly.expected_range.get('mean', 'unknown')} "
        f"with stddev {anomaly.expected_range.get('stddev', 'unknown')}."
    )

    logger.info(f"Retrieval query: {query_text[:100]}...")

    # Embedding + ChromaDB query are synchronous — run in thread pool
    def _sync_retrieve():
        embedding = _generate_query_embedding(query_text)
        chunks = _query_chromadb(embedding, chroma_client, settings.retrieval_top_k)
        return chunks

    chunks = await asyncio.to_thread(_sync_retrieve)

    distances = [f"{c['distance']:.3f}" for c in chunks]
    logger.info(
        f"Retrieved {len(chunks)} chunks from ChromaDB (distances: {distances})"
    )

    return chunks


# ─── Phase 3: Generation ──────────────────────────────────────────


def _call_llm(system_prompt: str, user_prompt: str) -> str:
    """
    Call Claude Haiku 4.5 via Bedrock Converse API.
    Returns the raw text response.

    Uses the Converse API (not InvokeModel) because it handles inference
    profile ARNs cleanly and provides a unified interface across model providers.
    """
    client = boto3.client("bedrock-runtime", region_name=settings.aws_region)

    response = client.converse(
        modelId=settings.bedrock_model_id,
        messages=[
            {
                "role": "user",
                "content": [{"text": user_prompt}],
            }
        ],
        system=[{"text": system_prompt}],
        inferenceConfig={
            "maxTokens": 1024,
            "temperature": 0.2,  # Low temperature for consistent, factual output
        },
    )

    return response["output"]["message"]["content"][0]["text"]


def _parse_diagnosis_response(raw_response: str) -> dict:
    """
    Parse the LLM's JSON response into a structured dict.
    Handles cases where the model wraps JSON in markdown code fences.
    """
    text = raw_response.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        # Remove first line (```json or ```) and last line (```)
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]).strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response as JSON: {e}\nRaw: {text[:500]}")
        # Return a fallback diagnosis
        parsed = {
            "root_cause_summary": f"Unable to parse structured diagnosis. Raw response: {text[:200]}",
            "confidence_score": 0.1,
            "supporting_evidence": ["LLM response parsing failed"],
            "recommended_actions": ["Review raw LLM response manually", "Check prompt template formatting"],
            "retrieved_incident_ids": [],
        }

    # Validate required fields exist
    required_fields = {"root_cause_summary", "confidence_score", "supporting_evidence", "recommended_actions"}
    for field in required_fields:
        if field not in parsed:
            parsed[field] = [] if field in ("supporting_evidence", "recommended_actions") else ""

    # Clamp confidence score
    if isinstance(parsed.get("confidence_score"), (int, float)):
        parsed["confidence_score"] = max(0.0, min(1.0, float(parsed["confidence_score"])))
    else:
        parsed["confidence_score"] = 0.1

    # Ensure retrieved_incident_ids exists
    if "retrieved_incident_ids" not in parsed:
        parsed["retrieved_incident_ids"] = []

    return parsed


async def generate_diagnosis(
    anomaly: AnomalyDetectedMessage,
    context: dict,
    retrieved_chunks: list[dict],
) -> dict:
    """
    Phase 3: Build the prompt, call Claude, and parse the structured response.

    Returns a dict with: root_cause_summary, confidence_score,
    supporting_evidence, recommended_actions, retrieved_incident_ids,
    raw_llm_response, generation_time_seconds.
    """
    # Build the user prompt from template
    device_type = context["device_metadata"].get("device_type", anomaly.device_type or "unknown")
    location = context["device_metadata"].get("location", anomaly.device_location or "unknown")

    user_prompt = USER_PROMPT_TEMPLATE.format(
        anomaly_id=anomaly.anomaly_id,
        device_id=anomaly.device_id,
        device_type=device_type,
        device_location=location,
        metric_type=anomaly.metric_type,
        observed_value=anomaly.observed_value,
        z_score=f"{anomaly.z_score:.2f}",
        severity=anomaly.severity,
        detected_at=anomaly.detected_at.isoformat(),
        expected_mean=anomaly.expected_range.get("mean", "unknown"),
        expected_stddev=anomaly.expected_range.get("stddev", "unknown"),
        expected_lower=anomaly.expected_range.get("lower", "unknown"),
        expected_upper=anomaly.expected_range.get("upper", "unknown"),
        window_count=anomaly.expected_range.get("window_count", "unknown"),
        context_window_minutes=settings.context_window_minutes,
        recent_telemetry=format_recent_telemetry(context["recent_events"]),
        retrieval_k=settings.retrieval_top_k,
        retrieved_incidents=format_retrieved_incidents(retrieved_chunks),
    )

    logger.info(f"Calling LLM (prompt length: {len(user_prompt)} chars)")

    # LLM call is synchronous (boto3) — run in thread pool
    start_time = time.time()
    raw_response = await asyncio.to_thread(_call_llm, SYSTEM_PROMPT, user_prompt)
    generation_time = time.time() - start_time

    logger.info(f"LLM response received in {generation_time:.2f}s ({len(raw_response)} chars)")

    # Parse structured response
    parsed = _parse_diagnosis_response(raw_response)

    # Add metadata
    parsed["raw_llm_response"] = raw_response
    parsed["generation_time_seconds"] = round(generation_time, 3)

    return parsed


# ─── Full Pipeline Orchestrator ───────────────────────────────────


async def run_diagnosis_pipeline(
    anomaly: AnomalyDetectedMessage,
    db_pool: asyncpg.Pool,
    redis_client: aioredis.Redis,
    chroma_client: chromadb.ClientAPI,
) -> dict:
    """
    Execute the full RAG diagnosis pipeline for a single anomaly.

    Phases:
        1. Build context (recent telemetry + device metadata)
        2. Retrieve similar historical incidents from ChromaDB
        3. Generate diagnosis via Claude Haiku 4.5

    Returns a dict ready to be persisted to the diagnoses table.
    """
    logger.info(
        f"Starting diagnosis pipeline for anomaly {anomaly.anomaly_id} "
        f"(device={anomaly.device_id}, metric={anomaly.metric_type}, "
        f"severity={anomaly.severity})"
    )

    # Phase 1: Context
    context = await build_context(anomaly, db_pool, redis_client)

    # Phase 2: Retrieval
    retrieved_chunks = await retrieve_similar_incidents(anomaly, context, chroma_client)

    # Phase 3: Generation
    diagnosis = await generate_diagnosis(anomaly, context, retrieved_chunks)

    logger.info(
        f"Diagnosis complete for anomaly {anomaly.anomaly_id}: "
        f"confidence={diagnosis['confidence_score']:.2f}, "
        f"generation_time={diagnosis['generation_time_seconds']:.2f}s"
    )

    return diagnosis

