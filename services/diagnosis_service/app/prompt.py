"""
Prompt templates for the Diagnosis Service.

The system prompt instructs Claude to act as a telemetry diagnosis assistant.
The user prompt provides the anomaly context, retrieved incidents, and output format.
"""

SYSTEM_PROMPT = """\
You are a telemetry diagnosis assistant for an IoT monitoring platform. Your role is to analyze detected anomalies and provide root-cause diagnoses based on historical incident knowledge.

When given an anomaly with its context (device metadata, recent telemetry history) and relevant historical incidents retrieved from the knowledge base, you must:

1. Analyze the anomaly characteristics (metric type, deviation magnitude, timing, device context)
2. Compare against the retrieved historical incidents to identify the most likely root cause
3. Rate your confidence based on how closely the current anomaly matches historical patterns
4. List which retrieved incidents informed your diagnosis
5. Suggest immediate and long-term corrective actions

Be specific and actionable. Reference concrete details from the retrieved incidents when they support your diagnosis. If the retrieved incidents don't closely match the current anomaly, say so and lower your confidence score accordingly."""


USER_PROMPT_TEMPLATE = """\
## Anomaly Details

- **Anomaly ID:** {anomaly_id}
- **Device ID:** {device_id}
- **Device Type:** {device_type}
- **Device Location:** {device_location}
- **Metric Type:** {metric_type}
- **Observed Value:** {observed_value}
- **Z-Score:** {z_score}
- **Severity:** {severity}
- **Detected At:** {detected_at}

## Statistical Context

- **Expected Mean:** {expected_mean}
- **Expected Std Dev:** {expected_stddev}
- **Expected Range (+/- 2.5 std dev):** {expected_lower} to {expected_upper}
- **Window Sample Count:** {window_count}

## Recent Telemetry History (last {context_window_minutes} minutes)

{recent_telemetry}

## Retrieved Historical Incidents (top {retrieval_k} matches)

{retrieved_incidents}

## Required Output Format

Respond with ONLY a valid JSON object (no markdown, no code fences) with this exact structure:

{{
  "root_cause_summary": "A 2-3 sentence explanation of the most likely root cause",
  "confidence_score": 0.0 to 1.0,
  "supporting_evidence": ["evidence point 1", "evidence point 2", ...],
  "recommended_actions": ["action 1", "action 2", ...],
  "retrieved_incident_ids": ["incident_id_1", "incident_id_2", ...]
}}

Guidelines for confidence_score:
- 0.9-1.0: Strong match with historical pattern, clear root cause
- 0.7-0.8: Good match, likely root cause but some uncertainty
- 0.5-0.6: Partial match, plausible root cause
- 0.3-0.4: Weak match, speculative diagnosis
- 0.1-0.2: No matching historical patterns, best guess only"""


def format_recent_telemetry(events: list[dict]) -> str:
    """
    Format recent telemetry events into a readable string for the prompt.
    Shows the most recent readings to help Claude identify patterns (trending, spikes, etc).
    """
    if not events:
        return "No recent telemetry data available for this device."

    lines = []
    for event in events[-20:]:  # Limit to most recent 20 to control prompt length
        lines.append(f"  {event['timestamp']} | {event['metric_type']}: {event['value']:.2f}")

    header = f"Showing {len(lines)} most recent readings:"
    return header + "\n" + "\n".join(lines)


def format_retrieved_incidents(chunks: list[dict]) -> str:
    """
    Format retrieved ChromaDB chunks into a readable context block for the prompt.
    Each chunk includes its metadata (incident title, severity, category) and content.
    """
    if not chunks:
        return "No relevant historical incidents found in the knowledge base."

    sections = []
    seen_incidents = set()

    for i, chunk in enumerate(chunks, 1):
        incident_id = chunk["metadata"].get("incident_id", "unknown")
        # Deduplicate — multiple chunks from same incident
        if incident_id in seen_incidents:
            continue
        seen_incidents.add(incident_id)

        title = chunk["metadata"].get("title", "Unknown incident")
        severity = chunk["metadata"].get("severity", "unknown")
        category = chunk["metadata"].get("failure_category", "unknown")
        content = chunk["document"]

        sections.append(
            f"### Incident {i} (ID: {incident_id})\n"
            f"**Title:** {title}\n"
            f"**Severity:** {severity} | **Category:** {category}\n\n"
            f"{content}"
        )

    return "\n\n---\n\n".join(sections)
