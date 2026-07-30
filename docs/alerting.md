# Alerting Architecture

Detailed documentation of the alerting pipeline: data flow, design decisions, alert rule taxonomy, and triage strategy.

---

## Data Flow

```
+-------------------------------------------------------------------------+
|                         METRICS COLLECTION                              |
+-------------------------------------------------------------------------+
|                                                                         |
|  API Gateway (:8000/metrics)                                            |
|  Ingestion Consumer (:9090/metrics)         ------->  Prometheus        |
|  Anomaly Detection (:9091/metrics)                   (15s scrape)       |
|  Diagnosis Service (:9092/metrics)                                      |
|  Alert Receiver (:9097/metrics)                                         |
|                                                                         |
+-------------------------------------------------------------------------+
                                    |
                                    ▼
+-------------------------------------------------------------------------+
|                         RULE EVALUATION                                 |
+-------------------------------------------------------------------------+
|                                                                         |
|  1. Recording Rules (every 15s)                                         |
|     - Pre-compute rates, ratios, percentiles                            |
|     - Results stored as new time series in Prometheus                   |
|                                                                         |
|  2. Alert Rules (every 15s)                                             |
|     - Evaluate PromQL expressions against recording rules               |
|     - If condition true for `for` duration → transition to FIRING       |
|     - If condition false → transition to RESOLVED                       |
|                                                                         |
|  3. Custom Rules (from Monitor CRUD API)                                |
|     - User-created rules stored in PostgreSQL                           |
|     - Written to custom_rules.yml on create/update/delete               |
|     - Prometheus hot-reloads via POST /-/reload                         |
|                                                                         |
+-------------------------------------------------------------------------+
                                    |
                                    ▼
+-------------------------------------------------------------------------+
|                         ALERT ROUTING (Alertmanager)                    |
+-------------------------------------------------------------------------+
|                                                                         |
|  1. Grouping                                                            |
|     - Alerts grouped by: alertname, pipeline, severity                  |
|     - Reduces notification spam (10 ServiceDown → 1 notification)       |
|                                                                         |
|  2. Inhibition (7 rules)                                                |
|     - ServiceDown suppresses warnings on same pipeline                  |
|     - CascadingFailure suppresses individual ServiceDown                |
|     - DiagnosisPipelineDown suppresses DiagnosisPartialFailures         |
|     - Critical suppresses info on same pipeline                         |
|                                                                         |
|  3. Routing                                                             |
|     - Critical → PagerDuty (page) + Webhook (persist)                   |
|     - Warning → Webhook only                                            |
|     - Info → Webhook only (relaxed timing)                              |
|                                                                         |
|  4. Timing                                                              |
|     - group_wait: 10s (critical) to 1m (info)                           |
|     - repeat_interval: 1h (critical) to 12h (info)                      |
|                                                                         |
+-------------------------------------------------------------------------+
                                    |
                                    ▼
+-------------------------------------------------------------------------+
|                         ALERT PROCESSING (Alert Receiver)               |
+-------------------------------------------------------------------------+
|                                                                         |
|  1. Receive Webhook (POST /alerts)                                      |
|     - Parse Alertmanager payload (batch of alerts)                      |
|     - Validate and extract labels/annotations                           |
|                                                                         |
|  2. Persist State (PostgreSQL)                                          |
|     - Upsert by fingerprint (deduplication)                             |
|     - Track firing → resolved transitions                               |
|     - Partial unique index prevents duplicate firing records            |
|                                                                         |
|  3. Update Metrics                                                      |
|     - alert_events_received_total{status, severity}                     |
|     - alerts_currently_firing (gauge)                                   |
|     - alert_webhook_delivery_seconds (latency histogram)                |
|                                                                         |
|  4. Trigger Triage (critical alerts only, async)                        |
|     - Product alert → query existing diagnoses → summarize              |
|     - Platform alert → LLM (or rule-based fallback)                     |
|     - Result stored in triage_summary column                            |
|                                                                         |
+-------------------------------------------------------------------------+
```

---

## Alert Rule Taxonomy

### Critical (5 rules) — Page immediately

| Alert | Condition | For | Rationale |
|-------|-----------|-----|-----------|
| ServiceDown | `up == 0` | 1m | Any service unreachable = data loss risk |
| IngestionPipelineStalled | `tip:ingestion:rate5m == 0` | 5m | Zero throughput = complete pipeline failure |
| CriticalAnomalyBurst | `anomalies{critical} > 0.1/s` | 2m | Sustained critical anomalies = real incident |
| DiagnosisPipelineDown | `failure_ratio > 0.9` | 5m | >90% failures = diagnoses unavailable |
| AlertReceiverDown | `up{alert-receiver} == 0` | 1m | Meta-alert: alerting pipeline itself is broken |

### Warning (8 rules) — Notify, don't wake

| Alert | Condition | For | Rationale |
|-------|-----------|-----|-----------|
| HighAPIErrorRate | `5xx ratio > 5%` | 5m | Users experiencing errors |
| HighAPILatency | `p95 > 2s` | 5m | Degraded user experience |
| DLQAccumulating | `DLQ rate > 0.05/s` | 5m | Validation failures trending up |
| SlowDiagnosisGeneration | `p95 > 60s` | 5m | LLM responses degraded |
| HighAnomalyRate | `total rate > 0.033/s` | 5m | >10 anomalies per 5min |
| KafkaPublishErrors | `errors > 0` | 3m | Events may be lost |
| HighMemoryUsage | `RSS > 512MB` | 10m | Potential OOM risk |
| DiagnosisPartialFailures | `failure_ratio > 25%` | 10m | Partial service degradation |

### Compound (6 rules) — Multi-condition

| Alert | Conditions | Why compound? |
|-------|-----------|---------------|
| PipelineDegradation | Ingestion ↓50% AND Detection ↓80% | Distinguishes "fewer events" from "pipeline broken" |
| CascadingFailure | Backend DOWN AND API errors >10% | Identifies failure propagation |
| KafkaIngestionFailure | Kafka errors AND Ingestion ↓50% | Pinpoints Kafka as root cause |
| DetectionWithoutDiagnosis | Critical anomalies AND Diagnosis failing >50% | "Seeing problems but can't explain them" |
| FullPipelineStall | Zero ingestion AND Zero analysis AND API UP | Silent failure — most dangerous |
| DLQRatioSpike | >10% of events going to DLQ | Systematic data quality issue |

---

## Triage Strategy

```
Critical alert fires
│
├── Is it a PRODUCT alert? (CriticalAnomalyBurst, DetectionWithoutDiagnosis)
│   │
│   ├── Query PostgreSQL: recent diagnoses (last 30 min)
│   │   └── Found? → Summarize existing diagnoses
│   │       (root causes, confidence scores, affected devices, actions)
│   │
│   └── Not found? → Fall through to LLM/rule-based
│
├── Is it a PLATFORM alert? (ServiceDown, PipelineStalled, etc.)
│   │
│   ├── CLAUDE_MODEL_ID configured?
│   │   └── Yes → Invoke Claude with alert context + system architecture
│   │
│   └── No → Rule-based template (always available)
│       └── Specific to each alert type:
│           - Root cause explanation
│           - Prioritized investigation steps (with docker commands)
│           - Blast radius assessment
│           - Immediate remediation actions
│
└── Result → stored in alert_events.triage_summary
    └── Available via GET /alerts/{id}/triage
```

### Why this approach?

1. **No redundant LLM calls** — If the Diagnosis Service already analyzed the anomalies, we reuse that work
2. **Works without LLM** — Rule-based templates provide actionable triage for all platform alerts
3. **Non-blocking** — Triage runs as a background task (asyncio.create_task), webhook responds immediately
4. **Targeted** — Only critical alerts get triage (warnings don't justify the compute)

---

## Recording Rules (Pre-computed Metrics)

Recording rules run every 15s and store results as new time series. Alert rules reference these instead of computing complex expressions in real-time.

| Rule | Expression | Used by |
|------|-----------|---------|
| `job:http_error_ratio_5xx:rate5m` | 5xx / total requests | HighAPIErrorRate, CascadingFailure |
| `job:http_latency_p95:rate5m` | histogram_quantile(0.95, ...) | HighAPILatency |
| `tip:ingestion:rate5m` | rate(events_ingested[5m]) | IngestionPipelineStalled, PipelineDegradation |
| `tip:anomalies_critical_high:rate5m` | rate(anomalies{critical\|high}[5m]) | CriticalAnomalyBurst, DetectionWithoutDiagnosis |
| `tip:diagnosis_failure_ratio:rate5m` | failures / (success + failures) | DiagnosisPipelineDown, DetectionWithoutDiagnosis |
| `tip:ingestion_rate_vs_baseline:ratio` | current / (15m ago) | PipelineDegradation, KafkaIngestionFailure |
| `tip:detection_rate_vs_baseline:ratio` | current / (15m ago) | PipelineDegradation |

---

## Inhibition Logic

Inhibition prevents alert storms by suppressing symptoms when the root cause is already known:

```
ServiceDown(ingestion)
  └── suppresses: all warnings on ingestion pipeline
      (HighAPIErrorRate won't fire if the backend is already known to be down)

CascadingFailure
  └── suppresses: individual ServiceDown alerts
      (CascadingFailure provides more context than bare ServiceDown)

FullPipelineStall
  └── suppresses: IngestionPipelineStalled
      (FullPipelineStall is the broader diagnosis)

DiagnosisPipelineDown (>90% failure)
  └── suppresses: DiagnosisPartialFailures (>25% failure)
      (Complete failure supersedes partial failure alert)

Any critical alert
  └── suppresses: all info-level alerts on same pipeline
      (Don't pollute with observational info during a crisis)
```

---

## Monitor CRUD API — Dynamic Rules

Users can create custom alert rules programmatically without restarting Prometheus:

```
POST /monitors                    →  Insert into PostgreSQL
  │                                   (source of truth)
  ├── Regenerate custom_rules.yml →  Written to shared Docker volume
  │                                   (Prometheus reads this)
  └── POST prometheus:9090/-/reload → Prometheus hot-reloads config
                                      (rule active within 15s)
```

The API response includes `prometheus_reload.success` — if Prometheus is unreachable, the rule is persisted in DB and will take effect on next restart.

On service startup, the alert receiver syncs `custom_rules.yml` from DB state, ensuring the file always matches reality even after container recreation.

---

## Self-Monitoring

The alert receiver monitors itself via:

| Metric | What it measures |
|--------|-----------------|
| `alert_events_received_total{status, severity}` | Total alerts processed |
| `alerts_currently_firing` | Gauge of active alerts |
| `alert_webhook_delivery_seconds` | Time from alert start to webhook receipt |
| `alert_webhook_requests_total{status_code}` | Webhook request outcomes |
| `alert_processing_errors_total{error_type}` | Processing failures |
| `alert_triage_invocations_total{result}` | Triage attempts (success/failure/skipped) |
| `alert_triage_duration_seconds` | Triage generation time |

The `AlertReceiverDown` alert rule fires if this service becomes unreachable — a meta-alert that detects when the alerting pipeline itself is broken.

---

## Load Test Results

Tested with Docker Compose on a single machine:

| Scenario | Throughput | Latency p95 | Drop Rate |
|----------|-----------|-------------|-----------|
| Sustained (100 alerts/sec × 30s) | 100/sec | 305ms | 0.00% |
| Burst (500 alerts, 1 request) | 390/sec | 1280ms total | 0.00% |
| Concurrent (50 × 10 alerts) | 226/sec | 2164ms | 0.00% |
| Triage non-blocking | — | 58ms webhook | — |

Key findings:
- Zero alert drops under all tested loads
- Triage background tasks do not block webhook processing (58ms vs potential 30s for LLM)
- Bottleneck is PostgreSQL connection pool (10 connections) under heavy concurrency


---

## Failure Modes & Tradeoffs

### What happens when the Alert Receiver is down?

```
Prometheus ---> Alertmanager ---> Alert Receiver (DOWN) ---> PostgreSQL
   ✅              ✅                  ❌                    ❌
 (rules still    (retries with      (webhook fails)     (no persistence)
  evaluate)      exponential
                 backoff)
```

**Impact:**
- Alerts that **fire AND resolve** entirely during the downtime → lost from PostgreSQL history
- Alerts that are **still firing** when receiver recovers → delivered and persisted (Alertmanager sends current state)
- PagerDuty notifications → **unaffected** (separate receiver, independent delivery path)
- Prometheus and Alertmanager → **unaffected** (they don't depend on the webhook succeeding)

**Alertmanager's built-in resilience:**
- Retries webhook delivery with exponential backoff
- Keeps all firing alerts in memory
- On receiver recovery, delivers current alert state (all currently firing + recently resolved)
- `--storage.path` persists notification state across Alertmanager restarts

**What we accept:**
- A few minutes of missing alert history in PostgreSQL during receiver downtime
- The `AlertReceiverDown` meta-alert fires but cannot be delivered to itself (circular). PagerDuty still receives it.
- Triage summaries won't be generated during downtime (but can be requested manually via GET endpoint after recovery)

**Why this is acceptable:**
- The alert receiver is a **persistence and enrichment** layer, not the alerting mechanism itself. Alertmanager is the source of truth for alert state.
- Critical alerts still reach PagerDuty regardless of receiver health.
- The receiver is a simple FastAPI service with minimal dependencies (only PostgreSQL). Its failure rate is expected to be very low.
- Adding a queue (e.g., Kafka) between Alertmanager and the receiver would add complexity and create a dependency cycle (Kafka down → alerts about Kafka → need Kafka to deliver alerts).

### What happens when Prometheus is down?

- **No rules evaluated** → no alerts fire → Alertmanager receives nothing
- Existing firing alerts in Alertmanager remain in their current state (not resolved, not re-notified)
- On recovery, Prometheus re-evaluates all rules immediately and fires any that are now true
- **Mitigation:** Prometheus is a single point of failure for the alerting pipeline. In production, consider Prometheus HA (Thanos or Cortex) or a separate watchdog that monitors Prometheus itself.

### What happens when Alertmanager is down?

- Prometheus continues evaluating rules but has no target to send alerts to
- Prometheus logs delivery failures and retries on the next evaluation cycle
- On recovery, Prometheus sends all currently firing alerts to Alertmanager
- **Mitigation:** Alertmanager supports HA clustering (multiple instances with gossip protocol). Not implemented here as it's a single-node dev setup.

### What happens when PostgreSQL is down?

- Alert Receiver health check fails → `AlertReceiverDown` fires (via Prometheus scraping metrics port, which still works)
- Webhook POST returns 500 → Alertmanager retries
- Monitor CRUD API returns 503
- On PostgreSQL recovery, Alertmanager redelivers current state
- **Mitigation:** PostgreSQL being down affects the entire platform (all services), not just alerting. It's already covered by the `ServiceDown` alert.
