# Architecture Decision Log

Documenting key design decisions and their rationale as the project evolves.

---

## 001: Event-Driven Ingestion via Kafka Instead of Direct Database Writes

**Date:** 2026-05-29
**Status:** Accepted

**Context:** The API Gateway receives telemetry events from devices and needs to persist them to PostgreSQL. The simplest approach is a direct database write in the request handler.

**Decision:** The API Gateway publishes events to a Kafka topic (`telemetry.raw`) and returns 202 Accepted immediately. A separate Ingestion Consumer service reads from Kafka and writes to PostgreSQL.

**Rationale:**
- **Decoupling:** Ingestion throughput is independent of database write speed. If PostgreSQL is slow or temporarily unavailable, events queue in Kafka rather than causing API timeouts.
- **Durability:** Kafka persists messages to disk with configurable retention. If the consumer crashes, no data is lost — it resumes from the last committed offset on restart.
- **Downstream flexibility:** Other services (Anomaly Detection, future analytics) can subscribe to the same topic without modifying the API. Adding consumers doesn't require API changes.
- **Backpressure handling:** Kafka naturally buffers during traffic spikes. The consumer processes at its own pace.

**Tradeoff:** Added operational complexity (Kafka infrastructure), eventual consistency (event isn't queryable until the consumer processes it), and a more complex debugging path (message may be in Kafka but not yet in the database).

---

## 002: Redis as Multi-Purpose State Store (Cache + Rolling Windows + Metadata)

**Date:** 2026-06-03
**Status:** Accepted

**Context:** Week 2 introduces three distinct needs: device metadata lookup on every event (high-frequency reads), rolling statistical windows for anomaly detection (sorted data with time-based expiry), and caching of read-heavy API responses.

**Decision:** Use a single Redis instance with different data structures for each purpose: Hashes for device metadata, Sorted Sets for rolling windows, and Strings with TTL for query caching.

**Rationale:**
- Redis natively supports all three access patterns with sub-millisecond latency.
- Hashes (HSET/HGETALL) provide field-level access to device metadata without full deserialization — efficient for enrichment where we only need 2-3 fields.
- Sorted Sets with timestamp scores give O(log N) insertion and efficient range trimming (ZREMRANGEBYSCORE) — ideal for sliding time windows.
- String keys with TTL (SET EX 60) provide simple cache-aside with automatic expiry — no invalidation logic needed.
- A single Redis instance reduces operational complexity compared to multiple caching solutions.

**Tradeoff:** Redis is an in-memory store. If it goes down, we lose cached data and rolling window state. The system is designed to degrade gracefully — device metadata falls back to PostgreSQL, cache misses hit the database directly, and rolling windows rebuild naturally as new events arrive.

---

## 003: Z-Score on Rolling Windows for Anomaly Detection (vs. Fixed Thresholds)

**Date:** 2026-06-04
**Status:** Accepted

**Context:** The system needs to detect anomalous telemetry readings automatically. Options considered: fixed threshold alerts (e.g., "temperature > 80°C"), percentage change from last reading, or statistical deviation from a rolling baseline.

**Decision:** Use z-score computation against a rolling 5-minute window per device per metric. A reading is anomalous when its z-score exceeds configurable thresholds (2.5/3.0/3.5/4.0 mapping to low/medium/high/critical severity).

**Rationale:**
- Z-score adapts to each device's normal behavior. A server running hot at 85°C baseline won't false-alarm, while a server normally at 45°C spiking to 85°C will.
- Rolling windows mean the baseline evolves with time-of-day patterns and gradual drift.
- Configurable thresholds via environment variables allow tuning without code changes.
- Pure-function computation (no ML model) makes it deterministic, testable, and explainable.

**Tradeoff:** Z-score on a short window is vulnerable to "mean drift" — if anomalous readings persist, they get absorbed into the window's mean and stddev, reducing subsequent z-scores. This is acceptable for this project's scope. In production, you'd add a separate long-term baseline or use a more robust algorithm (e.g., EWMA, isolation forests).


## 004: RBAC Model — Three Roles with Hierarchical Permissions

**Date:** 2026-06-09
**Status:** Accepted

**Context:** The API Gateway needs access control to differentiate between users who can write telemetry data, users who can only view it, and administrators who manage the system. The system design specifies JWT with RBAC.

**Decision:** Implement a three-role hierarchy (viewer < operator < admin) where higher roles inherit all permissions of lower roles. Roles are encoded in JWT claims and enforced via a `require_role()` FastAPI dependency factory.

**Permission matrix:**
- `viewer` — GET endpoints only (telemetry, anomalies, devices)
- `operator` — viewer permissions + POST telemetry + register devices
- `admin` — operator permissions + system configuration + knowledge ingestion (Week 5)

**Rationale:**
- Hierarchical roles simplify enforcement — each endpoint only specifies a minimum role rather than listing every allowed role.
- JWT claims carry the role, so authorization doesn't require a database lookup on every request.
- The `require_role()` factory pattern integrates cleanly with FastAPI's dependency injection system, keeping route handlers focused on business logic.

**Tradeoff:** Users are hardcoded in a dictionary with plain-text passwords. This is acceptable for a learning project demonstrating JWT/RBAC mechanics. A production system would require: a `users` table with bcrypt-hashed passwords, a user management API (registration, password changes), account lockout after failed attempts, and potentially integration with an external identity provider (OAuth2/OIDC). The JWT issuance and validation patterns would remain the same — only the credential verification step changes.


## 005: Rate Limiting — Sliding Window via Redis Sorted Sets

**Date:** 2026-06-10
**Status:** Accepted

**Context:** The API needs protection against abuse and accidental overload. Options considered: fixed-window counters (simple but has boundary spike problem), token bucket (complex state), or sliding window (accurate and uses familiar Redis patterns).

**Decision:** Implement per-user per-endpoint rate limiting using Redis sorted sets with timestamp scores — the same pattern used for anomaly detection rolling windows. Key pattern: `ratelimit:{user_id}:{endpoint_name}`. Configured limits: 1000 req/min for ingestion, 100 req/min for queries.

**Rationale:**
- Sliding window avoids the "boundary spike" problem where a user could double their effective limit by timing requests across a fixed-window boundary.
- Reuses the same Redis sorted set pattern as the anomaly detection rolling windows (ZADD + ZREMRANGEBYSCORE + ZCARD in a pipeline), reducing conceptual overhead.
- Fail-open design: if Redis is unavailable, rate limiting is skipped rather than blocking all requests. This prioritizes availability over strict enforcement.
- `Retry-After` header in 429 responses tells clients exactly when to retry (computed from the oldest entry in the window).

**Tradeoff:** Rate limit state is lost if Redis restarts — users get a fresh window. Acceptable since rate limiting is a protection mechanism, not a correctness requirement. The fail-open behavior means a Redis outage temporarily disables rate limiting entirely.

**Tech debt:** The Kafka publish timeout (5 seconds in `telemetry.py`) is currently hardcoded. This is sufficient for local single-broker development where normal latency is sub-millisecond, but should be promoted to a configurable environment variable (`KAFKA_PUBLISH_TIMEOUT_SECONDS`) in Week 4 when deploying to a cloud environment where cross-AZ latency and broker GC pauses could cause occasional slowness.


## 006: Prometheus Client Library for Application Metrics

**Date:** 2026-06-12
**Status:** Accepted

**Context:** Week 4 requires observability instrumentation across all three services. The API Gateway is an HTTP server (FastAPI), but the Ingestion Consumer and Anomaly Detection Service are standalone Kafka consumers with no HTTP server.

**Decision:** Use `prometheus-client` with the default global registry. The API Gateway exposes `/metrics` on its existing HTTP port (8000) via a FastAPI route and uses Starlette middleware for request-level metrics. The two consumer services use `prometheus_client.start_http_server()` to spin up a lightweight metrics server on side ports (9090 for Ingestion Consumer, 9091 for Anomaly Detection).

**Rationale:**
- `prometheus-client` is the standard Python library for Prometheus instrumentation — minimal dependencies and widely documented.
- The API Gateway already has an HTTP server, so adding a `/metrics` route is trivial (no new port needed).
- Consumer services have no HTTP server. `start_http_server()` runs a background thread serving only `/metrics` — avoids adding a full async web framework just for one endpoint.
- All metrics are defined in a shared module (`shared/metrics.py`) for consistency. Counters that aren't incremented by a particular service stay at 0 — cosmetically noisy but functionally harmless.
- Metrics ports are configurable via each service's `config.py` settings.

**Tradeoff:** Because all counters are defined in the shared global registry, every service's `/metrics` endpoint exposes all metric definitions (including ones it doesn't use). Prometheus won't care — it queries by metric name. If this becomes a problem, separate `CollectorRegistry()` instances per service would fix it, but adds complexity for no functional benefit at this scale.


## 007: Multi-Stage Docker Builds with uv for Reproducible Containers

**Date:** 2026-06-12
**Status:** Accepted

**Context:** Week 4 requires packaging all application services as Docker containers. Options considered for dependency installation inside containers: pip (traditional), pip with exported requirements.txt from uv.lock (hybrid), or uv directly in the build stage (modern).

**Decision:** Use multi-stage Docker builds with `uv` in the builder stage and `python:3.11-slim` as the base image. The `uv` binary is copied from `ghcr.io/astral-sh/uv:latest` into the builder stage, installs dependencies via `uv sync --frozen --no-dev --no-editable`, and the resulting `.venv` is copied to the slim runtime stage. Each service has its own Dockerfile but all use the project root as build context. The simulator uses a `profiles: [simulator]` to prevent automatic startup.

**Rationale:**
- `uv sync --frozen` guarantees the exact same dependency versions as local development (uses `uv.lock`), eliminating "works on my machine" issues.
- `uv` installs 10-100x faster than pip, significantly reducing CI/CD build times.
- Multi-stage builds keep the `uv` binary and build tools out of the runtime image (~150-200MB final size vs ~400MB+ with build dependencies).
- `python:3.11-slim` over Alpine avoids compilation issues with C extensions (`asyncpg`, `cryptography`).
- Project root as build context allows all Dockerfiles to access `pyproject.toml`, `uv.lock`, and `shared/` without duplication.
- Simulator profile prevents data flooding during debugging — must be explicitly started with `docker compose --profile simulator up`.

**Tradeoff:** All services install the full dependency set (since there's one `pyproject.toml`). Individual services only use a subset — e.g., the simulator doesn't need `asyncpg`. This adds ~20-30MB per image but avoids maintaining per-service dependency files. Acceptable for this project's scale.


## 008: Prometheus + Grafana for Observability with Auto-Provisioned Dashboards

**Date:** 2026-06-14
**Status:** Accepted

**Context:** Week 4 requires live observability dashboards showing ingestion throughput, request latency, error rates, and anomaly detection metrics. The system design specifies Prometheus for metrics collection and Grafana for visualization. All three application services already expose `/metrics` endpoints (decision 006).

**Decision:** Deploy Prometheus and Grafana as Docker Compose services alongside the application stack. Prometheus scrapes all three application services every 15 seconds via their Docker network hostnames. Grafana auto-provisions the Prometheus data source and three dashboards (System Overview, Ingestion Pipeline, Anomaly Detection) on startup via file-based provisioning in `monitoring/provisioning/`.

**Rationale:**
- File-based provisioning (`monitoring/provisioning/datasources/` and `monitoring/provisioning/dashboards/`) means Grafana is fully configured on first startup — no manual UI setup required. This is reproducible across environments and survives `docker compose down -v`.
- Prometheus uses service names (`api-gateway:8000`, `ingestion-consumer:9090`, `anomaly-detection:9091`) as scrape targets, leveraging Docker Compose's built-in DNS resolution.
- Three focused dashboards (system overview, ingestion pipeline, anomaly detection) rather than one monolithic dashboard — each serves a different operational question.
- 7-day retention on Prometheus (`--storage.tsdb.retention.time=7d`) is sufficient for development and keeps disk usage bounded.

**Tradeoff:** Dashboard JSON files are verbose and difficult to edit by hand. The practical workflow is: edit dashboards in the Grafana UI, export the JSON, and commit it to `monitoring/provisioning/dashboards/`. The `uid: prometheus` must be explicitly set in the datasource provisioning to match the `datasource` references in dashboard JSON. Prometheus host port (9094) chosen to avoid confusion with Kafka's internal port 9092, though there is no actual collision.


## 009: Liveness vs. Readiness Probes — What Each Checks and Why They Differ

**Date:** 2026-06-15
**Status:** Accepted

**Context:** Kubernetes uses probes to determine pod health. Two types serve different purposes: liveness (is the process alive?) and readiness (can it serve traffic?). The system design specifies both for all services.

**Decision:** Implement two distinct health endpoints:
- `GET /health` (liveness) — returns 200 if the process is running and can handle HTTP. Does NOT check downstream dependencies. If this fails, Kubernetes kills and restarts the pod.
- `GET /ready` (readiness) — returns 200 only if downstream dependencies (Kafka, Redis, PostgreSQL) are reachable. If this fails, Kubernetes stops sending traffic to the pod but does NOT restart it.

**Rationale:**
- Liveness must be cheap and dependency-free. If liveness checked Redis and Redis went down, Kubernetes would restart all API pods — making the outage worse (cascading failure). The process itself is fine; it just can't serve cached responses.
- Readiness is appropriate for dependency checks because the consequence is traffic routing, not pod termination. A pod that can't reach Kafka shouldn't receive POST telemetry requests, but it doesn't need to be killed — Kafka may recover momentarily.

**Tradeoff:** A pod could be "live" but not "ready" indefinitely if a dependency is permanently down. Operators must monitor readiness state and intervene. The HPA won't scale up unready pods, preventing resource waste.


## 010: kind for Development, Terraform for Production — Cost Strategy

**Date:** 2026-06-16
**Status:** Accepted

**Context:** The project needs both a local development environment (for daily iteration) and a production infrastructure definition. Running cloud infrastructure continuously is expensive (~$360/month for the defined stack).

**Decision:** Use `kind` (Kubernetes IN Docker) for local development and testing. Use Terraform to define production AWS infrastructure (VPC, EKS, RDS, ElastiCache, MSK, ECR, IAM) as code, validated via `terraform plan` without actually deploying. The same Kubernetes manifests work in both environments — only the infrastructure layer differs.

**Rationale:**
- `kind` runs a full Kubernetes cluster inside Docker containers at zero cost. The K8s manifests (Deployments, Services, ConfigMaps, etc.) are identical to production — the only difference is how images are loaded and how services are exposed (NodePort vs. LoadBalancer).
- Terraform validates configuration syntax and resource dependencies via `terraform plan` without creating resources (no AWS charges). The plan output demonstrates infrastructure knowledge without incurring cost.

**Tradeoff:** kind has no persistent storage by default — Kafka loses topics on pod restart (requires re-running the kafka-init Job). In production, AWS MSK provides durable storage natively. Local development requires a specific deployment order (infrastructure → kafka-init → applications) that wouldn't be needed with managed services.


## 011: AWS Bedrock via Application Inference Profiles

**Date:** 2026-06-23
**Status:** Accepted

**Context:** The RAG pipeline requires an embedding model (for indexing and retrieval) and a generation model (for producing diagnoses). Options considered: OpenAI (GPT-4 + text-embedding-3), self-hosted models (Ollama + local embeddings), or AWS Bedrock (managed access to multiple model providers).

**Decision:** Use AWS Bedrock accessed through application inference profiles. Cohere Embed v4 for embeddings (1536 dimensions, asymmetric search via `input_type` parameter). Claude Haiku 4.5 for diagnosis generation (fast, cost-effective, structured JSON output). Both accessed via full ARN inference profile IDs rather than direct model IDs.

**Rationale:**
- Ecosystem alignment: The platform already uses AWS (Terraform defines EKS, RDS, ElastiCache, MSK). Adding Bedrock keeps all services within one cloud provider, simplifying IAM, networking, and billing.
- No API keys to manage: Authentication uses IAM roles (IRSA in production, assumed roles in development). No secrets to rotate or store beyond AWS credentials.
- Application inference profiles provide organization-level access control and cost tracking without managing individual model access grants.
- Cohere Embed v4's `input_type` parameter (search_document vs search_query) improves retrieval accuracy through asymmetric embeddings — documents and queries are embedded differently for optimal matching.
- Claude Haiku 4.5 provides sufficient quality for structured diagnosis output at ~10x lower cost and ~5x lower latency than larger models.

**Tradeoff:** Vendor lock-in to AWS. The pipeline isolates all Bedrock calls in two small functions (`_generate_embeddings` and `_call_llm`) that could be swapped to OpenAI or local models by changing only those functions and the config. boto3 clients cache credentials and don't auto-refresh — services must be restarted when assumed-role sessions expire. In production, IRSA provides automatic credential rotation.


## 012: Chunking Strategy — RecursiveCharacterTextSplitter with 2048 Characters / 200 Overlap

**Date:** 2026-06-23
**Status:** Accepted

**Context:** Incident documents must be split into chunks for embedding and storage in ChromaDB. Options considered: fixed character split, token-based split (tiktoken `TokenTextSplitter`), or recursive character split with natural boundary detection.

**Decision:** Use LangChain's `RecursiveCharacterTextSplitter` with `chunk_size=2048` characters (~512 tokens) and `chunk_overlap=200` characters (~50 tokens). Separators: `["\n\n", "\n", ". ", ", ", " ", ""]`.

**Rationale:**
- RecursiveCharacterTextSplitter preserves semantic boundaries by trying paragraph breaks, then sentence breaks, then word breaks before resorting to arbitrary character splits. For our short, structured incident documents (typically 500-1500 characters), this means most incidents fit in a single chunk without any splitting at all.
- Character-based measurement avoids needing a tokenizer dependency (tiktoken). The approximation (4 chars ≈ 1 token for English) is sufficient for our use case where documents are short and chunk boundaries matter less than semantic coherence.
- 200-character overlap ensures context continuity if a document does split across chunks — the end of one chunk appears at the start of the next, preserving sentence context at boundaries.
- Token-based splitting (e.g., tiktoken `TokenTextSplitter`) guarantees exact token counts but may split mid-word or mid-sentence. For our structured incident reports where semantic boundaries matter more than exact token precision, recursive character splitting produces better retrieval results.

**Tradeoff:** Not exact token counts — a "2048 character" chunk could be 400-600 tokens depending on content. This imprecision is acceptable because our documents are short (most fit in a single chunk) and embedding models handle variable-length inputs gracefully.


## 013: Dual Storage — PostgreSQL + ChromaDB for Structured Queries and Semantic Search

**Date:** 2026-06-23
**Status:** Accepted

**Context:** The RAG pipeline stores incident knowledge that must support two access patterns: structured queries (list incidents by severity, filter by category, paginate) and semantic search (find incidents similar to a given anomaly context). A single storage system cannot optimally serve both.

**Decision:** Store incident documents in both PostgreSQL (`incidents_knowledge` table) and ChromaDB (`incident_embeddings` collection). PostgreSQL holds the complete structured record. ChromaDB holds chunked text with vector embeddings and metadata linking back to PostgreSQL via `incident_id`.

**Rationale:**
- PostgreSQL excels at structured queries: `SELECT * FROM incidents_knowledge WHERE severity = 'high' AND failure_category = 'thermal_management' ORDER BY ingested_at DESC LIMIT 10`. These queries power the `GET /api/v1/knowledge/incidents` API endpoint.
- ChromaDB excels at semantic similarity: "find the 5 incident chunks most similar to this anomaly context embedding." This powers the retrieval phase of the RAG pipeline.
- Metadata on ChromaDB chunks (`incident_id`, `severity`, `failure_category`) enables filtered semantic search if needed (e.g., only retrieve chunks from high-severity thermal incidents).
- The `incidents_knowledge` table also serves as the source of truth for what's been ingested — if ChromaDB needs to be rebuilt, all documents can be re-chunked and re-embedded from PostgreSQL.

**Tradeoff:** Data is duplicated across two stores. Ingestion must write to both (within a single operation in `knowledge.py`). If one write succeeds and the other fails, the stores become inconsistent. For this project, we accept this risk. Production would wrap both writes in a saga pattern or use a two-phase approach (write to PostgreSQL first as source of truth, then async embed/upsert to ChromaDB with retry).


## 014: Dead-Letter Queue Dual-Write (Kafka Topic + PostgreSQL Table)

**Date:** 2026-06-28
**Status:** Accepted

**Context:** Failed validation events were published to the `telemetry.dlq` Kafka topic but never persisted to a queryable store. Operators had no way to inspect failed events without consuming the Kafka topic directly.

**Decision:** The Ingestion Consumer's `send_to_dlq()` now writes to both the `telemetry.dlq` Kafka topic and the `dead_letter_events` PostgreSQL table. A new `GET /api/v1/telemetry/dlq` endpoint (operator+) exposes failed events for debugging.

**Rationale:**
- The Kafka topic preserves the existing event-stream semantics (other consumers could process the DLQ), while the PostgreSQL table provides ad-hoc queryability (filter by time, paginate, inspect error reasons).
- Persistence failures are caught and logged without failing the Kafka publish — the DLQ write is best-effort so one storage layer failing doesn't break the other.
- Non-JSON payloads are wrapped as `{"raw": <payload>}` before insertion to satisfy the JSONB column constraint.

**Tradeoff:** Data is duplicated across Kafka and PostgreSQL. Acceptable because DLQ volume is low (only malformed events) and the two stores serve different access patterns.

---

## 015: Redis-First Device Existence Check on Ingestion

**Date:** 2026-06-29
**Status:** Accepted

**Context:** Every `POST /api/v1/telemetry` request verified device existence with a PostgreSQL query, adding a database round-trip to the hot ingestion path. Load testing showed per-request latency of ~6-7ms limiting single-worker throughput to ~133 events/sec.

**Decision:** Check Redis first (`EXISTS device:{device_id}`) and fall back to PostgreSQL only on cache miss or Redis failure. The `device:{device_id}` hash is already populated by the Ingestion Consumer's enrichment step.

**Rationale:**
- Sub-millisecond Redis existence check replaces a PostgreSQL round-trip for devices that have already sent at least one event (the common case).
- Falls back gracefully: brand-new devices and Redis outages still resolve correctly via PostgreSQL.
- Reuses the existing `device:{device_id}` cache key — no new cache pattern needed.

**Tradeoff:** Marginal benefit in isolation because the throughput ceiling is the cumulative per-request chain (JWT verification, rate limiting, Kafka ACK), not any single step. The real scaling path is horizontal (more workers/pods). Documented here as a targeted optimization that reduces database load under sustained throughput.



