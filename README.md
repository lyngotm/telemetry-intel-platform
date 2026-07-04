# Telemetry Intelligence Platform

A real-time telemetry ingestion and analysis platform with AI-powered anomaly detection and root-cause diagnosis via a RAG pipeline.

## Overview

This platform collects streaming device telemetry data, detects anomalies using statistical methods (z-score on rolling windows), and generates AI-powered root-cause diagnoses by retrieving semantically similar historical incidents and passing them to an LLM.

The system follows an event-driven microservices architecture with Apache Kafka as the central event backbone, separating the ingestion path (write-heavy, high-throughput) from the query path (read-heavy, low-latency) and the intelligence layer (compute-heavy, async).


[![Demo Thumbnail](https://img.youtube.com/vi/W3VOC68UXYs/maxresdefault.jpg)](https://youtu.be/W3VOC68UXYs)

[![▶ Watch Demo — 2:50 min](https://img.shields.io/badge/▶_Watch_Demo-2:50_min-red?style=for-the-badge&logo=youtube)](https://youtu.be/W3VOC68UXYs)


## Architecture

```mermaid
%%{init: {'themeVariables': {'fontSize': '18px'}}}%%
flowchart TD
    %% ---- Client ----
    CLIENT[Client<br/>simulator · dashboard · devices]

    %% ---- API Gateway ----
    subgraph API["API Gateway (FastAPI)"]
        POST_EP["POST<br/>telemetry · devices · knowledge/ingest · auth/token"]
        GET_EP["GET<br/>telemetry · anomalies · devices · diagnosis · dlq · knowledge"]
        PATCH_EP["PATCH<br/>anomalies/{id}/status"]
    end

    %% ---- Kafka ----
    subgraph Kafka["Kafka Topics"]
        RAW[telemetry.raw]
        ENRICHED[telemetry.enriched]
        ANOM[anomalies.detected]
        DLQ[telemetry.dlq]
    end

    %% ---- Consumers ----
    subgraph Consumers["Consumer Services"]
        IC[Ingestion Consumer]
        AD[Anomaly Detection]
        DS[Diagnosis Service]
    end

    %% ---- Data Stores ----
    subgraph Data["Data Stores"]
        PG[("PostgreSQL<br/>devices · telemetry_events<br/>anomalies · diagnoses<br/>dead_letter_events · incidents_knowledge")]
        REDIS[("Redis<br/>device metadata · rolling windows<br/>telemetry cache · rate limiting")]
        CHROMA[("ChromaDB<br/>incident_embeddings")]
    end

    %% ---- Ingestion path (edges 0-8) ----
    CLIENT -->|POST events| POST_EP
    POST_EP -->|publish| RAW
    POST_EP -->|device check| REDIS
    RAW --> IC
    IC -->|enrich: HGETALL| REDIS
    IC -->|persist event| PG
    IC -->|publish| ENRICHED
    IC -->|validation failure| DLQ
    IC -->|persist failed| PG

    %% ---- Anomaly detection (edges 9-12) ----
    ENRICHED --> AD
    AD -->|rolling window| REDIS
    AD -->|persist anomaly| PG
    AD -->|publish| ANOM

    %% ---- Diagnosis (edges 13-16) ----
    ANOM --> DS
    DS -->|recent history| PG
    DS -->|semantic search| CHROMA
    DS -->|write diagnosis| PG

    %% ---- Query path (edges 17-19) ----
    CLIENT -->|queries| GET_EP
    GET_EP -->|cache-aside / rate limit| REDIS
    GET_EP -->|read| PG

    %% ---- Status update path (edges 20-21) ----
    CLIENT -->|update status| PATCH_EP
    PATCH_EP -->|update anomaly status| PG

    %% ---- Flow color coding ----
    linkStyle 0,1,2,3,4,5,6,7,8 stroke:#2563eb,stroke-width:2px
    linkStyle 9,10,11,12 stroke:#ea580c,stroke-width:2px
    linkStyle 13,14,15,16 stroke:#16a34a,stroke-width:2px
    linkStyle 17,18,19 stroke:#9333ea,stroke-width:2px
    linkStyle 20,21 stroke:#64748b,stroke-width:2px
```
**Flow legend:** 🔵 Ingestion · 🟠 Anomaly Detection · 🟢 Diagnosis · 🟣 Query · ⚪ Status Update

## Observability

Prometheus scrapes the `/metrics` endpoint of every service on a 15-second
interval. Grafana queries Prometheus to render four auto-provisioned dashboards.

```mermaid
%%{init: {'themeVariables': {'fontSize': '18px'}}}%%
flowchart LR
    subgraph Services["Application Services"]
        API[API Gateway<br/>:8000/metrics]
        IC[Ingestion Consumer<br/>:9090/metrics]
        AD[Anomaly Detection<br/>:9091/metrics]
        DS[Diagnosis Service<br/>:9092/metrics]
    end

    PROM[(Prometheus<br/>15s scrape · 7d retention)]

    subgraph Dashboards["Grafana Dashboards"]
        D1[System Overview]
        D2[Ingestion Pipeline]
        D3[Anomaly Detection]
        D4[Diagnosis Service]
    end

    API -->|scrape| PROM
    IC -->|scrape| PROM
    AD -->|scrape| PROM
    DS -->|scrape| PROM
    PROM -->|PromQL| D1
    PROM -->|PromQL| D2
    PROM -->|PromQL| D3
    PROM -->|PromQL| D4
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| API Framework | FastAPI |
| Event Backbone | Apache Kafka (aiokafka) |
| Primary Store | PostgreSQL (asyncpg) |
| Cache / State | Redis |
| Vector Store | ChromaDB |
| LLM / Embeddings | Cohere Embed + Claude |
| Orchestration | Kubernetes (kind) + Terraform |
| Observability | Prometheus + Grafana |
| Testing | pytest + pytest-asyncio |

---

## Quick Start

### Prerequisites
- Docker with Compose plugin
- Python 3.11+
- uv (Python package manager)

### Setup

```bash
# Clone and enter project
git clone <repo-url>
cd telemetry-intelligence-platform

# Install dependencies
uv sync --dev

# Generate RSA Keys (first time only)
mkdir -p keys
openssl genrsa -out keys/private.pem 2048
openssl rsa -in keys/private.pem -pubout -out keys/public.pem

# Start the entire stack (infrastructure + all services)
docker compose up -d

# Verify all containers are healthy
docker compose ps
```

### Verify

```bash
# Check API health
curl http://localhost:8000/health

# Obtain a token
TOKEN=$(curl -s -X POST http://localhost:8000/api/v1/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "operator", "password": "operator123"}' | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# Query ingested telemetry
curl -H "Authorization: Bearer $TOKEN" "http://localhost:8000/api/v1/telemetry?limit=5"

# Query detected anomalies
curl -H "Authorization: Bearer $TOKEN" http://localhost:8000/api/v1/anomalies

# Test RBAC (viewer can't POST)
VIEWER_TOKEN=$(curl -s -X POST http://localhost:8000/api/v1/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "viewer", "password": "viewer123"}' | python -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

curl -X POST -H "Authorization: Bearer $VIEWER_TOKEN" http://localhost:8000/api/v1/telemetry
# Returns 403 Forbidden

# View API documentation (open in browser)
# Navigate to: http://localhost:8000/docs

# Prometheus targets (all UP)
# Open: http://localhost:9094/targets

# Grafana dashboards (admin/admin)
# Open: http://localhost:3000
```

### Local Development (with hot-reload)

For active development, run infrastructure in Docker but services locally for instant code reloading:

```bash
# Install dependencies
uv sync --dev

# Start infrastructure only (PostgreSQL + Kafka + Redis + ChromaDB)
docker compose up -d postgres zookeeper kafka kafka-init redis chromadb

# Start services individually (each in its own terminal)
PYTHONPATH=. uv run uvicorn services.api_gateway.app.main:app --reload --port 8000
PYTHONPATH=. uv run python -m services.ingestion_consumer.app.main
PYTHONPATH=. uv run python -m services.anomaly_detection.app.main
PYTHONPATH=. uv run python -m services.diagnosis_service.app.main

# Start simulator
PYTHONPATH=. uv run python -m services.simulator.app.main
```

### Run Tests

```bash
# Unit tests only — no infrastructure, no cost
PYTHONPATH=. uv run pytest -v -m "not integration and not manual"

# Integration tests (pipeline, cache, auth) — no LLM cost
PYTHONPATH=. uv run pytest -v -m "integration and not manual and not call_llm"

# Everything non-destructive and free
PYTHONPATH=. uv run pytest -v -m "not manual and not call_llm"

# Manual resilience tests (stop Redis/Kafka first)
# docker stop tip-redis tip-kafka
PYTHONPATH=. uv run pytest -v -m "resilience"
# docker start tip-redis tip-kafka

# Follow instructions on tests/integration/test_load.py

# LLM tests only — conscious cost decision
PYTHONPATH=. uv run pytest -v -m "call_llm"
```
---

## Kubernetes Deployment (kind)

Deploy the full stack to a local Kubernetes cluster using [kind](https://kind.sigs.k8s.io/).

### Prerequisites

- Docker
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

### Create Cluster

```bash
kind create cluster --name tip --config k8s/kind-config.yaml
```

### Build and Load Images

```bash
# Build application images
docker compose build

# Load into kind cluster
kind load docker-image telemetry-intel-platform-api-gateway:latest --name tip
kind load docker-image telemetry-intel-platform-ingestion-consumer:latest --name tip
kind load docker-image telemetry-intel-platform-anomaly-detection:latest --name tip
kind load docker-image telemetry-intel-platform-diagnosis-service:latest --name tip
```

### Deploy (in dependency order)

```bash
# 1. Namespace + configuration
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl create secret generic aws-credentials \
  --from-literal=AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id)" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key)" \
  --from-literal=AWS_SESSION_TOKEN="$(aws configure get aws_session_token)" \
  --namespace=tip \
  --dry-run=client -o yaml | kubectl apply -f -

# 2. JWT keys secret (not committed — generate your own keys first)
kubectl create secret generic jwt-keys \
  --from-file=private.pem=keys/private.pem \
  --from-file=public.pem=keys/public.pem \
  --namespace=tip

# 3. PostgreSQL init script
kubectl apply -f k8s/postgres-init-configmap.yaml

# 4. Infrastructure (postgres, redis, zookeeper, kafka, chromadb)
kubectl apply -f k8s/infrastructure.yaml
kubectl wait --for=condition=Ready pods --all -n tip --timeout=120s

# 5. Create Kafka topics (must wait for Kafka to be healthy)
kubectl apply -f k8s/kafka-init.yaml
kubectl wait --for=condition=Complete job/kafka-init -n tip --timeout=60s

# 6. Application services (depend on topics existing)
kubectl apply -f k8s/applications.yaml
kubectl wait --for=condition=Ready pods -l app=api-gateway -n tip --timeout=60s

# 7. Monitoring (Prometheus + Grafana)
kubectl apply -f k8s/grafana-dashboards-configmap.yaml
kubectl apply -f k8s/monitoring.yaml
```

### Verify

```bash
# All pods running
kubectl get pods -n tip

# API Gateway health check
curl http://localhost:30080/health

# Prometheus targets (all UP)
# Open: http://localhost:30094/targets

# Grafana dashboards (admin/admin)
# Open: http://localhost:30030

# Run simulator against K8s cluster
PYTHONPATH=. API_GATEWAY_URL=http://localhost:30080 uv run python -m services.simulator.app.main
```

### Exposed Ports

| Service | URL | NodePort |
|---------|-----|----------|
| API Gateway | http://localhost:30080 | 30080 |
| Grafana | http://localhost:30030 | 30030 |
| Prometheus | http://localhost:30094 | 30094 |

### HPA (Horizontal Pod Autoscaler)

The API Gateway scales on CPU utilization (target 70%):
```bash
kubectl get hpa -n tip
```

### Tear Down

```bash
kind delete cluster --name tip
```
---

## RAG Diagnosis Pipeline

The platform automatically generates root-cause diagnoses for detected anomalies using a Retrieval-Augmented Generation (RAG) pipeline powered by LLM.

### Architecture

```mermaid
flowchart TD
    KAFKA[anomalies.detected] --> DS[Diagnosis Service]
    DS -->|1. Context| PG[(PostgreSQL<br/>recent telemetry)]
    DS -->|2. Embed query| CLAUDE_E[Embedding Model]
    CLAUDE_E --> CHROMA[(ChromaDB<br/>top-5 chunks)]
    DS -->|3. Generate| CLAUDE_G[LLM]
    CHROMA --> DS
    DS -->|4. Persist| PG2[(PostgreSQL<br/>diagnoses table)]
    API[GET /anomalies/id/diagnosis] --> PG2
```

### Knowledge Base

The knowledge base contains 30 synthetic incident reports covering failure modes: thermal management, environmental factors, firmware bugs, hardware degradation, network issues, security incidents, and configuration errors.

**Ingesting knowledge (admin only):**

```bash
# Single incident
curl -X POST http://localhost:8000/api/v1/knowledge/ingest \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d @data/incidents/001_thermal_runaway_edge_gateway.json

# Batch ingest all incidents
PYTHONPATH=. python scripts/ingest_incidents.py
```

**Incident document format:**

```json
{
  "title": "Short descriptive title of the incident",
  "description": "Detailed description of what was observed...",
  "affected_device_types": ["temperature_sensor", "edge_gateway"],
  "root_cause": "Explanation of the root cause...",
  "resolution_steps": ["Step 1", "Step 2", "Step 3"],
  "severity": "low|medium|high|critical",
  "failure_category": "thermal_management|environmental|firmware_bug|...",
  "tags": ["relevant", "search", "tags"]
}
```

### Diagnosis Output
When an anomaly is detected, the Diagnosis Service automatically generates a structured diagnosis (typically within 20-30 seconds):

```json
{
  "root_cause_summary": "2-3 sentence explanation of the most likely root cause",
  "confidence_score": 0.72,
  "supporting_evidence": ["evidence point 1", "evidence point 2"],
  "recommended_actions": ["action 1", "action 2"],
  "retrieved_incident_ids": ["uuid1", "uuid2"]
}
```

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CLAUDE_MODEL_ID` | — | generation model (Claude Haiku 4.5) |
| `CLAUDE_EMBEDDING_MODEL_ID` | — | embedding model (Cohere Embed v4) |
| `EMBEDDING_PROVIDER` | `bedrock` | Use `local` for ChromaDB's built-in model |
| `CHROMA_HOST` | `localhost` | ChromaDB hostname |
| `CHROMA_PORT` | `8100` | ChromaDB port (host-mapped; internal is 8000) |

### Running Without Claude Credentials

Set `EMBEDDING_PROVIDER=local` in `.env` to use ChromaDB's built-in embedding model (all-MiniLM-L6-v2, 384 dimensions) for knowledge ingestion and retrieval. This removes the Claude dependency for the embedding step only — the diagnosis generation step (Claude Haiku 4.5) still requires Claude credentials. Without Claude credentials, incidents can be ingested and retrieved semantically, but automated diagnoses will not be generated.

**Note**: You cannot mix embedding providers — if you ingest with `local`, you must query with `local`. Choose one provider per deployment and re-ingest if you switch.

---

## Project Structure

```
telemetry-intelligence-platform/
├── docker-compose.yml          # Full stack (PostgreSQL, Kafka, Redis, ChromaDB, apps, monitoring)
├── pyproject.toml              # Python dependencies
├── .env                        # Environment configuration
├── shared/                     # Shared code (models, utilities)
│ ├── logging_config.py
│ ├── metrics.py
│ └── models/
│ └── models.py
├── services/
│ ├── api_gateway/              # FastAPI REST API (auth, RBAC, rate limiting, cache-aside)
│ ├── ingestion_consumer/       # Kafka → Redis enrichment → PostgreSQL
│ ├── anomaly_detection/        # Rolling window z-score detection
│ ├── diagnosis_service         # RAG pipeline (context → retrieval → generation)
│ └── simulator/                # Telemetry data generator with anomaly injection
├── data/
│ └── incidents/                # 30 synthetic incident reports (JSON) for RAG knowledge base
├── scripts/
│ ├── generate_incidents.py     # Generate synthetic incident files
│ └── ingest_incidents.py       # Batch ingest incidents into knowledge base
├── db/
│ └── init.sql                  # PostgreSQL schema (devices, telemetry_events, anomalies, diagnoses, incidents_knowledge)
├── k8s/                        # Kubernetes manifests (kind)
├── terraform/                  # AWS production infrastructure (EKS, RDS, ElastiCache, MSK, ECR)
├── monitoring/ # Prometheus config + Grafana dashboards
├── tests/
│ ├── unit/                     # Pure validation tests
│ └── integration/              # Full pipeline + RAG loop tests
├──  docs/
│ └── decisions.md                # Architecture decision log
```
---

## API Endpoints

| Method | Endpoint | Role | Description |
|--------|----------|------|-------------|
| POST | `/api/v1/auth/token` | — | Obtain a JWT token |
| POST | `/api/v1/devices` | operator | Register a new device |
| GET | `/api/v1/devices` | viewer | List registered devices |
| POST | `/api/v1/telemetry` | operator | Ingest telemetry event (→ Kafka, returns 202) |
| GET | `/api/v1/telemetry` | viewer | Query historical telemetry with filters (cached, 60s TTL) |
| GET | `/api/v1/anomalies` | viewer | Query detected anomalies (filter by device, severity, time range) |
| GET | `/api/v1/anomalies/{id}` | viewer | Retrieve a single anomaly by ID |
| GET | `/api/v1/anomalies/{id}/diagnosis` | viewer | Retrieve RAG-generated diagnosis (or pending status) |
| PATCH | `/api/v1/anomalies/{id}/status` | operator | Update anomaly status (open, acknowledged, resolved) |
| POST | `/api/v1/knowledge/ingest` | admin | Ingest an incident document into the knowledge base |
| GET | `/api/v1/knowledge/incidents` | viewer | List ingested incident documents with filters |
| GET | `/health` | — | Liveness check |
| GET | `/ready` | — | Readiness check (verifies dependencies) |
| GET | `/api/v1/telemetry/dlq` | operator | Query dead-letter queue (failed validation events) |

Full interactive API docs available at `/docs` when the API is running.

---

## Authentication

The API uses JWT tokens (RS256) for authentication. All endpoints except `/health`, `/docs`, and `/api/v1/auth/token` require a valid token.

### Obtaining a Token

```bash
curl -X POST http://localhost:8000/api/v1/auth/token \
  -H "Content-Type: application/json" \
  -d '{"username": "operator", "password": "operator123"}'
```

### Using a Token

Include the token in the Authorization header:

```bash
curl -H "Authorization: Bearer <token>" http://localhost:8000/api/v1/telemetry
```

### Available Test Accounts

| Username | Password | Role | Access |
|----------|----------|------|--------|
| admin | admin123 | admin | Full access (all endpoints + knowledge ingestion) |
| operator | operator123 | operator | Read all + write telemetry + register devices + update anomaly status |
| viewer | viewer123 | viewer | Read-only (GET endpoints only) |

### Token Lifetime

Tokens expire after 60 minutes (configurable via `JWT_EXPIRY_MINUTES`). There is no refresh token mechanism — request a new token when the current one expires.

### Rate Limits

- Telemetry ingestion (POST): 1000 requests/minute per user
- Query endpoints (GET): 100 requests/minute per user
- Exceeded limits return 429 with a `Retry-After` header

