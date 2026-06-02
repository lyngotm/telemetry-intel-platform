# Telemetry Intelligence Platform

A real-time telemetry ingestion and analysis platform with AI-powered anomaly detection and root-cause diagnosis via a RAG pipeline.

## Overview

This platform collects streaming device telemetry data, detects anomalies using statistical methods (z-score on rolling windows), and generates AI-powered root-cause diagnoses by retrieving semantically similar historical incidents and passing them to an LLM.

The system follows an event-driven microservices architecture with Apache Kafka as the central event backbone, separating the ingestion path (write-heavy, high-throughput) from the query path (read-heavy, low-latency) and the intelligence layer (compute-heavy, async).

## Architecture

```mermaid
flowchart LR
    subgraph Clients
        SIM[Telemetry Simulator]
        USER[User / Dashboard]
    end

    subgraph API Gateway
        API[FastAPI<br/>POST /telemetry<br/>GET /telemetry<br/>GET /anomalies]
    end

    subgraph Kafka
        RAW[telemetry.raw]
        ENRICHED[telemetry.enriched]
        DLQ[telemetry.dlq]
        ANOMALIES[anomalies.detected]
    end

    subgraph Consumers
        IC[Ingestion Consumer]
        AD[Anomaly Detection<br/>Service]
        DS[Diagnosis Service<br/>Week 5]
    end

    subgraph Data Stores
        PG[(PostgreSQL)]
        REDIS[(Redis<br/>Week 2)]
        CHROMA[(ChromaDB<br/>Week 5)]
    end

    SIM -->|POST /api/v1/telemetry| API
    USER -->|GET queries| API
    API -->|publish| RAW
    API -->|query| PG

    RAW --> IC
    IC -->|validate + persist| PG
    IC -->|publish enriched| ENRICHED
    IC -->|validation failure| DLQ

    ENRICHED --> AD
    AD -->|rolling windows| REDIS
    AD -->|anomaly record| PG
    AD -->|publish| ANOMALIES

    ANOMALIES --> DS
    DS -->|retrieve context| PG
    DS -->|device metadata| REDIS
    DS -->|semantic search| CHROMA
    DS -->|write diagnosis| PG
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| API Framework | FastAPI |
| Event Backbone | Apache Kafka (aiokafka) |
| Primary Store | PostgreSQL (asyncpg) |
| Cache / State | Redis (Week 2+) |
| Vector Store | ChromaDB (Week 5) |
| LLM / Embeddings | AWS Bedrock — Titan + Claude (Week 5) |
| Orchestration | Kubernetes (kind) + Terraform |
| Observability | Prometheus + Grafana (Week 4) |
| Testing | pytest + pytest-asyncio |

## Current Status

**Week 1 ✅ — Core Ingestion Pipeline**

Working end-to-end data flow:
- Simulator generates telemetry for 10 devices across 4 metric types
- API Gateway validates and publishes to Kafka
- Ingestion Consumer persists to PostgreSQL (partitioned by month)
- Dead-letter queue handles malformed events
- GET endpoint serves historical data with filtering

## Quick Start

### Prerequisites
- Docker with Compose plugin
- Python 3.10+
- uv (Python package manager)

### Setup

```bash
# Clone and enter project
git clone <repo-url>
cd telemetry-intelligence-platform

# Install dependencies
uv sync --dev

# Start infrastructure (PostgreSQL + Kafka)
docker compose up -d

# Start the API Gateway (terminal 1)
PYTHONPATH=. uvicorn services.api_gateway.app.main:app --reload --port 8000

# Start the Ingestion Consumer (terminal 2)
PYTHONPATH=. python -m services.ingestion_consumer.app.main

# Run the simulator (terminal 3)
PYTHONPATH=. python -m services.simulator.app.main
```

### Verify

```bash
# Check API health
curl http://localhost:8000/health

# Query ingested telemetry
curl "http://localhost:8000/api/v1/telemetry?limit=5"

# View API documentation
open http://localhost:8000/docs
```

### Run Tests

```bash
# Unit tests (no infrastructure needed)
PYTHONPATH=. uv run pytest tests/unit/ -v

# Integration tests (requires Docker services + API + Consumer running)
PYTHONPATH=. uv run pytest tests/integration/ -v
```

## Project Structure

```
telemetry-intelligence-platform/
├── docker-compose.yml          # Infrastructure (PostgreSQL, Kafka, Zookeeper)
├── pyproject.toml              # Python dependencies
├── shared/                     # Shared code (models, utilities)
│   └── models/
├── services/
│   ├── api_gateway/            # FastAPI REST API
│   ├── ingestion_consumer/     # Kafka → PostgreSQL consumer
│   └── simulator/              # Telemetry data generator
├── db/
│   └── init.sql                # PostgreSQL schema
├── tests/
│   ├── unit/                   # Pure validation tests
│   └── integration/            # Full pipeline tests
└── docs/
    └── decisions.md            # Architecture decision log
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/devices` | Register a new device |
| GET | `/api/v1/devices` | List registered devices |
| POST | `/api/v1/telemetry` | Ingest telemetry event (→ Kafka, returns 202) |
| GET | `/api/v1/telemetry` | Query historical telemetry with filters |
| GET | `/health` | Liveness check |

Full interactive API docs available at `/docs` when the API is running.


