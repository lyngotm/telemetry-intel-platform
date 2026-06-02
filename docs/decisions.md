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

