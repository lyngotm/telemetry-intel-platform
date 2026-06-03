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
