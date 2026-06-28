"""
Rolling window state management using Redis sorted sets.

Key pattern: window:{device_id}:{metric_type}
Score: event timestamp (Unix seconds, float for sub-second precision)
Member: "{value}:{timestamp}" — timestamp suffix guarantees uniqueness

The window maintains the last N seconds of readings (configurable via
settings.window_size_seconds) and provides statistical summaries for
z-score computation.
"""

import logging
from dataclasses import dataclass

import redis.asyncio as aioredis

from services.anomaly_detection.app.config import settings

logger = logging.getLogger("anomaly_detection")


@dataclass
class WindowStats:
    """Statistical summary of the current rolling window."""

    mean: float
    stddev: float
    count: int
    min_value: float
    max_value: float


class RollingWindow:
    """Manages per-device, per-metric rolling windows in Redis sorted sets."""

    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client
        self.window_size = settings.window_size_seconds
        self.min_samples = settings.min_window_samples

    async def add_and_get_stats(
        self, device_id: str, metric_type: str, value: float, timestamp: float
    ) -> WindowStats | None:
        """
        Add a new reading to the rolling window and return current statistics.

        Returns None if the window has fewer than min_samples readings
        (not enough data to compute meaningful statistics).

        Args:
            device_id: UUID string of the device
            metric_type: Metric name (e.g., "temperature", "cpu_usage")
            value: The observed measurement
            timestamp: Unix timestamp of the reading (seconds, float)

        Returns:
            WindowStats with mean/stddev/count, or None if insufficient data
        """
        key = f"window:{device_id}:{metric_type}"

        # Member format: "value:timestamp" ensures uniqueness even for repeated values
        member = f"{value}:{timestamp}"

        # Calculate the cutoff: anything older than (now - window_size) gets trimmed
        cutoff = timestamp - self.window_size

        # Execute all three operations in a Redis pipeline for efficiency.
        # A pipeline batches commands into a single round-trip to Redis,
        # reducing network latency from 3 RTTs to 1.
        pipe = self.redis.pipeline()
        pipe.zadd(key, {member: timestamp})
        pipe.zremrangebyscore(key, "-inf", cutoff)
        pipe.zrangebyscore(key, "-inf", "+inf")
        # Set a TTL on the key so windows for inactive devices don't persist forever.
        # TTL is 2x window size — if no new data arrives within that time, the key expires.
        pipe.expire(key, self.window_size * 2)

        results = await pipe.execute()

        # results[0] = ZADD response (number of new elements added)
        # results[1] = ZREMRANGEBYSCORE response (number of elements removed)
        # results[2] = ZRANGEBYSCORE response (list of members in the window)
        # results[3] = EXPIRE response (True/False)
        members = results[2]

        # Parse values from the "value:timestamp" member format
        values = self._parse_members(members)

        if len(values) < self.min_samples:
            logger.debug(
                f"Window {key}: only {len(values)} samples "
                f"(need {self.min_samples}), skipping analysis"
            )
            return None

        # Compute statistics
        count = len(values)
        mean = sum(values) / count
        variance = sum((v - mean) ** 2 for v in values) / count
        stddev = variance**0.5

        return WindowStats(
            mean=mean,
            stddev=stddev,
            count=count,
            min_value=min(values),
            max_value=max(values),
        )

    @staticmethod
    def _parse_members(members: list[str]) -> list[float]:
        """
        Parse sorted set members back into float values.
        Member format is "value:timestamp" — we split on the last colon
        to handle negative values correctly (e.g., "-3.5:1717350010").
        """
        values = []
        for member in members:
            value_str = member.split(":", 1)[0]
            try:
                values.append(float(value_str))
            except ValueError:
                # Skip malformed entries — shouldn't happen, but defensive
                continue
        return values
