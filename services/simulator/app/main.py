"""
Telemetry Simulator — generates realistic device readings and POSTs them to the API Gateway.

Generates data for 10 devices across 4 metric types using normal distributions.
Supports configurable anomaly injection for testing the detection pipeline.
"""

import asyncio
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import httpx
import numpy as np
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("simulator")


def load_config() -> dict:
    """Load simulator configuration from YAML file."""
    config_path = Path(__file__).parent.parent / "config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


async def register_devices(client: httpx.AsyncClient, count: int) -> list[UUID]:
    """
    Register simulated devices with the API.
    Returns list of device_ids to use for telemetry generation.
    """
    device_ids = []
    device_types = ["temperature_sensor", "environmental_monitor", "server_node", "edge_gateway"]

    for i in range(count):
        payload = {
            "device_name": f"sim-device-{i:03d}",
            "device_type": device_types[i % len(device_types)],
            "location": f"Zone-{chr(65 + i % 4)}, Rack-{i // 4 + 1}",
            "firmware_version": f"v1.{i % 3}.0",
        }

        response = await client.post("/api/v1/devices", json=payload)

        if response.status_code == 201:
            data = response.json()
            device_id = UUID(data["data"]["device_id"])
            device_ids.append(device_id)
            logger.info(f"Registered device: {payload['device_name']} ({device_id})")
        elif response.status_code == 409:
            # Device already exists — this handles re-runs
            logger.info(f"Device {payload['device_name']} already registered, skipping")
        else:
            logger.error(f"Failed to register device: {response.status_code} {response.text}")
            sys.exit(1)

    return device_ids


def generate_reading(
    metric_config: dict,
    is_anomaly: bool,
    deviation_multiplier: float,
) -> float:
    """
    Generate a single metric reading using a normal distribution.
    If is_anomaly is True, shifts the value by deviation_multiplier * stddev.
    """
    mean = metric_config["mean"]
    stddev = metric_config["stddev"]

    # Normal reading from the distribution
    value = np.random.normal(mean, stddev)

    if is_anomaly:
        # Shift the value significantly above normal
        value = mean + (deviation_multiplier * stddev)
        # Add a small random component so it's not perfectly constant
        value += np.random.normal(0, stddev * 0.2)

    return round(float(value), 2)


async def run_simulator():
    """Main simulator loop."""
    config = load_config()
    api_url = config["api_url"]
    send_interval = config["send_interval"]
    metrics_config = config["metrics"]
    anomaly_config = config["anomaly"]

    # httpx.AsyncClient as a context manager maintains an internal connection pool —
    # it reuses TCP connections across requests instead of opening a new one each time,
    # which is significantly faster when sending hundreds of events per batch.
    async with httpx.AsyncClient(base_url=api_url, timeout=30.0) as client:
        # Step 1: Register devices
        logger.info(f"Registering {config['devices_count']} devices...")
        device_ids = await register_devices(client, config["devices_count"])

        if not device_ids:
            logger.error("No devices registered. Is the API running?")
            sys.exit(1)

        logger.info(f"Starting telemetry generation. Interval: {send_interval}s")
        logger.info(f"Anomaly injection: {'ENABLED' if anomaly_config['enabled'] else 'DISABLED'}")

        start_time = time.time()
        events_sent = 0
        events_failed = 0

        # Step 2: Generate and send telemetry in a loop
        try:
            while True:
                elapsed = time.time() - start_time
                batch_tasks = []

                for device_idx, device_id in enumerate(device_ids):
                    for metric_type, metric_cfg in metrics_config.items():
                        # Determine if this reading should be anomalous
                        is_anomaly = (
                            anomaly_config["enabled"]
                            and device_idx == anomaly_config["device_index"]
                            and metric_type == anomaly_config["metric_type"]
                            and elapsed > anomaly_config["start_after_seconds"]
                        )

                        value = generate_reading(
                            metric_cfg,
                            is_anomaly=is_anomaly,
                            deviation_multiplier=anomaly_config.get("deviation_multiplier", 4.5),
                        )

                        payload = {
                            "device_id": str(device_id),
                            "metric_type": metric_type,
                            "value": value,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "metadata": {"unit": metric_cfg["unit"]},
                        }

                        batch_tasks.append(send_event(client, payload))

                # Send all events in this batch concurrently
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        events_failed += 1
                    else:
                        events_sent += 1

                logger.info(
                    f"Batch sent: {len(batch_tasks)} events | "
                    f"Total sent: {events_sent} | Failed: {events_failed} | "
                    f"Elapsed: {elapsed:.0f}s"
                    + (" | ANOMALY ACTIVE" if anomaly_config["enabled"] and elapsed > anomaly_config["start_after_seconds"] else "")
                )

                await asyncio.sleep(send_interval)

        except KeyboardInterrupt:
            logger.info(
                f"Simulator stopped. Total events sent: {events_sent}, failed: {events_failed}"
            )


async def send_event(client: httpx.AsyncClient, payload: dict) -> None:
    """Send a single telemetry event to the API."""
    response = await client.post("/api/v1/telemetry", json=payload)
    if response.status_code != 202:
        raise Exception(f"API returned {response.status_code}: {response.text}")


if __name__ == "__main__":
    asyncio.run(run_simulator())
