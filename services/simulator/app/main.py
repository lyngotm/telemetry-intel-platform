"""
Telemetry Simulator — generates realistic device readings and POSTs them to the API Gateway.

Generates data for 10 devices across 4 metric types using normal distributions.
Supports configurable anomaly injection for testing the detection pipeline.
"""

import asyncio
import logging
import os
import signal
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


async def obtain_token(client: httpx.AsyncClient, config: dict) -> str:
    """
    Authenticate with the API Gateway and return a JWT access token.
    Uses operator credentials since the simulator needs to POST telemetry.
    """
    auth_url = "/api/v1/auth/token"
    auth_username = os.environ.get("AUTH_USERNAME", config.get("auth_username", "operator"))
    auth_password = os.environ.get("AUTH_PASSWORD", config.get("auth_password", "operator123"))
    credentials = {
        "username": auth_username,
        "password": auth_password,
    }

    response = await client.post(auth_url, json=credentials)

    if response.status_code == 200:
        token = response.json()["access_token"]
        logger.info(
            f"Authenticated as '{credentials['username']}' "
            f"(expires in {response.json()['expires_in']}s)"
        )
        return token
    else:
        logger.error(f"Authentication failed: {response.status_code} {response.text}")
        sys.exit(1)


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
    api_url = os.environ.get("API_URL", config["api_url"])

    send_interval = config["send_interval"]
    metrics_config = config["metrics"]
    anomaly_config = config["anomaly"]

    # httpx.AsyncClient as a context manager maintains an internal connection pool —
    # it reuses TCP connections across requests instead of opening a new one each time,
    # which is significantly faster when sending hundreds of events per batch.
    async with httpx.AsyncClient(base_url=api_url, timeout=30.0) as client:
        token = await obtain_token(client, config)
        client.headers["Authorization"] = f"Bearer {token}"

        # Register devices...
        logger.info(f"Registering {config['devices_count']} devices...")
        device_ids = await register_devices(client, config["devices_count"])

        if not device_ids:
            logger.error("No devices registered. Is the API running?")
            sys.exit(1)

        logger.info(f"Starting telemetry generation. Interval: {send_interval}s")

        # Graceful shutdown via signal handler
        shutdown_event = asyncio.Event()

        def handle_shutdown():
            shutdown_event.set()

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, handle_shutdown)
        loop.add_signal_handler(signal.SIGTERM, handle_shutdown)

        start_time = time.time()
        events_sent = 0
        events_failed = 0

        while not shutdown_event.is_set():
            elapsed = time.time() - start_time
            batch_tasks = []

            for device_idx, device_id in enumerate(device_ids):
                for metric_type, metric_cfg in metrics_config.items():
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
                + (
                    " | ANOMALY ACTIVE"
                    if anomaly_config["enabled"] and elapsed > anomaly_config["start_after_seconds"]
                    else ""
                )
            )

            # Use wait_for so shutdown_event can interrupt the sleep
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=send_interval)
            except asyncio.TimeoutError:
                pass  # Normal — timeout means "keep going"

        logger.info(f"Simulator stopped. Total events sent: {events_sent}, failed: {events_failed}")

        await print_anomaly_summary(client, device_ids, config)


async def send_event(client: httpx.AsyncClient, payload: dict) -> None:
    """Send a single telemetry event to the API."""
    response = await client.post("/api/v1/telemetry", json=payload)
    if response.status_code != 202:
        raise Exception(f"API returned {response.status_code}: {response.text}")


async def print_anomaly_summary(client: httpx.AsyncClient, device_ids: list[UUID], config: dict):
    """
    After simulation ends, query the anomalies API to show what was detected.
    Provides a quick verification that the detection pipeline is working.
    """
    anomaly_config = config["anomaly"]
    if not anomaly_config["enabled"]:
        return

    logger.info("--- Anomaly Injection Summary ---")
    anomaly_device_id = device_ids[anomaly_config["device_index"]]
    logger.info(f"Injected anomalies on device: {anomaly_device_id}")
    logger.info(
        f"Metric: {anomaly_config['metric_type']}, Deviation: {anomaly_config['deviation_multiplier']}σ"
    )

    # Query the anomalies API for this device
    try:
        response = await client.get(
            "/api/v1/anomalies",
            params={"device_id": str(anomaly_device_id)},
        )
        if response.status_code == 200:
            data = response.json()
            count = data.get("count", 0)
            anomalies = data.get("data", [])
            logger.info(f"Anomalies detected: {count}")
            for a in anomalies[:5]:  # Show first 5
                logger.info(
                    f"  [{a['severity'].upper()}] value={a['observed_value']}, "
                    f"z_score={a['z_score']:.2f}, detected_at={a['detected_at']}"
                )
            if count > 5:
                logger.info(f"  ... and {count - 5} more")
        else:
            logger.warning(f"Could not fetch anomalies: {response.status_code}")
    except Exception as e:
        logger.warning(f"Anomaly summary failed (API may be down): {e}")

    logger.info("--- End Summary ---")


if __name__ == "__main__":
    asyncio.run(run_simulator())
