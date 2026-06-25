"""
Batch ingest all incident documents from data/incidents/ into the knowledge base.

Usage:
    PYTHONPATH=. python scripts/ingest_incidents.py

docker compose up -d postgres redis chromadb zookeeper kafka kafka-init
Requires the API Gateway to be running (local dev or docker-compose).
Authenticates as admin, then POSTs each incident JSON file to the ingest endpoint.
"""

import json
import os
import sys
import time

import httpx

API_BASE_URL = os.getenv("API_URL", "http://localhost:8000")
INCIDENTS_DIR = "data/incidents"


def get_admin_token(client: httpx.Client) -> str:
    """Authenticate as admin and return the access token."""
    response = client.post(
        f"{API_BASE_URL}/api/v1/auth/token",
        json={"username": "admin", "password": "admin123"},
    )
    response.raise_for_status()
    return response.json()["access_token"]


def ingest_incident(client: httpx.Client, filepath: str) -> dict:
    """POST a single incident file to the ingest endpoint."""
    with open(filepath) as f:
        incident_data = json.load(f)

    response = client.post(
        f"{API_BASE_URL}/api/v1/knowledge/ingest",
        json=incident_data,
    )
    response.raise_for_status()
    return response.json()


def main():
    # Discover incident files
    if not os.path.isdir(INCIDENTS_DIR):
        print(f"ERROR: Directory '{INCIDENTS_DIR}' not found.")
        sys.exit(1)

    files = sorted(f for f in os.listdir(INCIDENTS_DIR) if f.endswith(".json"))
    if not files:
        print(f"ERROR: No JSON files found in '{INCIDENTS_DIR}'.")
        sys.exit(1)

    print(f"Found {len(files)} incident files in {INCIDENTS_DIR}/")
    print(f"API: {API_BASE_URL}")
    print("-" * 60)

    # Authenticate
    client = httpx.Client(timeout=60.0)  # Longer timeout for embedding generation
    token = get_admin_token(client)
    client.headers["Authorization"] = f"Bearer {token}"
    print("Authenticated as admin.\n")

    # Ingest each file
    success_count = 0
    fail_count = 0
    total_chunks = 0
    start_time = time.time()

    for i, filename in enumerate(files, 1):
        filepath = os.path.join(INCIDENTS_DIR, filename)
        try:
            result = ingest_incident(client, filepath)
            data = result["data"]
            chunks = data["chunk_count"]
            total_chunks += chunks
            success_count += 1
            print(f"  [{i:02d}/{len(files)}] ✓ {filename} → {chunks} chunk(s)")
        except httpx.HTTPStatusError as e:
            fail_count += 1
            print(f"  [{i:02d}/{len(files)}] ✗ {filename} → {e.response.status_code}: {e.response.text}")
        except Exception as e:
            fail_count += 1
            print(f"  [{i:02d}/{len(files)}] ✗ {filename} → {e}")

    elapsed = time.time() - start_time
    print("-" * 60)
    print(f"Done in {elapsed:.1f}s")
    print(f"  Ingested: {success_count}/{len(files)}")
    print(f"  Failed:   {fail_count}")
    print(f"  Total chunks in ChromaDB: {total_chunks}")

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
