"""Capture one immutable batch from MISO public near-real-time APIs."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from ..config import Settings
from ..constants import MISO_PUBLIC_ENDPOINTS
from ..landing import atomic_json, new_batch_id
from .http import IngestionError, get_json


def ingest_realtime(settings: Settings, batch_id: str | None = None) -> Path:
    retrieved_at = datetime.now(UTC)
    batch_id = batch_id or new_batch_id(retrieved_at)
    destination = settings.raw_data_dir / "miso_realtime" / batch_id
    if destination.exists():
        raise IngestionError(f"Batch already exists: {destination}")
    manifest: dict[str, Any] = {
        "source": "MISO public real-time APIs",
        "batch_id": batch_id,
        "retrieved_at_utc": retrieved_at.isoformat(),
        "files": [],
    }
    with httpx.Client(
        base_url=settings.miso_public_api_base_url, timeout=60, follow_redirects=True
    ) as client:
        for dataset, endpoint in MISO_PUBLIC_ENDPOINTS.items():
            payload = get_json(client, endpoint)
            name = f"{dataset}.json"
            checksum = atomic_json(destination / name, payload)
            manifest["files"].append(
                {"dataset": dataset, "endpoint": endpoint, "file": name, "sha256": checksum}
            )
    atomic_json(destination / "manifest.json", manifest)
    return destination
