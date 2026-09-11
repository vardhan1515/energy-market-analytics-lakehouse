"""Open-Meteo historical hourly weather ingestion."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx

from ..config import Settings
from ..constants import WEATHER_LOCATIONS, WEATHER_VARIABLES
from ..landing import atomic_json, new_batch_id
from .http import IngestionError, get_json


def ingest_weather(
    start_date: date,
    end_date: date,
    settings: Settings,
    batch_id: str | None = None,
) -> Path:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    batch_id = batch_id or new_batch_id()
    destination = settings.raw_data_dir / "open_meteo" / batch_id
    if destination.exists():
        raise IngestionError(f"Batch already exists: {destination}")
    manifest: dict[str, Any] = {
        "source": "Open-Meteo Historical Weather API",
        "batch_id": batch_id,
        "retrieved_at_utc": datetime.now(UTC).isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "files": [],
    }
    with httpx.Client(
        base_url=settings.open_meteo_api_base_url, timeout=90, follow_redirects=True
    ) as client:
        for location, (latitude, longitude) in WEATHER_LOCATIONS.items():
            params = {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "hourly": ",".join(WEATHER_VARIABLES),
                "timezone": "UTC",
            }
            payload = get_json(client, "/v1/archive", params=params)
            if not isinstance(payload, dict) or not payload.get("hourly", {}).get("time"):
                raise IngestionError(f"Open-Meteo returned no hourly data for {location}")
            name = f"{location}.json"
            checksum = atomic_json(destination / name, payload)
            manifest["files"].append({"location": location, "file": name, "sha256": checksum})
    atomic_json(destination / "manifest.json", manifest)
    return destination
