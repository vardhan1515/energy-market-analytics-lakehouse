"""Historical MISO actual-load and published-forecast ingestion."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx

from ..config import Settings
from ..constants import MISO_HISTORY_ENDPOINTS
from ..landing import atomic_json, new_batch_id
from ..validation import validate_history_payload
from .http import IngestionError, get_json


def _authenticated_headers(settings: Settings) -> dict[str, str]:
    if not settings.miso_api_token:
        raise IngestionError("MISO_API_TOKEN is required for historical ingestion")
    return {
        "Accept": "application/json",
        "Ocp-Apim-Subscription-Key": settings.miso_api_token,
        "User-Agent": "energy-market-analytics-lakehouse/0.1",
    }


def _fetch_pages(client: httpx.Client, endpoint: str, dataset: str) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    page_number = 0
    while True:
        payload = get_json(
            client, endpoint, params={"pageNumber": page_number} if page_number else None
        )
        if not isinstance(payload, dict):
            raise IngestionError("MISO history response must be a JSON object")
        validate_history_payload(payload, dataset)
        pages.append(payload)
        page = payload.get("page") or {}
        if not page:
            break
        last_page = page.get("lastPage")
        current = page.get("pageNumber")
        total = page.get("totalPages")
        if last_page is True or (
            isinstance(current, int) and isinstance(total, int) and current >= total - 1
        ):
            break
        if not isinstance(current, int):
            raise IngestionError("MISO pagination omitted a numeric pageNumber")
        page_number = current + 1
        if page_number > 1000:
            raise IngestionError("MISO pagination exceeded the safety limit")
    return pages


def check_history_access(
    market_date: date,
    settings: Settings,
    dataset: str = "actual_load",
) -> dict[str, str | int]:
    """Make one authenticated request and return only non-sensitive diagnostics."""
    try:
        endpoint = MISO_HISTORY_ENDPOINTS[dataset].format(date=market_date.isoformat())
    except KeyError as exc:
        raise ValueError(f"Unsupported MISO history dataset: {dataset}") from exc

    with httpx.Client(
        base_url=settings.miso_api_base_url,
        headers=_authenticated_headers(settings),
        timeout=60,
    ) as client:
        payload = get_json(client, endpoint)
    if not isinstance(payload, dict):
        raise IngestionError("MISO history response must be a JSON object")
    validate_history_payload(payload, dataset)
    return {
        "dataset": dataset,
        "market_date": market_date.isoformat(),
        "endpoint": endpoint,
        "first_page_rows": len(payload["data"]),
    }


def ingest_history(market_date: date, settings: Settings, batch_id: str | None = None) -> Path:
    batch_id = batch_id or new_batch_id()
    destination = settings.raw_data_dir / "miso_history" / market_date.isoformat() / batch_id
    if destination.exists():
        raise IngestionError(f"Batch already exists: {destination}")

    retrieved_at = datetime.now(UTC).isoformat()
    manifest: dict[str, Any] = {
        "source": "MISO Data Exchange",
        "market_date": market_date.isoformat(),
        "batch_id": batch_id,
        "retrieved_at_utc": retrieved_at,
        "datasets": {},
    }
    headers = _authenticated_headers(settings)
    with httpx.Client(base_url=settings.miso_api_base_url, headers=headers, timeout=60) as client:
        for dataset, template in MISO_HISTORY_ENDPOINTS.items():
            endpoint = template.format(date=market_date.isoformat())
            pages = _fetch_pages(client, endpoint, dataset)
            files = []
            for number, payload in enumerate(pages, 1):
                name = f"{dataset}_page_{number:04d}.json"
                checksum = atomic_json(destination / name, payload)
                files.append({"file": name, "rows": len(payload["data"]), "sha256": checksum})
            manifest["datasets"][dataset] = {"endpoint": endpoint, "files": files}
    atomic_json(destination / "manifest.json", manifest)
    return destination
