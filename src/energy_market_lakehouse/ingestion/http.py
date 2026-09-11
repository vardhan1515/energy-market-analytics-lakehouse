"""Shared HTTP behavior with bounded exponential retry."""

from __future__ import annotations

import time
from typing import Any

import httpx


class IngestionError(RuntimeError):
    pass


def get_json(
    client: httpx.Client,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    attempts: int = 4,
) -> Any:
    for attempt in range(attempts):
        try:
            response = client.get(path, params=params)
            if response.status_code == 429 or response.status_code >= 500:
                response.raise_for_status()
            response.raise_for_status()
            return response.json()
        except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPStatusError) as exc:
            retryable = not isinstance(exc, httpx.HTTPStatusError) or (
                exc.response.status_code == 429 or exc.response.status_code >= 500
            )
            if attempt + 1 == attempts or not retryable:
                raise IngestionError(f"GET {path} failed: {exc}") from exc
            time.sleep(min(2**attempt, 30))
        except ValueError as exc:
            raise IngestionError(f"GET {path} returned invalid JSON") from exc
    raise AssertionError("retry loop exhausted")
