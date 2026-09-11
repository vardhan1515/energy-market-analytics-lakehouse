"""Pure-Python source-contract checks used before Spark is available."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from typing import Any

from .constants import EXPECTED_REGIONS, EXPECTED_ZONES


class SourceValidationError(ValueError):
    """A source response does not satisfy its minimum contract."""


def validate_history_payload(payload: Mapping[str, Any], dataset: str) -> int:
    value_field = {"actual_load": "load", "load_forecast": "loadForecast"}.get(dataset)
    if value_field is None:
        raise ValueError(f"Unsupported history dataset: {dataset}")
    records = payload.get("data")
    if not isinstance(records, list) or not records:
        raise SourceValidationError("MISO response field 'data' must be a non-empty list")
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise SourceValidationError(f"Record {index} is not an object")
        interval = record.get("timeInterval")
        if not isinstance(interval, Mapping) or not interval.get("start"):
            raise SourceValidationError(f"Record {index} has no interval start")
        value = record.get(value_field)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise SourceValidationError(f"Record {index} has no numeric {value_field}")
        if not math.isfinite(float(value)) or value < 0:
            raise SourceValidationError(f"Record {index} has an invalid {value_field}")
    return len(records)


def ordinary_day_is_complete(
    actual_records: Sequence[Mapping[str, Any]],
    forecast_records: Sequence[Mapping[str, Any]],
) -> bool:
    """Validate a non-DST 24-hour day at the source grain."""
    actual_keys = {
        (r.get("timeInterval", {}).get("start"), str(r.get("region", "")).upper())
        for r in actual_records
    }
    forecast_keys = {
        (
            r.get("timeInterval", {}).get("start"),
            str(r.get("localResourceZone", "")).upper(),
        )
        for r in forecast_records
    }
    regions = {key[1] for key in actual_keys}
    zones = {key[1] for key in forecast_keys}
    return (
        len(actual_records) == len(actual_keys)
        and len(forecast_records) == len(forecast_keys)
        and len(actual_keys) == 72
        and len(forecast_keys) == 240
        and regions == EXPECTED_REGIONS
        and zones == EXPECTED_ZONES
    )
