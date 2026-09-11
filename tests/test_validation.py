from __future__ import annotations

import json
import unittest
from pathlib import Path

from energy_market_lakehouse.validation import (
    SourceValidationError,
    ordinary_day_is_complete,
    validate_history_payload,
)

SAMPLE = Path(__file__).parents[1] / "sample_data" / "miso" / "2023-09-01"


class HistoryValidationTests(unittest.TestCase):
    def payload(self, name: str) -> dict:
        return json.loads((SAMPLE / name).read_text(encoding="utf-8"))

    def test_real_sample_satisfies_source_contracts(self) -> None:
        actual = self.payload("actual_load_page_0001.json")
        forecast = self.payload("load_forecast_page_0001.json")
        self.assertEqual(validate_history_payload(actual, "actual_load"), 72)
        self.assertEqual(validate_history_payload(forecast, "load_forecast"), 240)
        self.assertTrue(ordinary_day_is_complete(actual["data"], forecast["data"]))

    def test_negative_load_is_rejected(self) -> None:
        payload = {"data": [{"timeInterval": {"start": "2026-01-01T00:00:00"}, "load": -1.0}]}
        with self.assertRaisesRegex(SourceValidationError, "invalid load"):
            validate_history_payload(payload, "actual_load")

    def test_missing_actual_record_makes_day_incomplete(self) -> None:
        actual = self.payload("actual_load_page_0001.json")["data"][:-1]
        forecast = self.payload("load_forecast_page_0001.json")["data"]
        self.assertFalse(ordinary_day_is_complete(actual, forecast))

    def test_duplicate_business_key_makes_day_incomplete(self) -> None:
        actual = self.payload("actual_load_page_0001.json")["data"]
        forecast = self.payload("load_forecast_page_0001.json")["data"]
        self.assertFalse(ordinary_day_is_complete(actual + [actual[0]], forecast))


if __name__ == "__main__":
    unittest.main()
