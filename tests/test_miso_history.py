from __future__ import annotations

import json
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from energy_market_lakehouse.config import Settings
from energy_market_lakehouse.ingestion.miso_history import check_history_access

SAMPLE = Path(__file__).parents[1] / "sample_data" / "miso" / "2023-09-01"


class MisoHistoryAccessTests(unittest.TestCase):
    def test_access_check_returns_only_non_sensitive_diagnostics(self) -> None:
        payload = json.loads(
            (SAMPLE / "actual_load_page_0001.json").read_text(encoding="utf-8")
        )
        settings = Settings(miso_api_token="private-test-token")

        with (
            patch("energy_market_lakehouse.ingestion.miso_history.httpx.Client"),
            patch(
                "energy_market_lakehouse.ingestion.miso_history.get_json",
                return_value=payload,
            ),
        ):
            result = check_history_access(date(2023, 9, 1), settings)

        self.assertEqual(result["dataset"], "actual_load")
        self.assertEqual(result["first_page_rows"], 72)
        self.assertNotIn("private-test-token", repr(result))

    def test_access_check_rejects_unknown_dataset(self) -> None:
        with self.assertRaisesRegex(ValueError, "Unsupported MISO history dataset"):
            check_history_access(
                date(2023, 9, 1),
                Settings(miso_api_token="private-test-token"),
                dataset="unknown",
            )


if __name__ == "__main__":
    unittest.main()
