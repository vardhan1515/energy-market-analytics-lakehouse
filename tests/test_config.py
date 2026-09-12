from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from energy_market_lakehouse.config import LakehouseNames, Settings


class FakeSecrets:
    def get(self, *, scope: str, key: str) -> str:
        if (scope, key) != ("energy-market", "miso-api-token"):
            raise AssertionError("unexpected secret lookup")
        return "test-token"


class FakeDbutils:
    secrets = FakeSecrets()


class ConfigTests(unittest.TestCase):
    def test_default_catalog_matches_databricks_free_edition(self) -> None:
        self.assertEqual(LakehouseNames().catalog, "workspace")

    def test_table_names_are_qualified(self) -> None:
        self.assertEqual(
            LakehouseNames("demo").table("silver", "actual_load_hourly"),
            "demo.silver.actual_load_hourly",
        )

    def test_unknown_layer_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            LakehouseNames().table("platinum", "anything")

    def test_environment_does_not_require_secret(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings.from_environment()
        self.assertIsNone(settings.miso_api_token)
        self.assertEqual(settings.raw_data_dir.as_posix(), "data/raw")

    def test_databricks_secret_overrides_environment_token(self) -> None:
        with patch.dict(os.environ, {"MISO_API_TOKEN": "environment-token"}, clear=True):
            settings = Settings.from_databricks_secret(FakeDbutils())
        self.assertEqual(settings.miso_api_token, "test-token")


if __name__ == "__main__":
    unittest.main()
