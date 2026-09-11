from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

HAS_PYSPARK = importlib.util.find_spec("pyspark") is not None
SAMPLE = Path(__file__).parents[1] / "sample_data" / "miso" / "2023-09-01"


@unittest.skipUnless(HAS_PYSPARK, "PySpark is not installed")
class SparkTransformationTests(unittest.TestCase):
    def test_complete_real_day_passes_silver_validation(self) -> None:
        from pyspark.sql import SparkSession

        from energy_market_lakehouse.silver import (
            parse_actual_load,
            parse_load_forecast,
            validate_history,
        )

        spark = SparkSession.builder.master("local[1]").appName("lakehouse-test").getOrCreate()
        try:

            def bronze(filename: str):
                raw = (SAMPLE / filename).read_text(encoding="utf-8")
                return spark.createDataFrame(
                    [("sample", filename, None, raw)],
                    "batch_id string, raw_file_name string, "
                    "ingestion_timestamp_utc timestamp, raw_payload string",
                )

            actual = parse_actual_load(bronze("actual_load_page_0001.json"))
            forecast = parse_load_forecast(bronze("load_forecast_page_0001.json"))
            valid_actual, valid_forecast, quarantine = validate_history(actual, forecast)
            self.assertEqual(valid_actual.count(), 72)
            self.assertEqual(valid_forecast.count(), 240)
            self.assertEqual(quarantine.count(), 0)
        finally:
            spark.stop()


if __name__ == "__main__":
    unittest.main()
