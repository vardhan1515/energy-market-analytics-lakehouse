"""Reusable data-quality result builders."""

from __future__ import annotations


def result_frame(spark, rows: list[tuple], schema: str):
    """Create consistent queryable rule outcomes from small driver-side summaries."""
    return spark.createDataFrame(rows, schema=schema)


def assert_no_blocking_failures(quality_frame) -> None:
    from pyspark.sql import functions as F

    failures = quality_frame.where((F.col("status") == "FAIL") & F.col("is_blocking")).count()
    if failures:
        raise RuntimeError(f"Data quality failed with {failures} blocking rule(s)")
