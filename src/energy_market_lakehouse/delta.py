"""Small Delta write helpers used by all layers."""

from __future__ import annotations

from collections.abc import Sequence
from uuid import uuid4


def ensure_schema(spark, schema_name: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")


def merge_insert_only(spark, frame, table_name: str, keys: Sequence[str]) -> None:
    """Insert unseen business keys and leave matching immutable rows unchanged."""
    if not spark.catalog.tableExists(table_name):
        frame.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return
    view = f"merge_source_{uuid4().hex}"
    frame.createOrReplaceTempView(view)
    condition = " AND ".join(f"target.`{key}` <=> source.`{key}`" for key in keys)
    try:
        spark.sql(
            f"MERGE INTO {table_name} target USING {view} source ON {condition} "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(view)


def merge_upsert(spark, frame, table_name: str, keys: Sequence[str]) -> None:
    """Update matching facts and insert unseen business keys."""
    if not spark.catalog.tableExists(table_name):
        frame.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return
    view = f"merge_source_{uuid4().hex}"
    frame.createOrReplaceTempView(view)
    condition = " AND ".join(f"target.`{key}` <=> source.`{key}`" for key in keys)
    try:
        spark.sql(
            f"MERGE INTO {table_name} target USING {view} source ON {condition} "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(view)
