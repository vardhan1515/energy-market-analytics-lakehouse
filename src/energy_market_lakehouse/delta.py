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


def replace_matching_values(spark, table_name: str, values, column: str) -> None:
    """Delete target rows for values being recomputed before a deterministic rebuild."""
    if not spark.catalog.tableExists(table_name):
        return
    distinct_values = values.select(column).where(f"`{column}` IS NOT NULL").distinct()
    if not distinct_values.limit(1).count():
        return
    view = f"replacement_values_{uuid4().hex}"
    distinct_values.createOrReplaceTempView(view)
    try:
        spark.sql(
            f"DELETE FROM {table_name} AS target WHERE EXISTS "
            f"(SELECT 1 FROM {view} AS source "
            f"WHERE target.`{column}` <=> source.`{column}`)"
        )
    finally:
        spark.catalog.dropTempView(view)
