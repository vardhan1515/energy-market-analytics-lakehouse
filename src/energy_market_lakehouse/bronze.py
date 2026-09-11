"""Bronze ingestion: one immutable Delta row per source JSON file."""

from __future__ import annotations

from .delta import merge_insert_only


def landing_files(spark, path: str, source_name: str, dataset_name: str, batch_id: str):
    from pyspark.sql import functions as F

    return (
        spark.read.option("wholetext", True)
        .text(path)
        .select(
            F.lit(source_name).alias("source_name"),
            F.lit(dataset_name).alias("dataset_name"),
            F.current_timestamp().alias("ingestion_timestamp_utc"),
            F.lit(batch_id).alias("batch_id"),
            F.input_file_name().alias("raw_file_name"),
            F.col("value").alias("raw_payload"),
        )
        .withColumn("record_checksum", F.sha2("raw_payload", 256))
    )


def ingest_bronze_files(
    spark,
    *,
    path: str,
    source_name: str,
    dataset_name: str,
    batch_id: str,
    table_name: str,
) -> int:
    frame = landing_files(spark, path, source_name, dataset_name, batch_id)
    merge_insert_only(spark, frame, table_name, ["record_checksum"])
    return frame.count()
