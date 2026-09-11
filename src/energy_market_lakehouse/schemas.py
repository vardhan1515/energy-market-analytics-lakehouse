"""Explicit Spark schemas for source payloads.

Imports live inside functions so ingestion and unit tests remain usable without a
local Spark installation.
"""


def actual_load_payload_schema():
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    interval = StructType(
        [
            StructField("resolution", StringType()),
            StructField("start", StringType()),
            StructField("end", StringType()),
            StructField("value", StringType()),
        ]
    )
    record = StructType(
        [
            StructField("timeInterval", interval),
            StructField("region", StringType()),
            StructField("load", DoubleType()),
        ]
    )
    return StructType([StructField("data", ArrayType(record))])


def load_forecast_payload_schema():
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    interval = StructType(
        [
            StructField("resolution", StringType()),
            StructField("start", StringType()),
            StructField("end", StringType()),
            StructField("value", StringType()),
        ]
    )
    record = StructType(
        [
            StructField("timeInterval", interval),
            StructField("region", StringType()),
            StructField("localResourceZone", StringType()),
            StructField("loadForecast", DoubleType()),
            StructField("init", StringType()),
        ]
    )
    return StructType([StructField("data", ArrayType(record))])
