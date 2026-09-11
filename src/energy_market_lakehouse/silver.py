"""Bronze-to-Silver historical load transformations."""

from __future__ import annotations

from .constants import EXPECTED_REGIONS, EXPECTED_ZONES
from .delta import merge_insert_only, merge_upsert
from .schemas import actual_load_payload_schema, load_forecast_payload_schema


def parse_actual_load(bronze_frame):
    from pyspark.sql import functions as F

    parsed = bronze_frame.select(
        "batch_id",
        "raw_file_name",
        "ingestion_timestamp_utc",
        F.explode_outer(F.from_json("raw_payload", actual_load_payload_schema()).data).alias("r"),
    )
    return parsed.select(
        F.to_date("r.timeInterval.start").alias("market_date"),
        F.to_utc_timestamp(F.to_timestamp("r.timeInterval.start"), "America/New_York").alias(
            "interval_start_utc"
        ),
        F.to_utc_timestamp(F.to_timestamp("r.timeInterval.end"), "America/New_York").alias(
            "interval_end_utc"
        ),
        F.col("r.timeInterval.value").cast("smallint").alias("hour_ending"),
        F.upper(F.trim("r.region")).alias("region"),
        F.col("r.load").cast("double").alias("actual_load_mw"),
        "batch_id",
        "raw_file_name",
        "ingestion_timestamp_utc",
    ).withColumn("processed_timestamp_utc", F.current_timestamp())


def parse_load_forecast(bronze_frame):
    from pyspark.sql import functions as F

    parsed = bronze_frame.select(
        "batch_id",
        "raw_file_name",
        "ingestion_timestamp_utc",
        F.explode_outer(F.from_json("raw_payload", load_forecast_payload_schema()).data).alias("r"),
    )
    return parsed.select(
        F.to_date("r.timeInterval.start").alias("market_date"),
        F.to_date("r.init").alias("forecast_init_date"),
        F.to_utc_timestamp(F.to_timestamp("r.timeInterval.start"), "America/New_York").alias(
            "interval_start_utc"
        ),
        F.to_utc_timestamp(F.to_timestamp("r.timeInterval.end"), "America/New_York").alias(
            "interval_end_utc"
        ),
        F.col("r.timeInterval.value").cast("smallint").alias("hour_ending"),
        F.upper(F.trim("r.region")).alias("region"),
        F.upper(F.trim("r.localResourceZone")).alias("local_resource_zone"),
        F.col("r.loadForecast").cast("double").alias("load_forecast_mw"),
        "batch_id",
        "raw_file_name",
        "ingestion_timestamp_utc",
    ).withColumn("processed_timestamp_utc", F.current_timestamp())


def validate_history(actual, forecast):
    """Return valid actuals, valid forecasts, quarantine rows, and rule results."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    actual_window = Window.partitionBy("interval_start_utc", "region").orderBy(
        F.col("ingestion_timestamp_utc").desc_nulls_last(), F.col("batch_id").desc()
    )
    forecast_window = Window.partitionBy(
        "forecast_init_date", "interval_start_utc", "local_resource_zone"
    ).orderBy(F.col("ingestion_timestamp_utc").desc_nulls_last(), F.col("batch_id").desc())
    actual = actual.withColumn("duplicate_rank", F.row_number().over(actual_window))
    forecast = forecast.withColumn("duplicate_rank", F.row_number().over(forecast_window))

    actual_reason = (
        F.when(F.col("interval_start_utc").isNull(), "INVALID_TIMESTAMP")
        .when(F.col("actual_load_mw").isNull(), "MISSING_MEASUREMENT")
        .when(F.col("actual_load_mw") < 0, "NEGATIVE_LOAD")
        .when(~F.col("region").isin(sorted(EXPECTED_REGIONS)), "UNEXPECTED_REGION")
        .when(F.col("duplicate_rank") > 1, "DUPLICATE_BUSINESS_KEY")
    )
    forecast_reason = (
        F.when(F.col("interval_start_utc").isNull(), "INVALID_TIMESTAMP")
        .when(F.col("load_forecast_mw").isNull(), "MISSING_MEASUREMENT")
        .when(F.col("load_forecast_mw") < 0, "NEGATIVE_FORECAST")
        .when(~F.col("local_resource_zone").isin(sorted(EXPECTED_ZONES)), "UNEXPECTED_ZONE")
        .when(F.col("duplicate_rank") > 1, "DUPLICATE_BUSINESS_KEY")
    )
    actual_checked = actual.withColumn("quarantine_reason", actual_reason)
    forecast_checked = forecast.withColumn("quarantine_reason", forecast_reason)
    clean_actual = actual_checked.where("quarantine_reason IS NULL").drop(
        "quarantine_reason", "duplicate_rank"
    )
    clean_forecast = forecast_checked.where("quarantine_reason IS NULL").drop(
        "quarantine_reason", "duplicate_rank"
    )

    actual_day = clean_actual.groupBy("market_date").agg(
        F.count("*").alias("actual_rows"),
        F.countDistinct("interval_start_utc").alias("actual_intervals"),
        F.countDistinct("region").alias("region_count"),
    )
    forecast_day = clean_forecast.groupBy("market_date").agg(
        F.count("*").alias("forecast_rows"),
        F.countDistinct("interval_start_utc").alias("forecast_intervals"),
        F.countDistinct("local_resource_zone").alias("zone_count"),
    )
    complete_dates = (
        actual_day.join(forecast_day, "market_date", "full")
        .withColumn(
            "is_complete",
            (F.col("region_count") == 3)
            & (F.col("zone_count") == 10)
            & (F.col("actual_rows") == F.col("actual_intervals") * 3)
            & (F.col("forecast_rows") == F.col("forecast_intervals") * 10)
            & (F.col("actual_intervals") == F.col("forecast_intervals")),
        )
        .where("is_complete")
        .select("market_date")
    )
    valid_actual = clean_actual.join(complete_dates, "market_date", "inner")
    valid_forecast = clean_forecast.join(complete_dates, "market_date", "inner")

    incomplete_actual = clean_actual.join(complete_dates, "market_date", "left_anti").withColumn(
        "quarantine_reason", F.lit("INCOMPLETE_MARKET_DATE")
    )
    incomplete_forecast = clean_forecast.join(
        complete_dates, "market_date", "left_anti"
    ).withColumn("quarantine_reason", F.lit("INCOMPLETE_MARKET_DATE"))

    def quarantine(frame, dataset, value_column):
        zone = F.col("local_resource_zone") if "local_resource_zone" in frame.columns else F.lit("")
        return frame.select(
            F.lit(dataset).alias("dataset_name"),
            F.col("market_date"),
            F.col("interval_start_utc"),
            F.coalesce(F.col("region"), F.lit("")).alias("region"),
            F.coalesce(zone, F.lit("")).alias("local_resource_zone"),
            F.col(value_column).cast("string").alias("rejected_value"),
            "quarantine_reason",
            "batch_id",
            "raw_file_name",
            F.current_timestamp().alias("quarantined_at_utc"),
        )

    actual_rejected = (
        actual_checked.where("quarantine_reason IS NOT NULL")
        .drop("duplicate_rank")
        .unionByName(incomplete_actual, allowMissingColumns=True)
    )
    forecast_rejected = (
        forecast_checked.where("quarantine_reason IS NOT NULL")
        .drop("duplicate_rank")
        .unionByName(incomplete_forecast, allowMissingColumns=True)
    )
    quarantined = quarantine(actual_rejected, "actual_load", "actual_load_mw").unionByName(
        quarantine(forecast_rejected, "load_forecast", "load_forecast_mw")
    )
    return valid_actual, valid_forecast, quarantined


def write_history_silver(spark, actual, forecast, quarantine, names) -> None:
    merge_upsert(
        spark,
        actual,
        names.table("silver", "actual_load_hourly"),
        ["interval_start_utc", "region"],
    )
    merge_upsert(
        spark,
        forecast,
        names.table("silver", "load_forecast_hourly"),
        ["forecast_init_date", "interval_start_utc", "local_resource_zone"],
    )
    if not quarantine.rdd.isEmpty():
        merge_insert_only(
            spark,
            quarantine,
            names.table("silver", "quarantined_records"),
            [
                "dataset_name",
                "batch_id",
                "raw_file_name",
                "interval_start_utc",
                "region",
                "local_resource_zone",
                "quarantine_reason",
            ],
        )
