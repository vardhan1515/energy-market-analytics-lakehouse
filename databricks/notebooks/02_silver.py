# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver validation
# MAGIC Parses explicit schemas, standardizes timestamps, checks complete days, and quarantines rejects.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Target catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from datetime import UTC, datetime
from pyspark.sql import functions as F

from energy_market_lakehouse.config import LakehouseNames
from energy_market_lakehouse.delta import merge_insert_only
from energy_market_lakehouse.silver import (
    parse_actual_load,
    parse_load_forecast,
    validate_history,
    write_history_silver,
)

names = LakehouseNames(catalog)
actual = parse_actual_load(spark.table(names.table("bronze", "actual_load_raw")))
forecast = parse_load_forecast(spark.table(names.table("bronze", "load_forecast_raw")))
valid_actual, valid_forecast, quarantine = validate_history(actual, forecast)
write_history_silver(spark, valid_actual, valid_forecast, quarantine, names)

# COMMAND ----------

run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
actual_count = valid_actual.count()
forecast_count = valid_forecast.count()
quarantine_count = quarantine.count()
metrics = [
    (
        run_id,
        "actual_load_hourly",
        "nonempty_valid_output",
        0 if actual_count else 1,
        "PASS" if actual_count else "FAIL",
        True,
    ),
    (
        run_id,
        "load_forecast_hourly",
        "nonempty_valid_output",
        0 if forecast_count else 1,
        "PASS" if forecast_count else "FAIL",
        True,
    ),
    (
        run_id,
        "historical_load",
        "quarantined_rows",
        quarantine_count,
        "PASS" if quarantine_count == 0 else "WARN",
        False,
    ),
]
quality = spark.createDataFrame(
    metrics,
    "run_id string, dataset_name string, rule_name string, failed_row_count long, "
    "status string, is_blocking boolean",
).withColumn("evaluated_at_utc", F.current_timestamp())
merge_insert_only(
    spark,
    quality,
    names.table("ops", "data_quality_results"),
    ["run_id", "dataset_name", "rule_name"],
)
display(quality)
