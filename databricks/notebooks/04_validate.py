# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Pipeline validation
# MAGIC Fails the workflow if the latest run contains a blocking data-quality failure.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Target catalog")
catalog = dbutils.widgets.get("catalog")

from energy_market_lakehouse.quality import assert_no_blocking_failures

quality = spark.table(f"{catalog}.ops.data_quality_results")
latest_run = quality.agg({"evaluated_at_utc": "max"}).first()[0]
latest = quality.where(quality.evaluated_at_utc == latest_run)
assert_no_blocking_failures(latest)
display(latest)
