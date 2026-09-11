# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingestion
# MAGIC Loads immutable raw JSON files. The default path is the checked-in real MISO sample.

# COMMAND ----------

from pathlib import Path

dbutils.widgets.text("catalog", "energy_market", "Target catalog")
dbutils.widgets.text("sample_root", "../../sample_data/miso/2023-09-01", "Landing path")
dbutils.widgets.text("batch_id", "sample-2023-09-01", "Batch identifier")
catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")
sample_root = Path(dbutils.widgets.get("sample_root")).resolve().as_uri()

# COMMAND ----------

from energy_market_lakehouse.bronze import ingest_bronze_files

actual_files = ingest_bronze_files(
    spark,
    path=f"{sample_root}/actual_load_page_*.json",
    source_name="MISO Data Exchange",
    dataset_name="actual_load",
    batch_id=batch_id,
    table_name=f"{catalog}.bronze.actual_load_raw",
)
forecast_files = ingest_bronze_files(
    spark,
    path=f"{sample_root}/load_forecast_page_*.json",
    source_name="MISO Data Exchange",
    dataset_name="load_forecast",
    batch_id=batch_id,
    table_name=f"{catalog}.bronze.load_forecast_raw",
)
print({"actual_files_seen": actual_files, "forecast_files_seen": forecast_files})
