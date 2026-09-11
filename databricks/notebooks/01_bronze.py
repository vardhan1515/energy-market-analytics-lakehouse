# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingestion
# MAGIC Loads immutable raw JSON files. The default path is the checked-in real MISO sample.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from pathlib import Path

dbutils.widgets.text("catalog", "workspace", "Target catalog")
dbutils.widgets.text("sample_root", "sample_data/miso/2023-09-01", "Landing path")
dbutils.widgets.text("batch_id", "sample-2023-09-01", "Batch identifier")
catalog = dbutils.widgets.get("catalog")
batch_id = dbutils.widgets.get("batch_id")


def resolve_sample_root(value: str) -> Path:
    requested = Path(value)
    candidates = [requested] if requested.is_absolute() else [
        base / requested for base in (Path.cwd(), *Path.cwd().parents)
    ]
    for candidate in candidates:
        if candidate.is_dir():
            return candidate.resolve()
    raise FileNotFoundError(f"Landing directory not found: {value}")


sample_root = resolve_sample_root(dbutils.widgets.get("sample_root"))


def read_payloads(pattern: str) -> list[tuple[str, str]]:
    files = sorted(sample_root.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files matched {sample_root / pattern}")
    return [(path.as_posix(), path.read_text(encoding="utf-8")) for path in files]

# COMMAND ----------

from energy_market_lakehouse.bronze import ingest_bronze_payloads

actual_files = ingest_bronze_payloads(
    spark,
    files=read_payloads("actual_load_page_*.json"),
    source_name="MISO Data Exchange",
    dataset_name="actual_load",
    batch_id=batch_id,
    table_name=f"{catalog}.bronze.actual_load_raw",
)
forecast_files = ingest_bronze_payloads(
    spark,
    files=read_payloads("load_forecast_page_*.json"),
    source_name="MISO Data Exchange",
    dataset_name="load_forecast",
    batch_id=batch_id,
    table_name=f"{catalog}.bronze.load_forecast_raw",
)
print({"actual_files_seen": actual_files, "forecast_files_seen": forecast_files})
