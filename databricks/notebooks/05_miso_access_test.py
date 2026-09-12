# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — MISO credential smoke test
# MAGIC Makes one authenticated request and prints only non-sensitive diagnostics.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from datetime import date

from energy_market_lakehouse.config import Settings
from energy_market_lakehouse.ingestion.miso_history import check_history_access

dbutils.widgets.text("secret_scope", "energy-market", "Secret scope")
dbutils.widgets.text("secret_key", "miso-api-token", "Secret key")
dbutils.widgets.text("market_date", "2023-09-01", "Market date")

# COMMAND ----------

settings = Settings.from_databricks_secret(
    dbutils,
    scope=dbutils.widgets.get("secret_scope"),
    key=dbutils.widgets.get("secret_key"),
)
result = check_history_access(date.fromisoformat(dbutils.widgets.get("market_date")), settings)
print(
    "MISO credential check succeeded: "
    f"dataset={result['dataset']}, "
    f"market_date={result['market_date']}, "
    f"first_page_rows={result['first_page_rows']}"
)
