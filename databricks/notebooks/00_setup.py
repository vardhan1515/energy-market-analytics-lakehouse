# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Workspace setup
# MAGIC Creates the four logical schemas. Run once per workspace or as the first workflow task.

# COMMAND ----------

# MAGIC %pip install -e ../..

# COMMAND ----------

dbutils.widgets.text("catalog", "energy_market", "Target catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from energy_market_lakehouse.delta import ensure_schema

for layer in ("bronze", "silver", "gold", "ops"):
    ensure_schema(spark, f"{catalog}.{layer}")

print(f"Lakehouse schemas are ready in catalog: {catalog}")
