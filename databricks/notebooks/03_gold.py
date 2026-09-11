# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold analytical model

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Target catalog")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

from energy_market_lakehouse.config import LakehouseNames
from energy_market_lakehouse.gold import (
    build_aggregates,
    build_dimensions,
    build_energy_load_fact,
    write_gold,
)

names = LakehouseNames(catalog)
actual = spark.table(names.table("silver", "actual_load_hourly"))
forecast = spark.table(names.table("silver", "load_forecast_hourly"))
fact = build_energy_load_fact(actual, forecast)
dates, regions, times = build_dimensions(spark, fact)
daily_peak, performance = build_aggregates(fact)
write_gold(spark, names, fact, dates, regions, times, daily_peak, performance)
display(performance.orderBy("market_date", "region"))
