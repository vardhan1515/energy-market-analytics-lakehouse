"""Business-ready facts, dimensions, and aggregates."""

from __future__ import annotations

from .delta import merge_upsert


def build_energy_load_fact(actual, forecast):
    from pyspark.sql import functions as F

    regional_forecast = forecast.groupBy(
        "market_date", "forecast_init_date", "interval_start_utc", "region"
    ).agg(F.sum("load_forecast_mw").alias("forecast_load_mw"))
    return (
        actual.join(
            regional_forecast,
            ["market_date", "interval_start_utc", "region"],
            "inner",
        )
        .select(
            "market_date",
            "forecast_init_date",
            "interval_start_utc",
            "region",
            "actual_load_mw",
            "forecast_load_mw",
        )
        .withColumn("forecast_error_mw", F.col("forecast_load_mw") - F.col("actual_load_mw"))
        .withColumn("absolute_error_mw", F.abs("forecast_error_mw"))
        .withColumn(
            "absolute_percentage_error",
            F.when(
                F.col("actual_load_mw") > 0,
                F.col("absolute_error_mw") / F.col("actual_load_mw") * 100,
            ),
        )
        .withColumn("updated_at_utc", F.current_timestamp())
    )


def build_dimensions(spark, fact):
    from pyspark.sql import functions as F

    dates = (
        fact.select("market_date")
        .distinct()
        .select(
            F.date_format("market_date", "yyyyMMdd").cast("int").alias("date_key"),
            "market_date",
            F.year("market_date").alias("year"),
            F.quarter("market_date").alias("quarter"),
            F.month("market_date").alias("month"),
            F.date_format("market_date", "MMMM").alias("month_name"),
            F.weekofyear("market_date").alias("week_of_year"),
            F.dayofweek("market_date").alias("day_of_week"),
            F.date_format("market_date", "EEEE").alias("day_name"),
            (F.dayofweek("market_date").isin(1, 7)).alias("is_weekend"),
        )
    )
    regions = (
        fact.select("region")
        .distinct()
        .withColumn("region_key", F.xxhash64("region"))
        .select("region_key", "region")
    )
    times = spark.range(24).select(
        F.col("id").cast("smallint").alias("hour"),
        F.format_string("%02d:00", F.col("id")).alias("hour_label"),
        F.when(F.col("id").between(6, 11), "Morning")
        .when(F.col("id").between(12, 17), "Afternoon")
        .when(F.col("id").between(18, 21), "Evening")
        .otherwise("Night")
        .alias("day_part"),
    )
    return dates, regions, times


def build_aggregates(fact):
    from pyspark.sql import functions as F

    daily_peak = (
        fact.groupBy("market_date", "region")
        .agg(
            F.max_by("actual_load_mw", "actual_load_mw").alias("peak_demand_mw"),
            F.max_by("interval_start_utc", "actual_load_mw").alias("peak_interval_utc"),
        )
        .withColumn("updated_at_utc", F.current_timestamp())
    )
    regional_performance = (
        fact.groupBy("market_date", "region")
        .agg(
            F.avg("absolute_error_mw").alias("mae_mw"),
            F.avg("absolute_percentage_error").alias("mape_percent"),
            F.sqrt(F.avg(F.pow("forecast_error_mw", 2))).alias("rmse_mw"),
            F.count("*").alias("hour_count"),
        )
        .withColumn("updated_at_utc", F.current_timestamp())
    )
    return daily_peak, regional_performance


def write_gold(spark, names, fact, dates, regions, times, daily_peak, performance) -> None:
    targets = [
        (fact, "fact_energy_load", ["forecast_init_date", "interval_start_utc", "region"]),
        (dates, "dim_date", ["date_key"]),
        (regions, "dim_region", ["region_key"]),
        (times, "dim_time", ["hour"]),
        (daily_peak, "daily_peak_demand", ["market_date", "region"]),
        (performance, "regional_forecast_performance", ["market_date", "region"]),
    ]
    for frame, table, keys in targets:
        merge_upsert(spark, frame, names.table("gold", table), keys)
