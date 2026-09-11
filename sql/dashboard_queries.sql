-- Energy Market Analytics Lakehouse dashboard queries
-- Replace `energy_market` if a different catalog widget was used.

-- 1. Actual versus published forecast
SELECT
  interval_start_utc,
  region,
  actual_load_mw,
  forecast_load_mw
FROM energy_market.gold.fact_energy_load
WHERE market_date BETWEEN :start_date AND :end_date
ORDER BY interval_start_utc, region;

-- 2. Forecast error trend
SELECT
  interval_start_utc,
  region,
  forecast_error_mw,
  absolute_percentage_error
FROM energy_market.gold.fact_energy_load
WHERE market_date BETWEEN :start_date AND :end_date
ORDER BY interval_start_utc, region;

-- 3. Daily peak demand
SELECT market_date, region, peak_demand_mw, peak_interval_utc
FROM energy_market.gold.daily_peak_demand
WHERE market_date BETWEEN :start_date AND :end_date
ORDER BY market_date, region;

-- 4. Regional forecast-performance scorecard
SELECT
  region,
  AVG(mae_mw) AS mean_absolute_error_mw,
  AVG(mape_percent) AS mean_absolute_percentage_error,
  AVG(rmse_mw) AS root_mean_squared_error_mw
FROM energy_market.gold.regional_forecast_performance
WHERE market_date BETWEEN :start_date AND :end_date
GROUP BY region
ORDER BY mean_absolute_error_mw DESC;

-- 5. Hour-of-day demand profile
SELECT
  hour(interval_start_utc) AS hour_utc,
  region,
  AVG(actual_load_mw) AS average_load_mw,
  MAX(actual_load_mw) AS maximum_load_mw
FROM energy_market.gold.fact_energy_load
WHERE market_date BETWEEN :start_date AND :end_date
GROUP BY hour(interval_start_utc), region
ORDER BY hour_utc, region;

-- 6. Pipeline data-quality status
SELECT
  evaluated_at_utc,
  dataset_name,
  rule_name,
  failed_row_count,
  status,
  is_blocking
FROM energy_market.ops.data_quality_results
QUALIFY evaluated_at_utc = MAX(evaluated_at_utc) OVER ()
ORDER BY is_blocking DESC, status DESC, dataset_name, rule_name;

-- 7. Freshness (age of newest accepted historical measurement)
SELECT
  MAX(interval_start_utc) AS latest_measurement_utc,
  TIMESTAMPDIFF(HOUR, MAX(interval_start_utc), current_timestamp()) AS age_hours
FROM energy_market.silver.actual_load_hourly;

-- Optional queries become available after the corresponding near-real-time and
-- weather Silver loaders are enabled for a workspace.

-- 8. Weather versus demand
-- SELECT date_trunc('hour', l.interval_start_utc) AS interval_hour,
--        AVG(l.actual_load_mw) AS demand_mw,
--        AVG(w.temperature_2m_c) AS temperature_c,
--        AVG(w.relative_humidity_percent) AS relative_humidity_percent
-- FROM energy_market.gold.fact_energy_load l
-- JOIN energy_market.silver.weather_hourly w USING (interval_start_utc)
-- GROUP BY date_trunc('hour', l.interval_start_utc);

-- 9. Near-real-time price distribution
-- SELECT date_trunc('hour', interval_start_utc) AS interval_hour,
--        percentile(lmp_usd_per_mwh, array(0.05, 0.5, 0.95)) AS p05_p50_p95,
--        MIN(lmp_usd_per_mwh) AS min_lmp,
--        MAX(lmp_usd_per_mwh) AS max_lmp
-- FROM energy_market.silver.realtime_price
-- GROUP BY date_trunc('hour', interval_start_utc);
