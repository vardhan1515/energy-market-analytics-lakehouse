# Dashboard specification

Create one Databricks AI/BI dashboard named **Energy Market Operations & Forecast Performance** using `sql/dashboard_queries.sql`.

Recommended layout:

1. Header filters: market-date range and region.
2. KPI row: latest measurement, peak demand, regional MAE, quarantined rows.
3. Main trend: actual versus forecast demand, with region as a series/filter.
4. Error row: forecast-error trend and regional-performance ranking.
5. Demand shape: daily peak trend and average demand by hour.
6. Operations footer: latest data-quality checks and freshness.
7. Optional source row, once implemented: weather/demand scatter, generation mix, and LMP distribution.

Use consistent MW and USD/MWh units, descriptive subtitles, and date ranges on every time-dependent tile. Do not display an unfiltered raw table as a dashboard tile. Export screenshots to `docs/images/` after the workspace dashboard is created.
