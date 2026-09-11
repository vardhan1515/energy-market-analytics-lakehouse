# Data dictionary

## Bronze

All Bronze raw tables share: `source_name`, `dataset_name`, `ingestion_timestamp_utc`, `batch_id`, `raw_file_name`, `raw_payload`, and `record_checksum`.

## Silver historical tables

### `silver.actual_load_hourly`

| Column | Type | Meaning |
|---|---|---|
| `market_date` | date | MISO market date |
| `interval_start_utc` | timestamp | Normalized interval start |
| `interval_end_utc` | timestamp | Normalized interval end |
| `hour_ending` | smallint | Source hour-ending label |
| `region` | string | Normalized MISO region |
| `actual_load_mw` | double | Actual regional electricity demand |
| `batch_id` | string | Landing batch lineage |
| `raw_file_name` | string | Source-file lineage |
| `ingestion_timestamp_utc` | timestamp | Bronze ingestion time |
| `processed_timestamp_utc` | timestamp | Silver processing time |

### `silver.load_forecast_hourly`

Shares the time/lineage fields above and adds `forecast_init_date`, `local_resource_zone`, and `load_forecast_mw`.

### `silver.quarantined_records`

Stores dataset, market date, interval, region/LRZ, rejected value, reason, batch/file lineage, and quarantine timestamp. It is not an error log; every row remains queryable evidence.

## Gold

### `gold.fact_energy_load`

Hourly × region × forecast-init fact containing actual demand, aggregated published forecast, signed error (`forecast - actual`), absolute error, and absolute percentage error.

### Dimensions and aggregates

- `dim_date`: calendar attributes.
- `dim_time`: hour label and day part.
- `dim_region`: stable region key and name.
- `daily_peak_demand`: daily peak MW and its UTC interval by region.
- `regional_forecast_performance`: daily MAE, MAPE, RMSE, and evaluated hour count by region.

## Operations

`ops.data_quality_results` records run ID, dataset, rule, affected-row count, status, blocking flag, and evaluation time.
