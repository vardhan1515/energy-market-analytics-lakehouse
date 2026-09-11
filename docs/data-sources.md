# Data sources

| Source | Dataset | Access | Cadence in this project | Landing type |
|---|---|---|---|---|
| MISO Data Exchange | Historical regional actual load | Private subscription token | Daily/backfill | Historical batch |
| MISO Data Exchange | Published LRZ load forecast | Private subscription token | Daily/backfill | Historical batch |
| MISO public API | Demand, prices, fuel mix, renewables, outages, interchange | Public | Periodic snapshot | Near-real-time snapshot |
| Open-Meteo Historical API | Hourly temperature, humidity, dew point, apparent temperature, precipitation, cloud, wind, radiation | Public | Daily/backfill | Historical batch |

## Source contracts

Historical MISO actual load is expected at hourly × regional grain for `CENTRAL`, `NORTH`, and `SOUTH`. The forecast is expected at hourly × Local Resource Zone grain for `Z1` through `Z10`, with its publication/init date. Completeness is checked after deduplication.

Public MISO responses have endpoint-specific JSON shapes and can change without version notice. Bronze retains the complete response; schema drift should be detected before Silver promotion. Open-Meteo arrays must have the same length as their `hourly.time` array.

## Terms and attribution

This repository contains a minimal real-data sample for reproducibility, not a redistribution of the full source archive. Users are responsible for complying with current source terms. MISO and Open-Meteo are independent data providers and do not endorse this project.
