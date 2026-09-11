# Architecture and processing semantics

## Design goals

The lakehouse is deliberately small: immutable source files, a narrow set of useful Silver tables, and a compact star-like Gold model. The design is suitable for Databricks Free Edition because work is batch-oriented, data is processed incrementally, and no paid cloud services are required.

## Data flow

1. Python clients download API responses into batch-specific landing folders.
2. Each file is written atomically. Its manifest records source, endpoint, retrieval time, row count where available, and SHA-256 checksum.
3. Bronze stores the complete JSON payload plus ingestion metadata. `record_checksum` prevents duplicate file ingestion.
4. Silver uses explicit schemas and source-grain business keys. Invalid rows are written to `quarantined_records`. A market date is promoted only when actual and forecast coverage agree and the expected three regions and ten LRZs are present.
5. Gold joins regional actual load to the sum of its LRZ forecasts, calculates errors, and produces dimensions and aggregates.
6. Quality outcomes are persisted independently from rejected records so operators can distinguish a warning from a blocking failure.

## Incremental and restart behavior

Landing directories are immutable and include a unique batch ID. Reusing an existing ID is rejected. Bronze uses an insert-only merge on the payload checksum. Silver and Gold use upserts on documented business keys so a later, traceable correction can replace an earlier value without duplication. When batches overlap, the newest ingested record wins and older duplicates are quarantined. A failed stage can be rerun: already committed records match their keys and are not duplicated.

Backfills use the same path as daily batches. Operators supply a historical date range to ingestion, then run Bronze through Gold. This avoids maintaining separate transformation logic.

## Time semantics

Raw timestamp strings are retained in Bronze. Historical MISO timestamps are interpreted as Eastern local time and normalized to UTC in Silver. `market_date` remains the source market date. DST completeness is based on the number of distinct source intervals and expected entities, so 23- and 25-hour dates are not automatically rejected.

## Data-quality policy

Blocking rules protect business keys and measurement trust: malformed JSON/schema, invalid timestamps, negative load/forecast, unknown region/LRZ, and incomplete paired market dates. Non-blocking observations include quarantine volume, unusual forecast error, freshness outside the expected batch cadence, and partial optional-source coverage. Thresholds should be set from verified historical distributions, not invented constants.

## Recovery and observability

The workflow is dependency-ordered. If a blocking check fails, Gold is not considered publishable. Investigators query `ops.data_quality_results`, then inspect `silver.quarantined_records` using batch ID and raw filename to trace the exact Bronze payload. Corrected files must arrive under a new batch ID so source lineage remains intact.
