# Limitations and next improvements

## Implemented boundary

Version 0.1 fully implements the credential-free sample path for historical actual load and published forecast through Bronze, Silver, Gold, quality tables, and dashboard queries. API clients can land the broader source set, but endpoint-specific Silver transforms for weather, prices, fuel mix, renewables, outages, and interchange are the next increment. Optional SQL for those tiles is commented out so the repository does not claim tables that do not yet exist.

Only compact real samples are committed: `2023-09-01` is complete, while `2023-08-31` is intentionally retained as an incomplete-day fixture. Aggregate portfolio findings must be recomputed from the user's full archive; this repository does not publish invented metrics or performance claims. Dashboard screenshots are workspace artifacts and have not yet been created.

Databricks Free Edition limits outbound internet access to trusted domains. In the current workspace,
`apim.misoenergy.org` fails DNS resolution before authentication even though the same subscription
returns HTTP 200 from MISO's signed-in API console. Automated historical ingestion therefore needs
local execution or Databricks compute with that hostname allowed by its egress policy.

## Planned, in order

1. Add explicit Silver schemas and quality tests for each public MISO endpoint.
2. Align ten weather locations to LRZs with a documented mapping and coverage score.
3. Add fuel/renewables, outage, interchange, price, and weather Gold aggregates.
4. Export the completed Databricks dashboard and record verified row counts.
5. Add scheduled incremental ingestion where the Free Edition workspace supports it.

Machine learning, RAG, Kafka, paid cloud deployment, and true streaming remain intentionally outside this version.
