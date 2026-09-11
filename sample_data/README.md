# Sample data

These files are compact extracts copied unchanged from the read-only Energy Market Copilot source archive. They contain real MISO responses and no credentials.

- `miso/2023-09-01`: verified complete historical day (72 regional actual rows, 240 LRZ forecast rows); this is the default pipeline sample.
- `miso/2023-08-31`: intentionally incomplete actual-load day (42 actual rows, 240 forecast rows); useful for demonstrating quarantine behavior.
- `miso_realtime/20260823T022214Z`: small public operations and fuel-mix snapshot fixtures. Endpoint-specific Silver normalization is planned after version 0.1.

The source manifests preserve retrieval time, endpoint, counts, and checksums. Do not add the full historical archive to Git.
