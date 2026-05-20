# Architecture Decisions

## Storage

Parquet is used as the default table format for portability and predictable local execution. The pipeline partitions high-volume tables by dates used by downstream filters and enables Snappy compression through Spark configuration.

Delta Lake is installed but not required for the default run. That keeps CI and local Docker startup simple while leaving a clear path to ACID merges once the runtime includes Delta extensions.

## Incrementality

Silver transformations use per-entity bookmarks and latest-record deduplication. For this portfolio-sized dataset, complete partition rewrites are easier to reason about than file-level mutation. The same business keys and watermark columns can be moved to Delta merges later.

## Quality

Quality is implemented as code-level rules because the project benefits from transparent, reviewable constraints. Invalid rows are not dropped silently; they are written to quarantine with the failing rule name.
