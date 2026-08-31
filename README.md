# Payer Medallion Lakehouse (Databricks + Lakeflow)

A payer-side healthcare lakehouse on Databricks. Raw claims data flows through a medallion architecture (bronze to silver to gold) using **Lakeflow Declarative Pipelines**, governed by **Unity Catalog**, and deployed as a **Databricks Asset Bundle** so the whole project is version-controlled and reproducible from Git.

Domain entities: claims, members, providers, diagnoses, procedures.

## Architecture

Each medallion layer is its own Unity Catalog **schema**, so access can be granted per layer (analysts on gold, engineers on silver, raw bronze locked down). Raw files live in a UC **Volume**, so even the source data sits under the same governance model as the tables.

## Design decisions

- **Separation of concerns per layer.** Bronze preserves, silver conforms, gold serves. Failures are isolated to a layer; layers can be reprocessed independently.
- **Governance first.** Schema-per-layer under Unity Catalog for role-based   access; the Volume keeps raw files governed too.
- **Declarative over imperative.** The pipeline declares tables and dependencies; the engine resolves execution order and gives lineage for free.
- **Explicit schemas in bronze.** Avoids per-file inference cost and cross-file drift.
- **Fully-qualified names across schemas.** Cross-layer reads that cross a schema boundary are fully qualified; sibling reads within the default schema use bare names. This is what routes each layer's tables to the correct schema.

## Deploy

This project is a Databricks Asset Bundle. With the Databricks CLI configured for your workspace:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run payer_medallion
```

Before the first pipeline run, execute `setup/00_setup.py` once as a notebook to create the catalog, schemas, Volume, and load the sample CSVs.

The bundle variables in `databricks.yml` (catalog and schema names) flow into the pipeline as `conf.*` configuration keys, so the same code deploys to different environments by overriding variables per target.

## Planned enhancements

The current pipeline is a working baseline. The following are the deliberate next steps to make it production-ready for a regulated client:

1. **Auto Loader ingestion.** Move bronze from plain file streaming to `cloudFiles` for incremental file discovery and scale.
2. **AUTO CDC with SCD Type 2 on the members dimension.** Replace `.distinct()` dedupe with a change-data-capture flow keyed on member id and sequenced by an update timestamp, preserving full history of member changes. This is the managed, Databricks-native equivalent of a hand-written Delta MERGE in`foreachBatch`.
3. **Data quality expectations.** Add Lakeflow expectations (positive charges, non-null keys, valid claim status) using warn / drop / fail modes; publish the    resulting quality metrics to Unity Catalog for auditability.
4. **Grain correction in gold.** Collapse diagnoses to one row per claim in `claims_enriched` so charges do not double-count on multi-diagnosis claims.
