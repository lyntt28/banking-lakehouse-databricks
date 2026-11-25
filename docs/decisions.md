# Architecture Decision Records

Each decision: context → decision → alternatives considered → consequences.

## ADR-001: Medallion architecture (Bronze / Silver / Gold)
- **Context:** Source data arrives as daily files from core banking systems and may contain duplicates, wrong types and invalid values.
- **Decision:** Bronze keeps raw data as-is (plus load metadata) for replay and audit. Silver holds cleaned, validated, deduplicated data. Gold holds a Kimball star schema and business marts.
- **Alternatives:** Loading straight into a warehouse model (no replay if logic changes).
- **Consequences:** More storage, but any layer can be rebuilt from the one below.

## ADR-002: Quarantine instead of dropping bad records
- **Context:** In banking, silently dropping records breaks reconciliation with source systems.
- **Decision:** Records failing a FAIL-level rule go to `ops.dq_quarantine` with the rule names. WARN-level rules only log. The job stops if any FAIL rule exceeds 5% of rows.
- **Consequences:** Row counts always reconcile: bronze = silver + quarantine.

## ADR-003: SCD Type 2 for customers, not for transactions
- **Context:** Customer attributes (address, segment) change and reports need the value at that time.
- **Decision:** `dim_customer` uses SCD2 (valid_from, valid_to, is_current, row_hash). Transactions are immutable facts, so they are append-only.

## ADR-004: One codebase, many environments via Asset Bundles
- **Decision:** Catalog names are bundle variables (`bank_dev`, `bank_test`, `bank_prod`). No hard-coded catalogs or paths in code.
- **Consequences:** Moving to Azure Databricks is adding one bundle target, not rewriting code.

## ADR-005: Safe casting in Silver, duplicates split in two kinds
- **Context:** Bronze keeps every column as string. Databricks serverless runs with ANSI mode, where `CAST`/`to_date` on a bad value fails the whole job.
- **Decision:** Silver casts with `try_cast` / `try_to_timestamp`, so a bad value becomes NULL, and a FAIL rule (`*_valid_format`) sends the row to quarantine. It keeps the raw value (`_raw_<col>`) so "empty" and "wrong format" are reported separately. Rows that are identical in every column (file sent twice) are dropped and counted. Rows with the same key but different content both go to quarantine, since we cannot tell which one is right.
- **Consequences:** Reconciliation is `bronze = silver + quarantine + exact duplicates`, checked in every run.

## ADR-006: Idempotent writes
- **Decision:** Silver and Gold facts use `MERGE` on the business key (transactions insert-only). Customers use SCD2 (ADR-003). Gold marts are recomputed for the whole day and written with `replaceWhere as_of_date = <day>`. Small dimensions are overwritten.
- **Consequences:** Re-running any day gives the same tables. Late data for a past day needs that day re-run.

<!-- TODO: liquid clustering, column masking -->
