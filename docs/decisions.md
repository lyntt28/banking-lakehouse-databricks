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

## ADR-007: Reconcile Gold against the source system's own summary files
- **Context:** Core banking sends summary files (`card_kpi`, `deposit_summary`/bang1247) computed on its side. A dashboard number that disagrees with them will not be trusted.
- **Decision:** Gold recomputes every metric from detail records and writes a per-metric comparison to `ops.recon_card_kpi` / `ops.recon_deposit`. Stock metrics (card counts, balances, DPD) must match within rounding. Transaction-based metrics may differ by ≤ 1%: when two card transactions share a `txn_id`, Silver quarantines both (it cannot know which is real) while the source keeps the first. Any `MISMATCH` fails the job.
- **FK rule:** a foreign key is valid if the parent key exists in the source batch, even when the parent row itself was quarantined for another reason (e.g. bad date of birth). Otherwise one bad customer attribute would silently drop that customer's accounts from deposit totals.

## ADR-008: Governance as code (Unity Catalog)
- **Context:** Customer PII (name, ID number, phone, email, address, date of birth, income) must not be visible to analysts or branch staff, and branch staff must only see their own branch.
- **Decision:** `src/common/governance.py` generates all SQL: catalog/schema GRANTs per group (least privilege), column masks as SQL functions in `ops` (keep last 3-4 digits, first letter of each name word, year of birth only, income hidden), `pii` column tags, and a row filter on `branchnbr` driven by `ops.user_branch`. The `governance` job task re-applies everything after Gold (Gold overwrites some tables).
- **Pipeline identity:** once governance is on, Silver/Gold refuse to run unless the job identity is in `bank_admins` or `data_engineers`; otherwise SCD2 would hash masked values and Gold would read row-filtered tables.
- **Free Edition:** account groups are not available, so `governance_dry_run` defaults to `true` (prints SQL only). Set it to `false` on a workspace with groups.

## ADR-009: Promotion test → prod through GitHub environments
- **Decision:** CD deploys to `test` on every push to `main` and runs the full daily job there; the job's own DQ gate and reconciliation checks act as the smoke test. `prod` uses a GitHub environment with required reviewers, so a person approves each release. Auth prefers GitHub OIDC federation (no long-lived token in GitHub); a PAT secret still works.
- **Alternatives:** deploying prod on tag only (kept possible via `workflow_dispatch`), Databricks Git folders pull (no review step, no test run).
- **Consequences:** A broken DQ rule or reconciliation mismatch on test data blocks prod. Only prod runs on a schedule, so test costs nothing between releases.

## ADR-010: Liquid clustering instead of partitioning
- **Context:** Tables are small per day (thousands to ~100k rows), so date partitions would create many tiny files; dashboards filter by date + branch, pipelines join on account / customer id.
- **Decision:** `CLUSTER BY` on the filter / join columns (`src/common/maintenance.py`), `OPTIMIZE` weekly, `VACUUM` keeping 7 days. Branch-filtered tables cluster on `branchnbr` so the row filter (ADR-008) can skip files.
- **Measuring:** notebook `00_setup/04_maintenance.py` (`mode=benchmark`) times 4 dashboard-style queries before and after clustering (median of N runs, result cache off) and records file counts, into `ops.perf_benchmark`.

## ADR-011: Insurance cross-sell as a propensity model on Silver features
- **Context:** The generator plants a buying pattern (income, card grade, travel / health / education spend, segment). A short daily history has few new purchases, so "will buy in the next 90 days" has too few positives at first.
- **Decision:** Default label `holder` (customer already holds an in-force policy) trains a look-alike propensity model; `future` (new policy within `horizon_days` after `as_of`, existing holders removed) is ready for when history is long enough. Features are point-in-time (customer SCD2 version valid at `as_of`, transactions ≤ `as_of`) and never read insurance tables. Model: scikit-learn HistGradientBoosting (balanced classes), tracked in MLflow, registered in Unity Catalog, scores written for non-holders with deciles.
- **Consequences:** Metrics reported: ROC-AUC, PR-AUC, lift in the top 10%. On generated data the test asserts ROC-AUC > 0.65 and lift > 1.5.
