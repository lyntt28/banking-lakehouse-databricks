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
- **Decision:** Silver and Gold facts use `MERGE` on the business key (transactions insert-only). Customers use SCD2 (ADR-003). Gold marts are recomputed for the whole day and published as a new batch (ADR-012). Silver small dimensions are overwritten.
- **Consequences:** Re-running any day gives the same tables. Late data for a past day needs that day re-run.

## ADR-007: Reconcile Gold against the source system's own summary files
- **Context:** Core banking sends summary files (`card_kpi`, `deposit_summary`/bang1247) computed on its side. A dashboard number that disagrees with them will not be trusted.
- **Decision:** Gold recomputes every metric from detail records and writes a per-metric comparison to `ops.recon_card_kpi` / `ops.recon_deposit`. Stock metrics (card counts, balances, DPD) must match within rounding. Transaction-based metrics may differ by ≤ 1%: when two card transactions share a `txn_id`, Silver quarantines both (it cannot know which is real) while the source keeps the first. Any `MISMATCH` fails the job.
- **FK rule:** a foreign key is valid if the parent key exists in the source batch, even when the parent row itself was quarantined for another reason (e.g. bad date of birth). Otherwise one bad customer attribute would silently drop that customer's accounts from deposit totals.

## ADR-008: Governance as code (Unity Catalog)
- **Context:** Customer PII (name, ID number, phone, email, address, date of birth, income) must not be visible to analysts or branch staff, and branch staff must only see their own branch.
- **Decision:** `src/common/governance.py` generates all SQL: catalog/schema GRANTs per group (least privilege), column masks as SQL functions in `ops` (keep last 3-4 digits, first letter of each name word, year of birth only, income hidden), `pii` column tags, and a row filter on `branchnbr` driven by `ops.user_branch`. The `governance` job task re-applies everything after Gold. Read-only groups get `gold_serving` views (and the dashboard views `gold.v_*`), never raw `gold` tables, which also hold unpublished batches; old raw-table grants are revoked. Row filters and masks stay on the base tables and still apply through the views.
- **Pipeline identity:** once governance is on, Silver/Gold refuse to run unless the job identity is in `bank_admins` or `data_engineers`; otherwise SCD2 would hash masked values and Gold would read row-filtered tables.
- **Free Edition:** account groups are not available, so `governance_dry_run` defaults to `true` (prints SQL only). Set it to `false` on a workspace with groups.

## ADR-009: Promotion test → prod through GitHub environments
- **Decision:** CD deploys to `test` on every push to `main` and runs the daily job there as the smoke test with `as_of_date=latest` and `require_progress=true`: it reprocesses the latest Silver day (idempotent) through the DQ gate, reconciliation gate and Gold publish, and fails if no day was processed, so a green run is never "green because there was nothing to do". `prod` uses a GitHub environment with required reviewers, so a person approves each release. Auth is GitHub OIDC federation only (no PAT, no long-lived token in GitHub); a missing OIDC setting fails the job instead of skipping it.
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

## ADR-012: Atomic Gold publish with batch IDs and one writer at a time
- **Context:** Gold publish writes 18 tables one after another. Overwriting them in place let a reader see some new and some old tables, and a correction for a day already published could not be told apart from the old version.
- **Decision:** Every Gold row carries `batch_id` (= Gold run_id). Publish only appends the new batch; activation is one append to `ops.published_batches` (one Delta commit). Readers use `gold_serving` views that pick, per day, the latest activated batch that is present in the table snapshot being read (Delta does not snapshot two tables at the same instant, so joining only the pointer could return an empty result mid-switch). After activation, failed batches and batches older than the previously active one are deleted; the previous one is kept for queries still in flight. A failure before activation is cleaned by `RESTORE`; a failure after activation is NOT rolled back (the pointer already targets the new batch) and the run is marked FAILED. Serving views are validated after each publish and a broken view fails the run. All writing jobs (daily, manual Silver/Gold, backfill reset, maintenance) share a lease lock in `ops.writer_lock`, because `max_concurrent_runs: 1` only covers one job.
- **Consequences:** Readers never see a half-written batch. Gold tables hold up to two published batches per day (about 2x storage for re-published days). A concurrent-reader test checks every read sees the whole old batch or the whole new one, never a mix or an empty result. Money comparisons in the gate stay `Decimal` end to end (no float). A crashed job's lock expires after 6 hours, or an operator deletes it (incident runbook 5b).
- **Empty batches:** each Gold write of a daily/snapshot table also appends one marker row (`_batch_marker = true`) in the same Delta commit. A valid batch with zero rows for a table is therefore still "present", so the view shows zero rows instead of falling back to the previous batch. Views and the cumulative-mart builder exclude marker rows.
- **Known limitations (accepted for this project):**
  - *Cross-table consistency:* Delta guarantees atomicity per table, not across tables. A single query that joins several `gold_serving` views while a batch is being activated may combine the new batch of one table with the previous batch of another for the same day. The concurrent-reader test covers one table only. Options if this matters: read a pinned `batch_id` from `ops.published_batches` once and filter every table by it, put the dashboard's tables into one wide table, or use Unity Catalog multi-table transactions where available.
  - *Lock fencing:* the lease is checked and renewed before the commits that change what readers see (Silver publish, Gold activation), not before every write. Between two checks a job whose lease expired could still append invisible Gold rows or write Silver staging/ops tables. These are cleaned up or overwritten by the next run, but a strict guarantee would need a fencing token checked inside every write (e.g. a conditional commit on the lock version), which is out of scope.

## ADR-013: Reporting calendar per dashboard (resolve dates first, then read snapshots)
- **Context:** Every dashboard dataset found its own report date with `max(as_of_date)` / `DISTINCT as_of_date` over the full mart history, then aggregated all history before picking D-1 / M-1 / Y-1. Each visual could also fall back to a different date.
- **Decision:** `gold_serving.<dashboard>_report_calendar` views (Overdue, Deposit, Card) list only dates whose batch is activated and present in the Gold table, with the actual previous / previous-month / previous-year dates (NULL when that period has no data). Every dataset resolves one report date from the calendar, filters exactly the snapshots it needs, then aggregates.
- **Consequences:** One consistent published date for all visuals; comparisons show the real date and `N/A` instead of 0. M-1 is "last date inside the previous month" (no silent fall-back to older months). The calendar self-joins only the small list of dates.

## ADR-014: Overdue source grain is not unique - keep and sum, do not quarantine
- **Context:** The original sample (2,000 rows) has 711 rows sharing the assumed business key but none fully identical. Quarantining them as duplicates would silently drop balances.
- **Decision:** No key-uniqueness rule for `card_overdue_source` until the source contract confirms the grain; Silver writes the day with `replace_day`; Gold sums by grain; exact Silver = Gold reconciliation of the four measures (Decimal / BIGINT) is part of the publish gate.
- **Consequences:** Re-deliveries / corrections are still handled by `latest_delivery`, exact duplicates by `drop_exact_duplicates`. When the contract confirms a unique grain, add the FAIL rule back.

## ADR-015: Stock vs flow, completeness flags, incremental monthly marts
- **Context:** Deposit "monthly average" was computed from whatever days existed (month-end history gave a fake average); Card "using members" was `max` of daily distinct counts; history generated as month-end snapshots has no intra-month flows.
- **Decision:** Deposit MTD average is incremental (`avg_T = (avg_{T-1}(n-1) + bal_T)/n`) and only valid with a continuous daily chain (`monthly_avg_complete`). Card monthly flows are summed over every day of the period and carry `flow_complete` / `ytd_complete`; using members are exact distinct cards from transactions MTD. `dep_mart_monthly` and `card_mart_kpi_monthly` are stored per report date and recompute only the current month (closed months copied from the previous publish date).
- **Consequences:** Incomplete periods show `N/A` instead of misleading numbers; selecting a mid-month date never reads later data. First run after an upgrade rebuilds the monthly marts from history.

## ADR-016: Star schema with conformed dimensions; marts read facts
- **Context:** Only loans and account transactions had facts; Deposit and Card marts read Silver directly, so each mart re-derived keys and attributes.
- **Decision:** Add `dim_card` (SCD1) and facts `fact_deposit_daily`, `fact_card_daily` (periodic snapshots) and `fact_card_transaction` (transaction, insert-only), all keyed to `dim_date`, `dim_branch`, `dim_customer` (SCD2, as-of the fact date) and `dim_product` / `dim_card`. Marts are built from the facts. `card_mart_overdue_daily` is an aggregate fact at the source grain.
- **Consequences:** One place for keys and attributes; facts keep Silver column names so mart logic is unchanged (tests prove mart-from-fact = mart-from-Silver). `customer_sk` is NULL only for customers quarantined in Silver (an "unknown member" row is a possible follow-up).

## ADR-017: Dashboard look via Vega-Lite specs, not hand-written widget JSON
- **Context:** The original dashboards were custom HTML / Chart.js (KPI cards, gauges, two-year grouped stacks). AI/BI native widgets cannot reproduce them; Custom Visualizations (Vega-Lite) are Public Preview and their serialized widget format in `.lvdash.json` is not documented.
- **Decision:** Keep AI/BI dashboards (no Databricks Apps, no new infrastructure). Generate Vega-Lite specs + helper datasets (`*_kpi_cards`) and a mapping README; specs are pasted in the UI. Do not emit undocumented widget JSON. Treemap stays a ranked bar (Vega-Lite has no treemap).
- **Consequences:** One manual step per widget until a UI-created widget is exported and its real format is known; then the generator can emit them. Specs are compiled in CI and checked against dataset columns in tests.
