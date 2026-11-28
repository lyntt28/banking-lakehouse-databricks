# Banking Lakehouse on Databricks

End-to-end lakehouse for a retail bank. Daily core-banking files (customers, deposits,
loans, transactions, credit cards, insurance) flow through Bronze → Silver → Gold into
two consumers: **BI dashboards** (credit risk, card business, deposits) and an
**ML feature table** (insurance cross-sell). Every step is tested, quality-gated and
reconciled against the source system's own summary reports.

> 🚧 Work in progress — see [Roadmap](#roadmap).

## Architecture

```
Source files (simulated core banking, daily folders as_of_date=YYYY-MM-DD)
  detail : customers, accounts, loans (core-banking "duno" layout), transactions,
           cards, card_transactions, card_statements, insurance_policies, branches, products
  summary: card_kpi (long format), deposit_summary (bang1247 layout)  -> reconciliation
        │  Unity Catalog Volume (Free Edition) / ADLS Gen2 (Azure)
        ▼
BRONZE  Auto Loader, all columns as string, append-only
        + _source_file, _as_of_date, _load_ts, _rescued_data
        ▼
SILVER  typed, standardised (MM/DD/YYYY, "1,234", "12.34%"), deduplicated
        DQ gate (FAIL → ops.dq_quarantine with reasons, WARN → logged)
        MERGE (idempotent), SCD2 for dim_customer
        ▼
GOLD ─┬─ BI : Kimball star schema (conformed dims, docs/star_schema.md)
      │      dims : dim_date, dim_branch, dim_product, dim_customer (SCD2), dim_card
      │      facts: loan_fact_daily, core_fact_transaction, fact_deposit_daily,
      │             fact_card_daily, fact_card_transaction, card_mart_overdue_daily (aggregate)
      │      marts (built from facts): loan overdue + NPL, deposit trend / monthly / tenor /
      │             CASA / new accounts, card KPI daily / monthly / spend mix, customer_360
      │      gate: reconciliation vs card_kpi / bang1247 / overdue file → atomic batch publish
      │      serving: gold_serving views + per-dashboard reporting calendars
      │             → 3 AI/BI dashboards (+ Vega-Lite custom viz specs)
      └─ ML : customer × snapshot feature table (income, card grade, spend by MCC,
               cross-border ratio, revolving, DPD, product holding)
               label: buys insurance within N days → MLflow model

Environments : bank_dev | bank_test | bank_prod   (one Unity Catalog catalog each)
Orchestration: Databricks Jobs (setup → bronze → silver → gold_bi / gold_ml)
Deployment   : Declarative Automation Bundles (DAB) + GitHub Actions
```

## Key features
- **Realistic fake data** — layouts mimic real Vietnamese core-banking extracts; ~0.5–2%
  injected errors (duplicate/null keys, bad dates, negative balances, orphan foreign keys,
  invalid debt groups). No real customer data. See [data_generator](data_generator/README.md).
- **Data quality gate** — FAIL/WARN rules, quarantine with failure reasons, stop the job if
  any FAIL rule exceeds 5%, row-count reconciliation (bronze = silver + quarantine).
- **Reconciliation with source reports** — Gold KPIs must equal the source system's
  summary files (card KPI report, daily deposit summary).
- **Banking rules** — debt classification (groups 1–5) by days past due per Circular
  11/2021/TT-NHNN, NPL = groups 3–5.
- **SCD Type 2** — customer history with a single idempotent MERGE.
- **Idempotent ingestion** — Auto Loader checkpoints: re-running reads only new files.
- **CI/CD** — ruff + pytest on every PR; bundle validation; dev → test → prod deploy.

## CI/CD
- **CI** (`.github/workflows/ci.yml`, every PR): ruff, pytest on local Spark, `bundle validate`.
- **CD** (`.github/workflows/cd.yml`, push to `main`): tests → deploy bundle to `test` and run the daily job there (DQ gate and reconciliation failures stop the pipeline) → wait for approval → deploy to `prod`. Only `prod` has a schedule (06:30 Asia/Ho_Chi_Minh).
- One-time setup in GitHub → Settings → Environments: create `ci`, `test` and `prod`; add secrets `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` (deploy service principal, **OIDC only**, federation policy for `repo:<owner>/<repo>:environment:<env>`) and, for `test`/`prod`, `DATABRICKS_RUN_AS_SP` (separate runtime service principal used as `run_as`); add *Required reviewers* to `prod`. Then set repo variable `CD_ENABLED=true`.
- `CD_ENABLED` unset → deploy jobs show as **skipped** (demo mode, nothing deployed). Set → missing config **fails** the run; prod only deploys after the smoke test on `test` succeeded.
- Batch control, the Gold reconciliation gate and rerun/correction/backfill semantics: see `docs/runbook.md`; incidents: `docs/incident_runbook.md`; grain & lineage: `docs/data_dictionary.md`; coding standards: `CONTRIBUTING.md`.
- Tests: `pytest -m "not integration" --cov` (unit) and `pytest -m integration` (Bronze → Silver → Gold end-to-end on local Delta with failure scenarios: missing/late/duplicate files, reruns, reconciliation failure, out-of-order runs).

## Repository layout
```
databricks.yml          Bundle: variables + targets dev / test / prod
resources/jobs.yml      Job banking_lakehouse_daily (tasks + retries)
data_generator/         Fake core-banking data (15 source feeds, see `src/common/bronze.py`) with injected errors
src/00_setup/           Catalog, schemas, volume, ops tables; sample data generation
src/01_bronze/          Auto Loader ingestion
src/02_silver/          Typing, DQ gate, quarantine, MERGE / SCD2
src/03_gold/            Star schema (dim/fact) and dashboard marts
src/04_ml/              Insurance cross-sell: features, training, MLflow, scoring
src/common/             Reusable logic: bronze, silver, gold, gold_cards, dq, scd2, banking, delta_io
tests/                  pytest unit tests (local Spark)
docs/decisions.md       Architecture decision records (ADR-001..017)
docs/star_schema.md     Gold star schema: dims, facts, grain, keys
docs/gold_dashboards.md Gold BI overview, links to the per-dashboard specs:
docs/overdue_dashboard.md, docs/deposit_dashboard.md, docs/card_dashboard.md
                        business rules, metric definitions, date logic, migration
docs/custom_visualizations.md  Vega-Lite custom viz (Public Preview) and how to apply
dashboards/custom_viz/  Generated Vega-Lite specs + field mapping
tools/                  benchmark_dashboards.py (SQL Warehouse benchmark), compile_vega_specs.js
```

## Run it
```bash
# local tests
# requires Python 3.11 (see .python-version) and Java 17
pip install -r requirements-dev.txt
ruff check . && pytest -q

# fake data locally (10% volume)
python -m data_generator.generate --out ./data/landing --days 3 --scale 0.1

# deploy to Databricks
databricks auth login --host <workspace-url>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run banking_lakehouse_daily -t dev
# test / prod need an alert email
databricks bundle deploy -t prod --var notification_email=you@example.com
```

## Status (honest)
| Area | Status |
|---|---|
| Daily pipeline Bronze → Silver → Gold (DQ gate, recon, atomic publish) | **Running** on Databricks Free Edition (dev), 40 business days loaded |
| 4 AI/BI dashboards (Card, Card Overdue, Deposit, Operations Monitor) | **Running** on the dev workspace, deployed by the bundle |
| Dashboard query cost | **Measured** — warm runs ~250–300 ms per dataset ([benchmark](docs/benchmark.md)); cold first runs are warehouse start-up / cache, not SQL |
| CI (ruff, pytest, bundle validate, Vega-Lite compile) | **Running** on GitHub Actions |
| CD test → approval → prod | Code only — not yet run against a workspace |
| Unity Catalog governance (grants, masks, row filters) | Dry-run only (Free Edition limits) |
| MLflow cross-sell model | Code + unit tests — no tracked run yet |
| Liquid clustering / OPTIMIZE | Code only — no before/after numbers yet |
| Parallel Gold publish | Code, off by default (`GoldConfig.parallel=False`) |
| Azure ADLS + Terraform, Streaming AML | Planned |

**Known limit:** atomic publish is atomic per table: a reader sees the whole old or the
whole new batch of each table. It is **not** multi-table ACID; a query that joins several
serving views while a batch is being activated may, in a narrow window, mix batches
(ADR-012, consequences).

## Roadmap
- [x] Project skeleton, bundle config, CI
- [x] Reusable DQ, SCD2 and debt-classification logic with unit tests
- [x] Fake data generator: core banking, cards, insurance, reconciliation summaries
- [x] Bronze ingestion (Auto Loader) — code ready, run on Databricks pending
- [x] Silver: typing, DQ gate, MERGE, SCD2 — code + unit tests, run on Databricks pending
- [x] Gold BI star schema + marts (loan overdue / NPL with D-1·M-1·Y-1, deposit, customer 360), Silver↔Gold reconciliation — run on Databricks pending
- [x] Gold card marts (daily, spend mix, monthly MoM/YoY + % target) and reconciliation with source summary files (card KPI, bang1247)
- [x] Deposit marts (trend D-1/M-1/Y-1, tenor, CASA customers, Edu-Save new accounts) + 13-month history generation and backfill notebook
- [x] 3 AI/BI dashboards (Card, Overdue, Deposit) rebuilt from the original Chart.js dashboards: same order, chart types and colours (`dashboards/`, deployed by the bundle) — not yet opened on a workspace
- [x] Gold ML feature table + insurance cross-sell propensity model (MLflow, UC registry, scored list) — run on Databricks pending
- [x] CD: main → test (deploy + smoke test that must reprocess the latest day through both gates and Gold publish) → manual approval → prod, OIDC auth only (needs GitHub environments + secrets; optional `DATABRICKS_WAREHOUSE_ID` variable for non-Free-Edition workspaces) — not yet run against a workspace
- [x] Unity Catalog governance as code: RBAC grants (readers only see `gold_serving` views), PII column masks + tags, branch row filter (dry-run on Free Edition)
- [x] Atomic Gold publish: batch IDs + activation pointer (`ops.published_batches`), serving views, rollback only before activation, shared writer lock across jobs (ADR-012) — integration-tested on local Delta, run on Databricks pending
- [x] Performance tuning code: liquid clustering, OPTIMIZE/VACUUM weekly job, before/after benchmark notebook (numbers pending a Databricks run)
- [x] Dashboard refactor (Overdue, Deposit, Card): reporting calendars, point-in-time date logic, stock vs flow + completeness flags, incremental monthly marts, Overdue Silver↔Gold recon, targets by customer type / growth targets (ADR-013..015)
- [x] Star schema for deposit and card (dim_card, 3 new facts; marts read facts) (ADR-016)
- [x] Vega-Lite specs for the original look (KPI cards, gauges, dual-axis, two-year stacks), compiled in CI (ADR-017) — to be pasted in the UI
- [ ] Azure Databricks deployment (ADLS Gen2, Terraform, service principal)
- [ ] Streaming AML alerts
