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
GOLD ─┬─ BI : Kimball star schema
      │      loan_fact_daily, core_fact_transaction, fact_card_txn
      │      dim_customer (SCD2), dim_account, dim_card, dim_branch, dim_product, dim_date
      │      marts: loan overdue × branch × debt group + NPL, card KPIs, deposit balance,
      │             customer_360  → Databricks SQL dashboards + alerts
      │      reconciliation vs card_kpi / deposit_summary → ops.dq_results
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
  any FAIL rule exceeds 5%, row-count reconciliation (bronze = silver + quarantine + dropped duplicates / superseded files).
- **Batch control** — every expected business day is processed in order (month-end snapshots
  before `daily_feed_start`, daily after); a missing day stops the run instead of being skipped,
  and in prod a day whose files are late past the SLA (`sla_lag_days`) fails the job. A
  re-sent file replaces the earlier delivery of that day. Silver and Gold are staged, gated and
  rolled back automatically if publishing fails part-way. Details: `docs/runbook.md`.
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
- Tests: `pytest -m "not integration" --cov` (unit) and `pytest -m integration` (Bronze → Silver → Gold end-to-end on local Delta with failure scenarios: missing/late/duplicate files, corrections, reruns, reconciliation failure, mid-publish failure + rollback, out-of-order runs). Each scenario sets up its own state and can be run on its own (`pytest "tests/integration/test_pipeline_scenarios.py::<test>"`).

## Repository layout
```
databricks.yml          Bundle: variables + targets dev / test / prod
resources/jobs.yml      Jobs: banking_lakehouse_daily, weekly maintenance, weekly ML
resources/dashboards.yml 4 AI/BI dashboards (dashboards/*.lvdash.json)
data_generator/         Fake core-banking data (13 tables) with injected errors
src/00_setup/           Catalog, schemas, volume, ops tables; sample data generation
src/01_bronze/          Auto Loader ingestion
src/02_silver/          Typing, DQ gate, quarantine, MERGE / SCD2
src/03_gold/            Star schema (dim/fact) and dashboard marts
src/04_ml/              Insurance cross-sell: features, training, MLflow, scoring
src/common/             Reusable logic: bronze, silver, gold, gold_cards, dq, scd2, banking, delta_io
tests/                  pytest unit tests (local Spark)
docs/decisions.md       Architecture decision records
docs/gold_dashboards.md Gold BI spec (card, overdue, deposit dashboards)
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

## Roadmap
- [x] Project skeleton, bundle config, CI
- [x] Reusable DQ, SCD2 and debt-classification logic with unit tests
- [x] Fake data generator: core banking, cards, insurance, reconciliation summaries
- [x] Bronze ingestion (Auto Loader) — code ready, run on Databricks pending
- [x] Silver: typing, DQ gate, MERGE, SCD2 — code + unit tests, run on Databricks pending
- [x] Gold BI star schema + marts (loan overdue / NPL with D-1·M-1·Y-1, deposit, customer 360), Silver↔Gold reconciliation — run on Databricks pending
- [x] Gold card marts (daily, spend mix, monthly MoM/YoY + % target) and reconciliation with source summary files (card KPI, bang1247)
- [x] Deposit marts (trend D-1/M-1/Y-1, tenor, CASA customers, Edu-Save new accounts) + 13-month history generation and backfill notebook
- [x] 3 AI/BI dashboards (Card, Overdue, Deposit) rebuilt from the original Chart.js dashboards (original source not kept in this repo; spec in `docs/gold_dashboards.md`) + an operations monitor (`dashboards/`, deployed by the bundle) — not yet opened on a workspace
- [x] Gold ML feature table + insurance cross-sell propensity model (MLflow, UC registry, scored list) — run on Databricks pending
- [x] CD: main → test (deploy + run job as smoke test) → manual approval → prod, OIDC or token auth (needs GitHub environments + secrets)
- [x] Unity Catalog governance as code: RBAC grants, PII column masks + tags, branch row filter (dry-run on Free Edition)
- [x] Performance tuning code: liquid clustering, OPTIMIZE/VACUUM weekly job, before/after benchmark notebook (numbers pending a Databricks run)
- [ ] Azure Databricks deployment (ADLS Gen2, Terraform, service principal)
- [ ] Streaming AML alerts
