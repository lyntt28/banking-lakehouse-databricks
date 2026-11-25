# Banking Lakehouse on Databricks

End-to-end lakehouse for a retail bank: customers, accounts, loans and transactions flow from daily source files into a governed, tested star schema for credit risk and customer analytics.

> 🚧 Work in progress — see [Roadmap](#roadmap).

## Architecture

```
Source files (CSV/JSON, simulated core banking)
        │  Unity Catalog Volume (Free Edition) / ADLS Gen2 (Azure)
        ▼
BRONZE  raw, append-only, + _load_ts, _source_file
        ▼
SILVER  typed, deduplicated, validated
        DQ gate → ops.dq_quarantine
        MERGE (idempotent), SCD2 for dim_customer
        ▼
GOLD    Kimball star schema
        fact_loan_daily, fact_transaction
        dim_customer (SCD2), dim_branch, dim_product, dim_date
        marts: outstanding balance by debt group, customer_360
        ▼
Dashboards & alerts

Environments: bank_dev | bank_test | bank_prod  (Unity Catalog)
Orchestration: Databricks Jobs   ·   Deployment: Asset Bundles + GitHub Actions
```

## Key features
- **Data quality gate** — FAIL/WARN rules, quarantine table with failure reasons, row-count and balance reconciliation, stop-threshold.
- **Banking rules** — debt classification (nhóm nợ 1–5) by days past due, NPL flag.
- **SCD Type 2** — customer history with a single idempotent MERGE.
- **CI/CD** — lint + unit tests on every PR, bundle validation, deploy dev → test → prod.

## Repository layout
```
databricks.yml        Asset Bundle: targets dev / test / prod
resources/            Job definitions
src/00_setup/         Catalog, schemas, volume, ops tables
src/01_bronze/ …      Pipeline notebooks
src/common/           Reusable logic: dq.py, scd2.py, banking.py
tests/                pytest unit tests (local Spark)
docs/decisions.md     Architecture decision records
```

## Run it
```bash
# local tests
pip install -r requirements-dev.txt
pytest -q

# deploy to Databricks
databricks auth login --host <workspace-url>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run banking_lakehouse_daily -t dev
```

## Roadmap
- [x] Project skeleton, bundle config, CI
- [x] Reusable DQ, SCD2 and debt-classification logic with unit tests
- [ ] Bronze ingestion (Auto Loader)
- [ ] Silver: DQ, MERGE, SCD2
- [ ] Gold star schema and marts
- [ ] Unity Catalog governance: RBAC, column masks, row filters
- [ ] Performance tuning (liquid clustering, OPTIMIZE) with before/after numbers
- [ ] Azure Databricks deployment (ADLS Gen2, Terraform, service principal)
- [ ] Streaming AML alerts, feature table + MLflow
