# Fake core-banking data generator

All data is **synthetic** (no real customer data). Column layouts mimic real Vietnamese
core-banking extracts (loan balance file `duno`, daily deposit summary, card KPI report)
so that Silver has realistic cleaning work to do.

```bash
python -m data_generator.generate --out ./data/landing --start-date 2026-11-01 --days 3
python -m data_generator.generate --scale 0.1   # 10% volume for quick tests
```

Output: `<out>/<table>/as_of_date=YYYY-MM-DD/<file>` (one folder per day → Auto Loader).

| Table | Format | Key | Rows/day (scale 1) | Used for |
|---|---|---|---|---|
| branches, products | CSV, day 1 only | branchnbr / prodcode | 10 / 11 | dim_branch, dim_product |
| customers | CSV | customer_id | ~20k, ~3% attributes change daily | dim_customer (SCD2) |
| accounts | CSV | account_id | ~42k (deposit + loan accounts) | dim_account, deposit facts |
| loans | CSV, `duno` layout, MM/DD/YYYY | acctnbr (= account_id) | ~12k | fact_loan_daily, NPL mart |
| transactions | JSON lines | txn_id | 100k | fact_transaction |
| cards | CSV | card_no | ~9k, open/close daily | dim_card |
| card_transactions | JSON lines (MCC, merchant, POS/ONLINE, DOMESTIC/XBORDER) | txn_id | 50k | fact_card_txn, ML features |
| card_statements | CSV | (stnd_dt, card_no) | ~9k | card balance / DPD |
| insurance_policies | CSV | policy_id | ~1.5k, grows daily | ML label (insurance cross-sell) |
| card_kpi | 9 TSV files, long format, `"219,627"`, `"56.78%"` | (Trx_Date, Metric_Code) | ~45 | **reconciliation** vs Gold card KPIs |
| deposit_summary | TSV, `bang1247` layout, YYYYMMDD | (group, currency, branch) | ~80 | **reconciliation** vs Gold deposit balance |

Injected errors (~0.5–2% each, below the 5% stop threshold): null/duplicate keys,
bad ID length, age < 18, wrong date formats, negative/NULL balance, disbursement after
maturity, debt group outside 1–5 or not matching DPD, rate > 40%, orphan foreign keys,
amount ≤ 0, future timestamps, unknown status, credit limit 0.

Summary files are computed from **valid** rows only: after Silver quarantines FAIL rows
(keeping the first row of duplicated keys), Gold totals must equal them exactly.

Built-in signal for ML: higher income, higher card grade, travel/health/education spending
and AFFLUENT/PRIORITY segment → higher probability of buying insurance.

## Gold layer: two branches

```
Silver ──┬─> Gold BI  (Kimball star: facts + dims, marts)  ─> SQL dashboard + alerts
         │     fact_loan_daily, fact_transaction, fact_card_txn, dim_* (SCD2 customer)
         │     mart: loan balance x branch x debt group + NPL, card KPI, deposit, customer_360
         │     reconciliation vs card_kpi / deposit_summary -> ops.dq_results
         └─> Gold ML  (one row per customer per snapshot date, point-in-time correct)
               feature table: income, age, segment, card grade/limit, spend 3/6/12m,
               spend by MCC group, cross-border ratio, revolving, max DPD, product holding
               label: buys insurance within next N days  -> MLflow model (cross-sell)
```
