# Gold BI: dashboards to rebuild on Databricks SQL

Source: three dashboards previously built with Flask + PostgreSQL + Chart.js/ApexCharts
(CARD, RAMC, RCD). Gold must provide one table/view per visual so each chart is a
plain `SELECT` with filters.

Common filters: report date, region, branch, product, customer type (consumer/corporate).
Common comparisons: vs last day (D-1), last month-end (M-1), last year-end/same day (Y-1),
% of month target, % of year target.

## 1. Card Business Dashboard (Performance & Growth)

| Section | Visual | Metric | Gold source |
|---|---|---|---|
| Overview | 5 KPI cards with MoM, YoY, % month/year target | New Acquisition, Customer Base (EOP members), Using Customer, Sales Volume (K USD), Balance (K USD) | `mart_card_kpi_monthly` + `card_targets` |
| New Issuance vs Termination | stacked bar + total line, 7 months | New cards by channel (Smart Credit / Branches) | `mart_card_kpi_monthly` (by acquisition_channel) |
| | clustered bar, 7 months | In (new) vs Out (closed) | same |
| Sales Performance | bar, 7 months | Customer Base vs Using Members | same |
| | bar, 7 months | Balance vs Revolving balance | same |
| | bar, 7 months | Sales Volume | same |
| | line, 7 months | Spend per customer (ticket size) | same |
| Spending Distribution | 3 donuts | By sector (Purchasing / Installment / Cash advance / Flexi), by location (Domestic / X-Border), by channel (POS / Online) | `mart_card_spend_mix` |
| Delinquency | 4 KPI | DPD 1+, 10+, 30+, NPL 90+ ratio (balance-based) | `mart_card_kpi_monthly` |

Reconciliation: same metric codes as `card_kpi` source files → `ops.dq_results`.

## 2. Overdue Dashboard (retail loan overdue tracking, RAMC)

| Visual | Metric | Gold source |
|---|---|---|
| 3 tickers | Overdue amount, overdue ratio = overdue balance / portfolio balance, vs D-1 / M-1 / Y-1 | `mart_loan_overdue_daily` |
| Bar + line, daily trend (last N days) and monthly | Overdue amount (bar), overdue ratio % (line) | same |
| Donut by debt group | Overdue balance by debt group 2–5 | same, `GROUP BY debt_group` |
| Stacked bar by month x debt group | Overdue balance | same |
| Bar by product group | Overdue amount / ratio | same + `dim_product` |
| Branch ranking + treemap | Overdue amount and ratio by branch | same + `dim_branch` |

Definitions: portfolio balance = `ducuoi` (VND equivalent), overdue balance = balance of
loans with DPD > 0, debt group from DPD (TT 11/2021, `common.banking`).

## 3. Retail Deposit Performance Dashboard (RCD)

| Visual | Metric | Gold source |
|---|---|---|
| 3 gauges | % target: All products, CASA, TDA/ISA | `mart_deposit_daily` + `deposit_targets` |
| Account group table | Balance, share %, diff D-1 / M-1 / Y-1, month average balance | `mart_deposit_daily` (same layout as `bang1247`) |
| Balance by region / by product, stacked bar by group class | Balance | same |
| CASA donut | CASA share by account group | same |
| Daily balance chart | Balance trend | same |
| Deposit by term and interest rate | Balance, min/max/avg rate by tenor | `mart_deposit_by_tenor` |
| CASA customer distribution | Retail customers, CASA customers, active vs dormant | `mart_casa_customer` |
| Edu-Save new open accounts (cumulative by month) | New study-abroad accounts | `mart_deposit_new_accounts` |

Filters: region, currency, nationality, group class (1-CASA / 2-TDA / 3-ISA).

Reconciliation: `mart_deposit_daily` vs `deposit_summary` (bang1247) source files.

## Data the generator still needs for these dashboards

| Gap | Needed by | Change |
|---|---|---|
| ≥ 13 months of history (MoM/YoY, 7-month trends) | all three | backfill mode: month-end snapshots for past 13 months + daily for recent days |
| Targets (month / year) | Card KPI cards, RCD gauges | `targets` table: metric, period, branch, target value |
| Card acquisition channel | New Acquisition chart | `acquisition_channel` in `cards` (SMART_CREDIT / BRANCH) |
| Customer nationality | RCD filter, bang1247 NATION | `nationality` in `customers` |
| Deposit tenor + 3-ISA class, Edu-Save product | RCD tenor chart, Edu-Save chart | `tenor_months`, product `EDUSAVE`, ISA group in `accounts` |
| Amounts in USD (K USD) | Card dashboard | FX rate table `fx_rates` (VND → USD) |
