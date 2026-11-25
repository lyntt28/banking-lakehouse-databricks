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
| K-Study new open accounts (cumulative by month) | New study-abroad accounts | `mart_deposit_new_accounts` |

Filters: region, currency, nationality, group class (1-CASA / 2-TDA / 3-ISA).

Reconciliation: `mart_deposit_daily` vs `deposit_summary` (bang1247) source files.

## Built so far (`src/common/gold_cards.py`)

| Table | Status |
|---|---|
| `mart_card_kpi_daily` | done: members EOP/new/closed/using, balance, revolving, spend by type/location/channel, DPD 1+/10+/30+/90+, by branch x customer type x acquisition channel |
| `mart_card_spend_mix` | done |
| `mart_card_kpi_monthly` | done: stock = last day of month, flow = month sum, MoM / YoY %, % of month / year target (`targets`) |
| `ops.recon_card_kpi` | done: all 45 `card_kpi` metric codes vs Gold; stock metrics exact, transaction-based within 1% (see ADR-007) |
| `ops.recon_deposit` | done: `mart_deposit_daily` vs `deposit_summary` per group x currency x branch, exact |
| `mart_deposit_trend` | done: balance by account group x class x currency x region, share %, diff vs D-1 / M-1 / Y-1 (`src/common/gold_deposit.py`) |
| `mart_deposit_by_tenor` | done: TD / ISA by tenor, min / max / avg / balance-weighted rate |
| `mart_casa_customer` | done: retail customers, CASA customers, active vs dormant, penetration % |
| `mart_deposit_new_accounts` | done: new accounts by open month + YTD (filter `KSTUDY` for K-Study) |
| Dashboards on Databricks SQL | not yet: needs a workspace run |

## Data the generator still needs for these dashboards

| Gap | Needed by | Change |
|---|---|---|
| ~~≥ 13 months of history~~ | done | `generate(..., history_months=13)`: month-end snapshots before the daily range; load with notebook `00_setup/05_backfill.py` |
| ~~Targets (month / year)~~ | done | `targets` table: metric, period, branch, target value |
| ~~Card acquisition channel~~ | done | `acquisition_channel` in `cards` (SMART_CREDIT / BRANCH) |
| ~~Customer nationality~~ | done | `nationality` in `customers` |
| ~~Deposit tenor + 3-ISA class, K-Study product~~ | done | `tenor_months`, products `ISA` (3-ISA) and `KSTUDY` |
| ~~Amounts in USD (K USD)~~ | done | `fx_rates` (rate to VND per day) |
