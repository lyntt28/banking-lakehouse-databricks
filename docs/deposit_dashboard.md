# Retail Deposit Dashboard — business rules, Gold marts, migration

Dashboard: `dashboards/retail_deposit.lvdash.json` (generated from `src/common/lakeview/deposit.py`).
Marts: `src/common/gold_deposit.py`, wired in `src/pipelines/gold.py`.

```
Generator (account-level daily + month-end, FX, growth targets)
  → Bronze → Silver (core_accounts, core_customers SCD2, ref_targets, ref_fx_rates)
  → Gold: dep_mart_trend (daily snapshot + D1/M1/Y1 + MTD avg)
          dep_mart_monthly (month-end / report-date snapshot + monthly avg)   [NEW]
          dep_mart_by_tenor (balance + weighted-rate numerator/denominator)
          dep_mart_casa_customer (customers as of date + region)
          dep_mart_new_accounts (each opening once, <= report date, monthly + YTD)
  → recon gate (bang1247) → atomic publish → gold_serving + deposit_report_calendar
  → dashboard: resolve ONE published date → read that snapshot → aggregate
```

## 1. Metric definitions

| Metric | Definition |
|---|---|
| Ending Balance | Σ `balance_vnd` at report date T |
| Monthly Avg Balance | month-to-date average from day 1 to T (`monthly_avg_balance_vnd`). Requires a continuous daily snapshot chain; otherwise NULL (`monthly_avg_complete = false`), never replaced by the ending balance |
| Retail Customers | retail (`cust_type = 'CN'`) customers at T, customer version effective at T |
| vs Last Day / Month / Year | **absolute** difference vs D-1 (previous published date), M-1 (last date in previous month), Y-1 (last date in previous year); actual date shown, `N/A` when the snapshot does not exist |
| Target Achievement | YTD growth (`Σ diff_y1` = balance T − balance at previous year-end) / annual growth target `DEPOSIT_GROWTH_ALL / _CASA / _TD_ISA` (branch × year; region / bank = Σ branches) |
| Daily Balance | Σ balance per published day in [T−29, T] |
| Monthly Ending Balance – Year Comparison | month-end balance per month (current month = T), this year vs last year. The original chart was named "Monthly Average Balance (MoM)" but its API summed `bal` at month-end, so it is renamed |
| Deposit by Term | balance by tenor bucket; rate = Σ(rate × balance) / Σ balance (never AVG of averages) |
| CASA Distribution | Non-CASA, CASA Active (≥ 1 active CASA account), CASA Dormant; active + dormant = CASA ≤ retail |
| K-Study (Edu-Save) | previous-year total + current-year cumulative new accounts by month (YTD resets in January) |
| Portion | balance / grand total within the current filters (recomputed, `share_pct` not summed) |

FX: balances are stored in VND; the dashboard converts to USD 1,000 with the latest USD rate
`<=` the resolved report date, the same rate for every comparison period.

### Comparison semantics (all marts)
- Comparison snapshot missing → NULL.
- Snapshot exists but dimension combination absent → 0 (product closed / not yet opened). The
  trend mart keeps rows for combinations that still had balance at D-1 / M-1 / Y-1 (balance 0
  today) so closed products are not lost.

### Filters (as in the original)

| Visual | Date | Region | Branch | Product (group class) | Currency | Nationality |
|---|:-:|:-:|:-:|:-:|:-:|:-:|
| Tickers / compare / gauges | ✓ | ✓ | ✓ | | | |
| Daily Balance | ✓ | ✓ | ✓ | ✓ | | |
| Monthly Ending Balance | ✓ | ✓ | ✓ | | | |
| Regional Distribution | ✓ | ✓ | ✓ | ✓ | | |
| Deposit by Term, CASA, K-Study | ✓ | ✓ | ✓ | | | |
| Balance by Product | ✓ | ✓ | ✓ | | ✓ | ✓ |

## 2. Gold changes

| Table | Change |
|---|---|
| `dep_mart_trend` | `compare_with_history` (range join + window over all history) replaced: resolve D1/M1/Y1 dates from the published dates, read only those 3 snapshots, equality join on the dimension key. New columns: `monthly_avg_balance_vnd`, `monthly_avg_complete`, `monthly_avg_balance_{d1,m1,y1}`, `comparison_date_{d1,m1,y1}`. MTD avg is incremental: `avg_T = (avg_{T−1}·(n−1) + bal_T) / n` (reads only D-1) |
| `dep_mart_monthly` (new) | grain `as_of_date × month_id × dims`; months from January of previous year to T's month. Incremental: copies closed months from the previous publish date's monthly rows, recomputes only the current month; first run bootstraps from `dep_mart_trend` history |
| `dep_mart_by_tenor` | + `tenor_bucket`, `tenor_sort_order` (`[0,1) [1,6) [6,12) [12,24) [24,∞)` months), `interest_weighted_numerator/denominator`, `region` |
| `dep_mart_casa_customer` | customers effective at T (SCD2 `valid_from/valid_to`, not `is_current`), + `region`, `non_casa_customers` |
| `dep_mart_new_accounts` | dedupe by `account_id`, `open_date <= T`, from January of previous year, + `region` |
| targets | `DEPOSIT_BALANCE/CASA/TD_ISA` (balance targets) replaced by `DEPOSIT_GROWTH_*` (YEAR rows only) |

## 3. Migration

1. Deploy code. Affected Gold tables: `dep_mart_trend`, `dep_mart_by_tenor`,
   `dep_mart_casa_customer`, `dep_mart_new_accounts` (new columns, appended with
   `mergeSchema`), new `dep_mart_monthly`, new view `gold_serving.deposit_report_calendar`.
2. Old trend rows have NULL monthly-average columns → monthly averages show `N/A` until rebuilt.
3. Targets: reload `ref_targets` with growth targets (regenerate targets file), otherwise gauges
   are NULL.
4. Rebuild the history in one replay **after** the three dashboards are refactored (see runbook
   "replay"), on a small dataset first, then full history.

## 4. Performance

No numbers are recorded yet (needs a SQL Warehouse). Measure per dataset: execution time
(cold/warm separately from warehouse start-up), bytes/files read, query count. Every dataset now
reads one snapshot (or a 30-day / 2-year monthly window) instead of aggregating full history.

## 5. Tests
`tests/test_deposit_logic.py` (D1/M1/Y1, closed + new products, missing snapshot, MTD average,
monthly mart no-future, weighted rate, CASA as-of, duplicate openings + YTD reset, dashboard
tickers / compare / gauges / grouping sets / daily window), `tests/test_gold_deposit.py`,
`tests/test_lakeview.py`, `tests/test_dashboard_views.py`, `tests/test_data_generator.py`.
