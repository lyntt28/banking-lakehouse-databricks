# Overdue Dashboard — business rules, data flow, runbook

Dashboard: `dashboards/loan_overdue.lvdash.json` (generated from `src/common/lakeview/overdue.py`).
Source file: `credit_card_overdue_YYYYMMDD.txt` → `silver.card_overdue_source` →
`gold.card_mart_overdue_daily` → `gold_serving.card_mart_overdue_daily` +
`gold_serving.overdue_report_calendar`.

```
Generator → Landing (UC Volume) → Bronze (Auto Loader, raw STRING + lineage)
  → Silver card_overdue_source (cast, normalize, DQ gate, quarantine, replace_day)
  → Gold card_mart_overdue_daily (SUM by business grain, batch_id) + Silver↔Gold recon gate
  → atomic activation (ops.published_batches) → gold_serving views + overdue_report_calendar
  → dashboard datasets: resolve dates from calendar → filter snapshots → aggregate
```

## 1. Gap analysis (before → after)

The original Flask/PostgreSQL/Chart.js files (`api_server.py`, `dashboardv200.js`, …) are not in
this repository; the rules below come from the refactoring brief that describes them.

| Area | Before | After |
|---|---|---|
| Silver grain | `ovd_key_unique` quarantined every row sharing the business key (would drop the 711 same-key rows in the original sample) | No uniqueness rule until the source contract confirms the grain; rows kept and summed in Gold. Silver written with `replace_day` (MERGE by key cannot take several rows per key). Re-delivered / corrected files are still handled by `latest_delivery`, exact duplicates by `drop_exact_duplicates` |
| DQ | 6 rules, no account-count checks | + invalid report date, account count numeric / non-negative (FAIL), `ovd_acc <= port_acc` (WARN) |
| Gold recon | none for Overdue | Exact Silver = Gold for `port_acc`, `ovd_acc`, `port_bal`, `ovd_bal` (Decimal / BIGINT, no float), part of the publish gate |
| Reporting dates | every dataset ran `max(as_of_date)` / `DISTINCT as_of_date` over the full mart history | `gold_serving.overdue_report_calendar`: published dates + actual D-1 / M-1 / Y-1 dates |
| KPI + compare | 2 datasets each aggregating the whole history (`day` CTE) before choosing dates | Both read only the ≤ 4 needed snapshots; comparison rows show the actual date (`vs Last Month (2026-10-31)`), missing period → `N/A` (not 0) |
| Last 5 days / Monthly | aggregated history; missing values shown as 0 | dates from calendar first; missing → NULL |
| Debt groups | **wrong**: combo chart by month with no `del_grp` breakdown | stacked bar month × `del_grp` (source groups `1-10`, `11-90`, `91-180`, `181-360`, `361+`), one snapshot per month for all groups, year of the report date (not hard-coded 2025), never after report date |
| Product | 2 datasets | amount (all groups) + Top-5 ratio; both read only the current snapshot |
| Branch | **missing** | Top-5 by overdue ratio with original treemap rules (exclude `8125`, overdue > 10,000 USD, branch filter). Lakeview has no treemap in our generator → horizontal bars (ratio + amount) |
| Branch filter options | `DISTINCT mng_br` over full history | branches of the latest published snapshot |

Datasets: `ovd_kpi` (3 KPI + FX + actual date), `ovd_compare`, `ovd_daily_trend`,
`ovd_monthly_trend`, `ovd_debt_groups`, `ovd_product`, `ovd_product_top5`, `ovd_branch_top5`,
`ovd_branch_options` (9; FX merged into `ovd_kpi`). Lakeview cannot apply a per-widget LIMIT /
row filter, so the Top-5 charts keep their own small datasets and the KPI counters (1 row) and
comparison tables (3 rows) stay separate.

## 2. Business rules

- Grain: `as_of_date × product_type × product_group × product_name × mng_br × sal_br × del_grp`.
- Measures are additive and stored in VND (`port_acc`, `ovd_acc`, `port_bal`, `ovd_bal`).
  No ratio is stored.
- Overdue ratio = `SUM(ovd_bal) / NULLIF(SUM(port_bal), 0) * 100`, computed after filtering /
  grouping. Zero denominator → NULL.
- USD = VND / latest USD rate `<= :report_date` (`silver.ref_fx_rates`); the same rate is used for
  every period in one view (as the original). K USD = USD / 1000.
- Branch filter = management branch (`mng_br`).
- Debt groups follow the source file. `181-360` and `361+` may be merged into `181+` if the
  business asks; `11-90` cannot be split without DPD detail. Group `0` (not overdue) is excluded
  from the debt-group chart.

### Date selection (`gold_serving.overdue_report_calendar`)

Only dates whose Overdue batch is **activated** in `ops.published_batches` **and** present in the
Gold table (same condition as the serving view) — not every date in landing / Bronze.

| Period | Rule |
|---|---|
| Current | max published date `<= :report_date` |
| D-1 | previous published date (on month-end-only history this is the previous snapshot; the actual date is displayed) |
| M-1 | last published date inside the previous calendar month, else NULL |
| Y-1 | last published date inside the previous calendar year, else NULL |
| Last 5 days | 5 latest published dates `<= current` |
| Monthly trend | 6 months incl. current: max published date per month, current month = current date |
| Debt groups | Jan of the report year → current month, one snapshot per month shared by all groups |

Note: the previous implementation took M-1 as "latest date `<=` end of previous month" (could
fall back to an older month). The new rule returns NULL when the previous month has no data.

## 3. Data generator

`data_generator/summaries.py::card_overdue` aggregates the evolving card book (state carried
day to day, not re-randomised) and `card_overdue_errors` checks every valid file:
`0 <= OVD_BAL <= PORT_BAL`, `0 <= OVD_ACC <= PORT_ACC`, product name belongs to its group, valid
management / sales branch, known debt group. The Overdue file contains no injected errors;
invalid records for DQ tests live in `tests/test_silver.py`.

Demo history (not a production retention policy; Card / Deposit unchanged):

```bash
python -m data_generator.generate --start-date 2026-10-30 --days 30 --history-months 13
```

## 4. Runbook

- **Gate failed "Đối soát Overdue Silver-Gold lệch: …"**: the batch is not activated (dashboard
  keeps the previous valid batch). Compare `SUM(...)` of `silver.card_overdue_source` and
  `gold_stage.card_mart_overdue_daily` for the day; usually a schema / cast change in
  `build_mart_card_overdue_daily`.
- **Dashboard shows an older date than selected**: expected when the selected date has no
  activated batch. Check `SELECT * FROM gold_serving.overdue_report_calendar ORDER BY report_date DESC`.
- **Correction file**: re-run Silver + Gold for the day; the new batch replaces the old one
  atomically, the calendar is unchanged.
- **Source confirms unique grain**: add back `Rule("ovd_key_unique", "_dup_key_count = 1")` in
  `CARD_OVERDUE` (severity FAIL).

## 5. Performance

Compare `overdue_dashboard_old_history_scan` and `overdue_dashboard_new_calendar_first` in
`common.maintenance.BENCHMARKS` on a SQL Warehouse (cold + warm, Query Profile: time, bytes /
files read, shuffle). Check `DESCRIBE DETAIL` (numFiles / sizeInBytes) before OPTIMIZE; liquid
clustering on `(as_of_date, mng_br)` only if profiles show significant file over-read.
No benchmark numbers have been recorded yet — they need a Databricks workspace.

## 6. Tests

`tests/test_overdue_dashboard.py` (multi-date business logic), `tests/test_lakeview.py`
(every dataset runs, widget fields exist, layout), `tests/test_serving.py`
(calendar = activated batches only), `tests/test_silver.py` (same-key rows kept, DQ rules),
`tests/test_ops.py` (gate blocks Overdue recon mismatch), `tests/test_data_generator.py`
(business rules + continuity).
