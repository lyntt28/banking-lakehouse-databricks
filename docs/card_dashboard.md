# Card Business Dashboard — metric definitions, Gold, migration

Dashboard: `dashboards/card_business.lvdash.json` (from `src/common/lakeview/card.py`).
Marts: `card_mart_kpi_daily`, `card_mart_kpi_monthly` (rebuilt), `card_mart_spend_mix`
(`src/common/gold_cards.py`), read through `gold_serving` + `gold_serving.card_report_calendar`.

## 1. Metric definitions

| Metric | Type | Definition |
|---|---|---|
| New Acquisition | flow | cards opened from day 1 of the month to report date T (MTD) |
| Customer Base | stock | ACTIVE cards at T |
| Using Customer | distinct | **distinct cards** with a valid transaction from day 1 to T (never SUM / MAX of daily counts). Unit = card (`card_no`, as source file 02_member_usage), not customer |
| Sales Volume | flow | MTD spending |
| Balance / Revolving | stock | outstanding / revolving balance at T; revolving is a subset → grouped bars, never stacked |
| Spend per Member | ratio | MTD spending / EOP cards (original `calculate_avg_spending_per_member`) |
| Spending distribution | flow | MTD sector / location / channel amounts (customer-type filter applies) |
| DPD 1+/10+/30+, NPL 90+ | stock ratio | Σ balance of cards with DPD ≥ n / Σ balance; changes shown in percentage points (pp) |

Comparisons: **vs Last Month** = previous month (month-end stock, full-month flow);
**vs Last Year-end** = December of the previous year (as the original API — not same-period YoY).
Trends: 6 points = 5 previous months + the report month (current month stops at T).

Completeness: a month's flow (and YTD) is shown only when every day of the period has data
(`flow_complete`, `ytd_complete`); otherwise `N/A`. History generated as month-end snapshots
only gives correct stock but incomplete flows — those months show `N/A` for New / Using /
Sales instead of treating one day as a month.

## 2. Targets
Codes `CARD_<KPI>_<CN|TC>` for the 5 Overview KPIs (branch × month / year; Total = CN + TC):

| KPI | Month target | Year achievement |
|---|---|---|
| NEW_ACQ, SALES_VOLUME (flow) | MTD / month target | YTD / annual target (Σ 12 months) |
| CUSTOMER_BASE, BALANCE (stock) | current / month-end target | current / year-end target (December, not summed) |
| USING (distinct) | MTD distinct / month target | none (N/A) |

The customer-type filter applies to both actual and target.

## 3. Gold changes
- `card_mart_kpi_monthly`: grain `as_of_date × year_month × branchnbr × cust_type ×
  acquisition_channel`, months from January of the previous year to T. Columns: stock at
  `snapshot_date`, MTD flows, `using_members` (distinct MTD), `days_with_data / days_expected`,
  `flow_complete`, `is_month_closed`, `*_ytd`, `ytd_complete`. Targets no longer joined into the
  mart (done in dashboard SQL with the right customer type).
- Incremental: each day recomputes only the report month (daily mart rows of the month + MTD
  transactions); closed months are copied from the previous publish date's monthly rows. First
  run / old table without `flow_complete` → rebuilt from daily history + transactions.
- Reconciliation: `New_Total` renamed `New_Member` (source code); `Avg_Spending_Per_Member` =
  spending / EOP members (generator and Gold).

## 4. Data generator limits
The generator evolves card state one step per generated date. Historical months generated as
month-end snapshots do not contain the month's openings / closures / transactions, so flows for
those months are flagged incomplete. For a demo with complete 6-month flows, generate daily data
for those months (`--days` covering them, small `--scale`).

## 5. Migration
1. Deploy; `card_mart_kpi_monthly` schema changes (new grain) — the pipeline rebuilds it on the
   first run (old rows lack `flow_complete`).
2. Reload `ref_targets` with `CARD_*_CN/_TC` codes; old `CARD_NEW_ACQ` / `CARD_SALES_VOLUME`
   rows are ignored.
3. Backfill once after all three dashboards are merged.

## 6. Tests
`tests/test_card_logic.py` (distinct MTD, stock vs flow, mid-month, incremental = rebuild,
KPI / compare / targets by customer type / year-end, spend per member, pp risk change,
grouped balance chart), `tests/test_gold_cards.py`, `tests/test_lakeview.py`,
`tests/test_dashboard_views.py`.
