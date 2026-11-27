# Reference: legacy dashboards (to rebuild in Gold + Databricks SQL)

Source code of three dashboards I built earlier with **Flask + PostgreSQL + Chart.js /
ApexCharts**. They are kept here only as the **specification** for the Gold layer and the
Databricks SQL dashboards of this project — they are not deployed and not part of CI.

- No data files are included. Sample input files are produced by `data_generator/`.
- Passwords and internal hosts were replaced by placeholders (`<PG_PASSWORD>`,
  `<internal-host>`).
- Third-party JS libraries (Chart.js, ApexCharts, flatpickr, papaparse) were not copied;
  load them from a CDN if you want to open the HTML locally.

| Folder | Dashboard | Main files | Gold target (see `docs/gold_dashboards.md`) |
|---|---|---|---|
| `card/` | Card Business Dashboard – Performance & Growth | `dashboard.html`, `dashboard.js`, `api_server.py` (`/api/dashboard/full-stats`) | `mart_card_kpi_monthly`, `mart_card_spend_mix` |
| `overdue/` | Retail loan overdue tracking (LOAN) | `dashboard.html`, `dashboardv200.js`, `api_server.py` (`/ticker*`, `/bar_line_chart`, `/overdue_*`) | `mart_loan_overdue_daily` |
| `deposit/` | Retail Deposit Performance (DEPOSIT) | `dashboard.html`, `dashboardv2.js`, `api_server.py` (`/api/gauge-*`, `/api/account-group-table`, `/api/balance-by-*`, `/api/casa-*`, `/api/edusave-newopen`) | `mart_deposit_daily`, `mart_deposit_by_tenor`, `mart_casa_customer` |

How to use when building Gold:
1. Read the SQL inside each `api_server.py` endpoint — it defines the metric
   (e.g. overdue ratio = `SUM(ovd_bal) / SUM(port_bal)` by `report_dt`, `branch_code`,
   `product_type`, `del_grp`).
2. Re-implement it as a Gold table/view over Silver (Spark SQL), one view per chart.
3. Build the visual in a Databricks SQL dashboard with the same filters
   (date, region, branch, product, customer type) and comparisons (D-1, M-1, Y-1, % target).
