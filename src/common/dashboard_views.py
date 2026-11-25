"""View SQL cho 3 dashboard: mỗi visual = 1 view, xếp theo thứ tự trên dashboard gốc
(docs/gold_dashboards.md). Dashboard (Databricks SQL / Power BI) chỉ cần SELECT * từ view.

Màu sắc / bố cục / loại biểu đồ KHÔNG nằm ở đây - sẽ dựng theo ảnh chụp dashboard gốc.
Template dùng {catalog}; ngày báo cáo = ngày mới nhất có dữ liệu trong mart.
"""

from __future__ import annotations

# (dashboard, thứ tự, tên view, mô tả visual, SQL)
VIEWS: list[tuple[str, int, str, str, str]] = []


def _v(dash: str, order: int, name: str, desc: str, sql: str) -> None:
    VIEWS.append((dash, order, name, desc, " ".join(sql.split())))


G = "{catalog}.gold"

# ============================================================ 1. CARD BUSINESS
_CARD_MONTH = f"""
WITH m AS (
  SELECT year_month, max(last_date) AS last_date,
         sum(new_members) AS new_acquisition, sum(eop_members) AS customer_base,
         sum(using_members) AS using_customers, sum(spend_amt) AS sales_volume,
         sum(balance) AS balance, sum(revolving_balance) AS revolving_balance,
         sum(closed_members) AS closed_members,
         sum(target_new_month) AS target_new_month, sum(target_sales_month) AS target_sales_month,
         sum(target_new_year) AS target_new_year, sum(target_sales_year) AS target_sales_year,
         sum(new_members_ytd) AS new_ytd, sum(spend_amt_ytd) AS sales_ytd
  FROM {G}.mart_card_kpi_monthly GROUP BY year_month)
"""

_v("card", 1, "v_card_kpi_cards",
   "5 KPI cards: New Acquisition, Customer Base, Using Customer, Sales Volume, Balance "
   "+ MoM, YoY, % month / year target",
   _CARD_MONTH + """
, x AS (
  SELECT m.*,
    lag(new_acquisition) OVER (ORDER BY year_month) AS new_prev_m,
    lag(customer_base) OVER (ORDER BY year_month) AS base_prev_m,
    lag(using_customers) OVER (ORDER BY year_month) AS using_prev_m,
    lag(sales_volume) OVER (ORDER BY year_month) AS sales_prev_m,
    lag(balance) OVER (ORDER BY year_month) AS balance_prev_m
  FROM m)
SELECT x.year_month, x.last_date,
  x.new_acquisition, x.customer_base, x.using_customers, x.sales_volume, x.balance,
  (x.new_acquisition - x.new_prev_m) / nullif(x.new_prev_m, 0) * 100 AS new_mom_pct,
  (x.customer_base - x.base_prev_m) / nullif(x.base_prev_m, 0) * 100 AS base_mom_pct,
  (x.using_customers - x.using_prev_m) / nullif(x.using_prev_m, 0) * 100 AS using_mom_pct,
  (x.sales_volume - x.sales_prev_m) / nullif(x.sales_prev_m, 0) * 100 AS sales_mom_pct,
  (x.balance - x.balance_prev_m) / nullif(x.balance_prev_m, 0) * 100 AS balance_mom_pct,
  (x.new_acquisition - y.new_acquisition) / nullif(y.new_acquisition, 0) * 100 AS new_yoy_pct,
  (x.customer_base - y.customer_base) / nullif(y.customer_base, 0) * 100 AS base_yoy_pct,
  (x.using_customers - y.using_customers) / nullif(y.using_customers, 0) * 100 AS using_yoy_pct,
  (x.sales_volume - y.sales_volume) / nullif(y.sales_volume, 0) * 100 AS sales_yoy_pct,
  (x.balance - y.balance) / nullif(y.balance, 0) * 100 AS balance_yoy_pct,
  x.new_acquisition / nullif(x.target_new_month, 0) * 100 AS new_vs_month_target_pct,
  x.new_ytd / nullif(x.target_new_year, 0) * 100 AS new_vs_year_target_pct,
  x.sales_volume / nullif(x.target_sales_month, 0) * 100 AS sales_vs_month_target_pct,
  x.sales_ytd / nullif(x.target_sales_year, 0) * 100 AS sales_vs_year_target_pct
FROM x LEFT JOIN m y
  ON y.year_month = date_format(add_months(to_date(concat(x.year_month, '-01')), -12), 'yyyy-MM')
WHERE x.year_month = (SELECT max(year_month) FROM m)
""")

_v("card", 2, "v_card_new_by_channel_7m",
   "Stacked bar + total line, 7 months: new cards by channel (Smart Credit / Branches)",
   f"""
SELECT date_format(as_of_date, 'yyyy-MM') AS year_month, acquisition_channel,
       sum(new_members) AS new_cards
FROM {G}.mart_card_kpi_daily
WHERE as_of_date > add_months((SELECT max(as_of_date) FROM {G}.mart_card_kpi_daily), -7)
GROUP BY 1, 2
""")

_v("card", 3, "v_card_in_out_7m", "Clustered bar, 7 months: In (new) vs Out (closed)",
   _CARD_MONTH + """
SELECT year_month, new_acquisition AS cards_in, closed_members AS cards_out FROM m
WHERE year_month > date_format(add_months(to_date(concat((SELECT max(year_month) FROM m),
  '-01')), -7), 'yyyy-MM')
""")

_v("card", 4, "v_card_sales_performance_7m",
   "Sales Performance, 7 months: Customer Base vs Using, Balance vs Revolving, "
   "Sales Volume, Spend per customer (ticket size)",
   _CARD_MONTH + """
SELECT year_month, customer_base, using_customers, balance, revolving_balance, sales_volume,
       sales_volume / nullif(using_customers, 0) AS spend_per_customer FROM m
WHERE year_month > date_format(add_months(to_date(concat((SELECT max(year_month) FROM m),
  '-01')), -7), 'yyyy-MM')
""")

_v("card", 5, "v_card_spend_distribution",
   "3 donuts: by sector (Purchasing / Installment / Cash advance / Flexi), "
   "by location (Domestic / X-Border), by channel (POS / Online)",
   f"""
SELECT 'sector' AS donut, txn_type AS slice, sum(amount) AS amount
FROM {G}.mart_card_spend_mix
WHERE as_of_date = (SELECT max(as_of_date) FROM {G}.mart_card_spend_mix) GROUP BY txn_type
UNION ALL
SELECT 'location', location, sum(amount) FROM {G}.mart_card_spend_mix
WHERE as_of_date = (SELECT max(as_of_date) FROM {G}.mart_card_spend_mix) GROUP BY location
UNION ALL
SELECT 'channel', channel, sum(amount) FROM {G}.mart_card_spend_mix
WHERE as_of_date = (SELECT max(as_of_date) FROM {G}.mart_card_spend_mix) GROUP BY channel
""")

_v("card", 6, "v_card_delinquency",
   "4 KPI: DPD 1+, 10+, 30+, NPL 90+ ratio (balance-based)",
   f"""
SELECT as_of_date,
  sum(dpd_1p_balance) / nullif(sum(balance), 0) * 100 AS dpd_1p_ratio,
  sum(dpd_10p_balance) / nullif(sum(balance), 0) * 100 AS dpd_10p_ratio,
  sum(dpd_30p_balance) / nullif(sum(balance), 0) * 100 AS dpd_30p_ratio,
  sum(npl_90p_balance) / nullif(sum(balance), 0) * 100 AS npl_90p_ratio
FROM {G}.mart_card_kpi_daily
WHERE as_of_date = (SELECT max(as_of_date) FROM {G}.mart_card_kpi_daily)
GROUP BY as_of_date
""")

# ============================================================ 2. OVERDUE (RAMC)
_LAST_LOAN = f"(SELECT max(as_of_date) FROM {G}.mart_loan_overdue_daily)"

_v("overdue", 1, "v_overdue_tickers",
   "3 tickers: overdue amount, overdue ratio, vs D-1 / M-1 / Y-1",
   f"""
SELECT as_of_date, overdue_balance, portfolio_balance, overdue_ratio * 100 AS overdue_ratio_pct,
  (overdue_ratio - overdue_ratio_d1) * 100 AS ratio_vs_d1_pp,
  (overdue_ratio - overdue_ratio_m1) * 100 AS ratio_vs_m1_pp,
  (overdue_ratio - overdue_ratio_y1) * 100 AS ratio_vs_y1_pp,
  npl_ratio * 100 AS npl_ratio_pct
FROM {G}.mart_npl_daily WHERE as_of_date = (SELECT max(as_of_date) FROM {G}.mart_npl_daily)
""")

_v("overdue", 2, "v_overdue_daily_trend",
   "Bar + line, last 30 days: overdue amount (bar), overdue ratio % (line)",
   f"""
SELECT as_of_date, overdue_balance, overdue_ratio * 100 AS overdue_ratio_pct
FROM {G}.mart_npl_daily
WHERE as_of_date > (SELECT max(as_of_date) FROM {G}.mart_npl_daily) - INTERVAL 30 DAYS
""")

_v("overdue", 3, "v_overdue_monthly_trend",
   "Bar + line by month (last data day of each month)",
   f"""
SELECT date_format(as_of_date, 'yyyy-MM') AS year_month, overdue_balance,
       overdue_ratio * 100 AS overdue_ratio_pct
FROM (SELECT *, row_number() OVER (PARTITION BY date_format(as_of_date, 'yyyy-MM')
                                   ORDER BY as_of_date DESC) AS rn
      FROM {G}.mart_npl_daily) WHERE rn = 1
""")

_v("overdue", 4, "v_overdue_by_debt_group",
   "Donut: overdue balance by debt group 2-5",
   f"""
SELECT debt_group, sum(overdue_balance) AS overdue_balance
FROM {G}.mart_loan_overdue_daily
WHERE as_of_date = {_LAST_LOAN} AND debt_group BETWEEN 2 AND 5 GROUP BY debt_group
""")

_v("overdue", 5, "v_overdue_month_x_group",
   "Stacked bar: overdue balance by month x debt group",
   f"""
SELECT date_format(as_of_date, 'yyyy-MM') AS year_month, debt_group,
       sum(overdue_balance) AS overdue_balance
FROM (SELECT *, dense_rank() OVER (PARTITION BY date_format(as_of_date, 'yyyy-MM')
                                   ORDER BY as_of_date DESC) AS rk
      FROM {G}.mart_loan_overdue_daily)
WHERE rk = 1 AND debt_group >= 2 GROUP BY 1, 2
""")

_v("overdue", 6, "v_overdue_by_product_group",
   "Bar: overdue amount / ratio by product group",
   f"""
SELECT product_group, sum(overdue_balance) AS overdue_balance,
       sum(overdue_balance) / nullif(sum(portfolio_balance), 0) * 100 AS overdue_ratio_pct
FROM {G}.mart_loan_overdue_daily WHERE as_of_date = {_LAST_LOAN} GROUP BY product_group
""")

_v("overdue", 7, "v_overdue_branch_rank",
   "Branch ranking + treemap: overdue amount and ratio by branch",
   f"""
SELECT o.branchnbr, b.br_name, b.region, sum(o.overdue_balance) AS overdue_balance,
       sum(o.overdue_balance) / nullif(sum(o.portfolio_balance), 0) * 100 AS overdue_ratio_pct,
       rank() OVER (ORDER BY sum(o.overdue_balance) DESC) AS rank_by_amount
FROM {G}.mart_loan_overdue_daily o LEFT JOIN {G}.dim_branch b ON o.branchnbr = b.branchnbr
WHERE o.as_of_date = {_LAST_LOAN} GROUP BY o.branchnbr, b.br_name, b.region
""")

# ============================================================ 3. RETAIL DEPOSIT (RCD)
_LAST_DEP = f"(SELECT max(as_of_date) FROM {G}.mart_deposit_trend)"

_v("deposit", 1, "v_deposit_gauges",
   "3 gauges: % target All products, CASA, TDA/ISA (targets: total only, class split "
   "needs class-level targets from the source)",
   f"""
WITH bal AS (
  SELECT sum(balance_vnd) AS all_bal,
         sum(CASE WHEN group_class = '1-CASA' THEN balance_vnd END) AS casa_bal,
         sum(CASE WHEN group_class IN ('2-TD', '3-ISA') THEN balance_vnd END) AS td_isa_bal
  FROM {G}.mart_deposit_trend WHERE as_of_date = {_LAST_DEP}),
t AS (
  SELECT sum(target_value) AS year_target FROM {{catalog}}.silver.targets
  WHERE metric_code = 'DEPOSIT_BALANCE' AND period_type = 'YEAR'
    AND period = CAST(year({_LAST_DEP}) AS STRING))
SELECT all_bal, casa_bal, td_isa_bal, year_target,
       all_bal / nullif(year_target, 0) * 100 AS all_vs_target_pct,
       casa_bal / nullif(all_bal, 0) * 100 AS casa_share_pct,
       td_isa_bal / nullif(all_bal, 0) * 100 AS td_isa_share_pct
FROM bal CROSS JOIN t
""")

_v("deposit", 2, "v_deposit_account_group_table",
   "Account group table: balance, share %, diff D-1 / M-1 / Y-1, month average balance "
   "(bang1247 layout)",
   f"""
WITH d AS (
  SELECT as_of_date, account_group, group_class, sum(balance_vnd) AS balance,
         sum(balance_vnd_d1) AS bal_d1, sum(balance_vnd_m1) AS bal_m1,
         sum(balance_vnd_y1) AS bal_y1
  FROM {G}.mart_deposit_trend GROUP BY 1, 2, 3),
avg_m AS (
  SELECT account_group, avg(balance) AS month_avg_balance FROM d
  WHERE date_format(as_of_date, 'yyyy-MM') = date_format({_LAST_DEP}, 'yyyy-MM')
  GROUP BY account_group)
SELECT d.group_class, d.account_group, d.balance,
       CAST(d.balance AS DOUBLE) / sum(CAST(d.balance AS DOUBLE)) OVER () * 100 AS share_pct,
       d.balance - d.bal_d1 AS diff_d1, d.balance - d.bal_m1 AS diff_m1,
       d.balance - d.bal_y1 AS diff_y1, a.month_avg_balance
FROM d LEFT JOIN avg_m a USING (account_group)
WHERE d.as_of_date = {_LAST_DEP}
""")

_v("deposit", 3, "v_deposit_by_region_product",
   "Balance by region / by product, stacked bar by group class",
   f"""
SELECT region, group_class, account_group, sum(balance_vnd) AS balance
FROM {G}.mart_deposit_trend WHERE as_of_date = {_LAST_DEP} GROUP BY 1, 2, 3
""")

_v("deposit", 4, "v_deposit_casa_donut", "CASA donut: CASA share by account group",
   f"""
SELECT account_group, sum(balance_vnd) AS balance
FROM {G}.mart_deposit_trend WHERE as_of_date = {_LAST_DEP} AND group_class = '1-CASA'
GROUP BY account_group
""")

_v("deposit", 5, "v_deposit_daily_balance", "Daily balance trend",
   f"""
SELECT as_of_date, group_class, sum(balance_vnd) AS balance
FROM {G}.mart_deposit_trend GROUP BY 1, 2
""")

_v("deposit", 6, "v_deposit_by_tenor",
   "Deposit by term and interest rate: balance, min / max / avg rate by tenor",
   f"""
SELECT tenor_months, group_class, sum(balance_vnd) AS balance, min(min_rate) AS min_rate,
       max(max_rate) AS max_rate,
       sum(weighted_rate * balance_vnd) / nullif(sum(balance_vnd), 0) AS avg_rate
FROM {G}.mart_deposit_by_tenor
WHERE as_of_date = (SELECT max(as_of_date) FROM {G}.mart_deposit_by_tenor) GROUP BY 1, 2
""")

_v("deposit", 7, "v_casa_customer_distribution",
   "CASA customer distribution: retail customers, CASA customers, active vs dormant",
   f"""
SELECT c.branchnbr, b.br_name, b.region, c.retail_customers, c.casa_customers,
       c.casa_active_customers, c.casa_dormant_customers, c.casa_penetration_pct
FROM {G}.mart_casa_customer c LEFT JOIN {G}.dim_branch b ON c.branchnbr = b.branchnbr
WHERE c.as_of_date = (SELECT max(as_of_date) FROM {G}.mart_casa_customer)
""")

_v("deposit", 8, "v_kstudy_new_accounts",
   "K-Study new open accounts, cumulative by month",
   f"""
SELECT open_month, sum(new_accounts) AS new_accounts, sum(new_accounts_ytd) AS cumulative_ytd
FROM {G}.mart_deposit_new_accounts WHERE product_type = 'KSTUDY' GROUP BY open_month
""")


def view_statements(catalog: str) -> list[str]:
    out = []
    for dash, order, name, desc, sql in sorted(VIEWS, key=lambda v: (v[0], v[1])):
        body = sql.replace("{catalog}", catalog)
        comment = f"[{dash} #{order}] {desc}".replace("'", "''")
        out.append(f"CREATE OR REPLACE VIEW {catalog}.gold.{name} "
                   f"COMMENT '{comment}' AS {body}")
    return out
