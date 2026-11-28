"""View SQL cho 3 dashboard: mỗi visual = 1 view, xếp theo thứ tự trên dashboard gốc
(docs/gold_dashboards.md). Dashboard (Databricks SQL / Power BI) chỉ cần SELECT * từ view.

Màu sắc / bố cục / loại biểu đồ KHÔNG nằm ở đây - sẽ dựng theo ảnh chụp dashboard gốc.
Template dùng {catalog}; ngày báo cáo = batch mới nhất ĐÃ PUBLISH (ops.published_batches), không
phải max(as_of_date) của từng mart: publish đang chạy dở / bị rollback thì dashboard vẫn đọc
batch hợp lệ trước đó, mọi visual cùng một ngày.
"""

from __future__ import annotations

# (dashboard, thứ tự, tên view, mô tả visual, SQL)
VIEWS: list[tuple[str, int, str, str, str]] = []


def _v(dash: str, order: int, name: str, desc: str, sql: str) -> None:
    VIEWS.append((dash, order, name, desc, " ".join(sql.split())))


G = "{catalog}.gold_serving"   # view batch đang dùng (common.serving)
# ngày Gold đang phục vụ: chỉ đổi khi publish đủ 17 bảng (pipelines/gold.py)
PUB = "(SELECT max(as_of_date) FROM {catalog}.ops.published_batches)"

# ============================================================ 1. CARD BUSINESS
_CARD_MONTH = f"""
WITH m AS (
  SELECT year_month, max(snapshot_date) AS last_date,
         CASE WHEN bool_and(flow_complete) THEN sum(new_members) END AS new_acquisition,
         sum(eop_members) AS customer_base,
         CASE WHEN bool_and(flow_complete) THEN sum(using_members) END AS using_customers,
         CASE WHEN bool_and(flow_complete) THEN sum(spend_amt) END AS sales_volume,
         sum(balance) AS balance, sum(revolving_balance) AS revolving_balance,
         CASE WHEN bool_and(flow_complete) THEN sum(closed_members) END AS closed_members,
         CASE WHEN bool_and(ytd_complete) THEN sum(new_members_ytd) END AS new_ytd,
         CASE WHEN bool_and(ytd_complete) THEN sum(spend_amt_ytd) END AS sales_ytd
  FROM {G}.card_mart_kpi_monthly WHERE as_of_date = {PUB} GROUP BY year_month)
"""

_v("card", 1, "v_card_kpi_cards",
   "5 KPI cards (MTD flow, report-date stock) + vs last month, vs last year-end, "
   "% month / year target (all customer types)",
   _CARD_MONTH + """
, t AS (
  SELECT substr(metric_code, 1, length(metric_code) - 3) AS code, period_type,
         sum(target_value) AS target
  FROM {catalog}.silver.ref_targets
  WHERE left(metric_code, 5) = 'CARD_'
    AND ((period_type = 'MONTH' AND period = date_format(""" + PUB + """, 'yyyy-MM'))
      OR (period_type = 'YEAR' AND period = CAST(year(""" + PUB + """) AS STRING)))
  GROUP BY 1, 2),
tt AS (SELECT
  max(CASE WHEN code = 'CARD_NEW_ACQ' AND period_type = 'MONTH' THEN target END) AS new_tm,
  max(CASE WHEN code = 'CARD_NEW_ACQ' AND period_type = 'YEAR' THEN target END) AS new_ty,
  max(CASE WHEN code = 'CARD_SALES_VOLUME' AND period_type = 'MONTH' THEN target END) AS sal_tm,
  max(CASE WHEN code = 'CARD_SALES_VOLUME' AND period_type = 'YEAR' THEN target END) AS sal_ty
  FROM t),
x AS (SELECT * FROM m WHERE year_month = date_format(""" + PUB + """, 'yyyy-MM')),
lm AS (SELECT * FROM m
       WHERE year_month = date_format(add_months(""" + PUB + """, -1), 'yyyy-MM')),
ye AS (SELECT * FROM m
       WHERE year_month = concat(CAST(year(""" + PUB + """) - 1 AS STRING), '-12'))
SELECT x.year_month, x.last_date,
  x.new_acquisition, x.customer_base, x.using_customers, x.sales_volume, x.balance,
  (x.new_acquisition - lm.new_acquisition) / nullif(lm.new_acquisition, 0) * 100 AS new_mom_pct,
  (x.customer_base - lm.customer_base) / nullif(lm.customer_base, 0) * 100 AS base_mom_pct,
  (x.sales_volume - lm.sales_volume) / nullif(lm.sales_volume, 0) * 100 AS sales_mom_pct,
  (x.balance - lm.balance) / nullif(lm.balance, 0) * 100 AS balance_mom_pct,
  (x.customer_base - ye.customer_base) / nullif(ye.customer_base, 0) * 100
      AS base_vs_year_end_pct,
  (x.balance - ye.balance) / nullif(ye.balance, 0) * 100 AS balance_vs_year_end_pct,
  x.new_acquisition / nullif(tt.new_tm, 0) * 100 AS new_vs_month_target_pct,
  x.new_ytd / nullif(tt.new_ty, 0) * 100 AS new_vs_year_target_pct,
  x.sales_volume / nullif(tt.sal_tm, 0) * 100 AS sales_vs_month_target_pct,
  x.sales_ytd / nullif(tt.sal_ty, 0) * 100 AS sales_vs_year_target_pct
FROM x LEFT JOIN lm ON true LEFT JOIN ye ON true CROSS JOIN tt
""")

_v("card", 2, "v_card_new_by_channel_7m",
   "Stacked bar + total line, 7 months: new cards by channel (Smart Credit / Branches)",
   f"""
SELECT year_month, acquisition_channel,
       CASE WHEN bool_and(flow_complete) THEN sum(new_members) END AS new_cards
FROM {G}.card_mart_kpi_monthly
WHERE as_of_date = {PUB}
  AND year_month > date_format(add_months({PUB}, -7), 'yyyy-MM')
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
       sales_volume / nullif(customer_base, 0) AS spend_per_member FROM m
WHERE year_month > date_format(add_months(to_date(concat((SELECT max(year_month) FROM m),
  '-01')), -7), 'yyyy-MM')
""")

_v("card", 5, "v_card_spend_distribution",
   "3 donuts: by sector (Purchasing / Installment / Cash advance / Flexi), "
   "by location (Domestic / X-Border), by channel (POS / Online)",
   f"""
SELECT 'sector' AS donut, txn_type AS slice, sum(amount) AS amount
FROM {G}.card_mart_spend_mix
WHERE as_of_date = {PUB} GROUP BY txn_type
UNION ALL
SELECT 'location', location, sum(amount) FROM {G}.card_mart_spend_mix
WHERE as_of_date = {PUB} GROUP BY location
UNION ALL
SELECT 'channel', channel, sum(amount) FROM {G}.card_mart_spend_mix
WHERE as_of_date = {PUB} GROUP BY channel
""")

_v("card", 6, "v_card_delinquency",
   "4 KPI: DPD 1+, 10+, 30+, NPL 90+ ratio (balance-based)",
   f"""
SELECT as_of_date,
  sum(dpd_1p_balance) / nullif(sum(balance), 0) * 100 AS dpd_1p_ratio,
  sum(dpd_10p_balance) / nullif(sum(balance), 0) * 100 AS dpd_10p_ratio,
  sum(dpd_30p_balance) / nullif(sum(balance), 0) * 100 AS dpd_30p_ratio,
  sum(npl_90p_balance) / nullif(sum(balance), 0) * 100 AS npl_90p_ratio
FROM {G}.card_mart_kpi_daily
WHERE as_of_date = {PUB}
GROUP BY as_of_date
""")

# ============================================================ 2. OVERDUE (LOAN)
_LAST_LOAN = PUB

_v("overdue", 1, "v_overdue_tickers",
   "3 tickers: overdue amount, overdue ratio, vs D-1 / M-1 / Y-1",
   f"""
SELECT as_of_date, overdue_balance, portfolio_balance, overdue_ratio * 100 AS overdue_ratio_pct,
  (overdue_ratio - overdue_ratio_d1) * 100 AS ratio_vs_d1_pp,
  (overdue_ratio - overdue_ratio_m1) * 100 AS ratio_vs_m1_pp,
  (overdue_ratio - overdue_ratio_y1) * 100 AS ratio_vs_y1_pp,
  npl_ratio * 100 AS npl_ratio_pct
FROM {G}.loan_mart_npl_daily
WHERE as_of_date = {PUB}
""")

_v("overdue", 2, "v_overdue_daily_trend",
   "Bar + line, last 30 days: overdue amount (bar), overdue ratio % (line)",
   f"""
SELECT as_of_date, overdue_balance, overdue_ratio * 100 AS overdue_ratio_pct
FROM {G}.loan_mart_npl_daily
WHERE as_of_date > {PUB} - INTERVAL 30 DAYS AND as_of_date <= {PUB}
""")

_v("overdue", 3, "v_overdue_monthly_trend",
   "Bar + line by month (last data day of each month)",
   f"""
SELECT date_format(as_of_date, 'yyyy-MM') AS year_month, overdue_balance,
       overdue_ratio * 100 AS overdue_ratio_pct
FROM (SELECT *, row_number() OVER (PARTITION BY date_format(as_of_date, 'yyyy-MM')
                                   ORDER BY as_of_date DESC) AS rn
      FROM {G}.loan_mart_npl_daily) WHERE rn = 1
""")

_v("overdue", 4, "v_overdue_by_debt_group",
   "Donut: overdue balance by debt group 2-5",
   f"""
SELECT debt_group, sum(overdue_balance) AS overdue_balance
FROM {G}.loan_mart_overdue_daily
WHERE as_of_date = {_LAST_LOAN} AND debt_group BETWEEN 2 AND 5 GROUP BY debt_group
""")

_v("overdue", 5, "v_overdue_month_x_group",
   "Stacked bar: overdue balance by month x debt group",
   f"""
SELECT date_format(as_of_date, 'yyyy-MM') AS year_month, debt_group,
       sum(overdue_balance) AS overdue_balance
FROM (SELECT *, dense_rank() OVER (PARTITION BY date_format(as_of_date, 'yyyy-MM')
                                   ORDER BY as_of_date DESC) AS rk
      FROM {G}.loan_mart_overdue_daily)
WHERE rk = 1 AND debt_group >= 2 GROUP BY 1, 2
""")

_v("overdue", 6, "v_overdue_by_product_group",
   "Bar: overdue amount / ratio by product group",
   f"""
SELECT product_group, sum(overdue_balance) AS overdue_balance,
       sum(overdue_balance) / nullif(sum(portfolio_balance), 0) * 100 AS overdue_ratio_pct
FROM {G}.loan_mart_overdue_daily WHERE as_of_date = {_LAST_LOAN} GROUP BY product_group
""")

_v("overdue", 7, "v_overdue_branch_rank",
   "Branch ranking + treemap: overdue amount and ratio by branch",
   f"""
SELECT o.branchnbr, b.br_name, b.region, sum(o.overdue_balance) AS overdue_balance,
       sum(o.overdue_balance) / nullif(sum(o.portfolio_balance), 0) * 100 AS overdue_ratio_pct,
       rank() OVER (ORDER BY sum(o.overdue_balance) DESC) AS rank_by_amount
FROM {G}.loan_mart_overdue_daily o LEFT JOIN {G}.dim_branch b ON o.branchnbr = b.branchnbr
WHERE o.as_of_date = {_LAST_LOAN} GROUP BY o.branchnbr, b.br_name, b.region
""")

# ============================================================ 3. RETAIL DEPOSIT (DEPOSIT)
_LAST_DEP = PUB

_v("deposit", 1, "v_deposit_gauges",
   "3 gauges: YTD growth (vs previous year-end) / annual growth target (DEPOSIT_GROWTH_*), "
   "All products, CASA, TDA/ISA",
   f"""
WITH g AS (
  SELECT sum(diff_y1) AS all_growth,
         sum(CASE WHEN group_class = '1-CASA' THEN diff_y1 END) AS casa_growth,
         sum(CASE WHEN group_class IN ('2-TD', '3-ISA') THEN diff_y1 END) AS td_isa_growth
  FROM {G}.dep_mart_trend WHERE as_of_date = {_LAST_DEP}),
t AS (
  SELECT sum(CASE WHEN metric_code = 'DEPOSIT_GROWTH_ALL' THEN target_value END) AS all_t,
         sum(CASE WHEN metric_code = 'DEPOSIT_GROWTH_CASA' THEN target_value END) AS casa_t,
         sum(CASE WHEN metric_code = 'DEPOSIT_GROWTH_TD_ISA' THEN target_value END) AS td_isa_t
  FROM {{catalog}}.silver.ref_targets
  WHERE period_type = 'YEAR' AND period = CAST(year({_LAST_DEP}) AS STRING))
SELECT g.*, t.*, all_growth / nullif(all_t, 0) * 100 AS all_achievement_pct,
       casa_growth / nullif(casa_t, 0) * 100 AS casa_achievement_pct,
       td_isa_growth / nullif(td_isa_t, 0) * 100 AS td_isa_achievement_pct
FROM g CROSS JOIN t
""")

_v("deposit", 2, "v_deposit_account_group_table",
   "Account group table: balance, share %, diff D-1 / M-1 / Y-1, month-to-date average "
   "balance (bang1247 layout)",
   f"""
SELECT group_class, account_group, sum(balance_vnd) AS balance,
       CAST(sum(balance_vnd) AS DOUBLE) / sum(CAST(sum(balance_vnd) AS DOUBLE)) OVER () * 100
           AS share_pct,
       sum(diff_d1) AS diff_d1, sum(diff_m1) AS diff_m1, sum(diff_y1) AS diff_y1,
       CASE WHEN bool_and(monthly_avg_complete) THEN sum(monthly_avg_balance_vnd) END
           AS month_avg_balance
FROM {G}.dep_mart_trend WHERE as_of_date = {_LAST_DEP}
GROUP BY group_class, account_group
""")

_v("deposit", 3, "v_deposit_by_region_product",
   "Balance by region / by product, stacked bar by group class",
   f"""
SELECT region, group_class, account_group, sum(balance_vnd) AS balance
FROM {G}.dep_mart_trend WHERE as_of_date = {_LAST_DEP} GROUP BY 1, 2, 3
""")

_v("deposit", 4, "v_deposit_casa_donut", "CASA donut: CASA share by account group",
   f"""
SELECT account_group, sum(balance_vnd) AS balance
FROM {G}.dep_mart_trend WHERE as_of_date = {_LAST_DEP} AND group_class = '1-CASA'
GROUP BY account_group
""")

_v("deposit", 5, "v_deposit_daily_balance", "Daily balance trend",
   f"""
SELECT as_of_date, group_class, sum(balance_vnd) AS balance
FROM {G}.dep_mart_trend
WHERE as_of_date BETWEEN {_LAST_DEP} - INTERVAL 29 DAYS AND {_LAST_DEP} GROUP BY 1, 2
""")

_v("deposit", 9, "v_deposit_monthly_ending",
   "Monthly ending balance (month-end snapshot, current month = report date) + monthly avg",
   f"""
SELECT month_id, snapshot_date, group_class, sum(ending_balance_vnd) AS ending_balance,
       CASE WHEN bool_and(monthly_avg_complete) THEN sum(monthly_avg_balance_vnd) END
           AS monthly_avg_balance,
       bool_and(is_month_closed) AS is_month_closed
FROM {G}.dep_mart_monthly WHERE as_of_date = {_LAST_DEP}
GROUP BY month_id, snapshot_date, group_class
""")

_v("deposit", 6, "v_deposit_by_tenor",
   "Deposit by term and interest rate: balance, min / max / avg rate by tenor",
   f"""
SELECT tenor_bucket, tenor_months, group_class, sum(balance_vnd) AS balance,
       min(min_rate) AS min_rate, max(max_rate) AS max_rate,
       sum(interest_weighted_numerator) / nullif(sum(interest_weighted_denominator), 0)
           AS avg_rate
FROM {G}.dep_mart_by_tenor
WHERE as_of_date = {PUB} GROUP BY 1, 2, 3
""")

_v("deposit", 7, "v_casa_customer_distribution",
   "CASA customer distribution: retail customers, CASA customers, active vs dormant",
   f"""
SELECT c.branchnbr, b.br_name, c.region, c.retail_customers, c.casa_customers,
       c.casa_active_customers, c.casa_dormant_customers, c.non_casa_customers,
       c.casa_penetration_pct
FROM {G}.dep_mart_casa_customer c LEFT JOIN {G}.dim_branch b ON c.branchnbr = b.branchnbr
WHERE c.as_of_date = {PUB}
""")

_v("deposit", 8, "v_edusave_new_accounts",
   "Edu-Save new open accounts, cumulative by month",
   f"""
SELECT open_month, sum(new_accounts) AS new_accounts,
       -- luỹ kế sau khi cộng chi nhánh (CN không mở TK tháng đó vẫn được tính đúng)
       sum(sum(new_accounts)) OVER (PARTITION BY substr(open_month, 1, 4)
                                    ORDER BY open_month) AS cumulative_ytd
FROM {G}.dep_mart_new_accounts WHERE product_type = 'EDUSAVE' AND as_of_date = {PUB}
GROUP BY open_month
""")


def view_statements(catalog: str) -> list[str]:
    out = []
    for dash, order, name, desc, sql in sorted(VIEWS, key=lambda v: (v[0], v[1])):
        body = sql.replace("{catalog}", catalog)
        comment = f"[{dash} #{order}] {desc}".replace("'", "''")
        out.append(f"CREATE OR REPLACE VIEW {catalog}.gold.{name} "
                   f"COMMENT '{comment}' AS {body}")
    return out
