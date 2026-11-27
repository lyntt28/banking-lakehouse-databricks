"""Dashboard Retail Deposit (DEPOSIT): chỉ sửa file này khi đổi dashboard này."""

from __future__ import annotations

from common.lakeview.core import USD_RATE, Dash, _arrow_pct, _q, _report_day

# ============================================================ 3. RETAIL DEPOSIT (DEPOSIT)
DEP_COLORS = {   # reference/dashboards/deposit/dashboardv2.js, api_server.py
    "gauge": {"all": "#22AFFF", "casa": "#10B981", "tda": "#F47B20"},
    "ticker": {"bal": "#223A5E", "mavg": "#009688", "cust": "#F4A300"},
    "daily": "#223A5E",
    "mom": {"1-CASA last year": "#FFE082", "1-CASA this year": "#F4A300",
            "2-TD last year": "#80CBC4", "2-TD this year": "#009688",
            "3-ISA last year": "#90A4AE", "3-ISA this year": "#223A5E"},
    "region": {"South 1": "#223A5E", "South 2": "#1A237E", "South 3": "#80CBC4",
               "North 1": "#FFE082", "North 2": "#F4A300", "North": "#FFE082",
               "Central": "#F4A300", "Others": "#F47B20"},
    "term_bar": "#009688", "term_line": "#F4A300",
    "casa": {"Non-CASA": "#223A5E", "Active": "#009688", "Dormant": "#b0b0b0"},
    "edusave": "#223A5E",
}


def deposit_dashboard(catalog: str) -> dict:
    """1 trang, cùng kiểu với Card: header + bộ lọc (Date, Region, Branch) bên phải, các mục
    có tiêu đề 🟧. Số dư: nghìn USD (như bản gốc 'Unit: USD 1,000')."""
    G, S = _q(catalog, "gold_serving"), _q(catalog, "silver")
    d = Dash("retail_deposit", "Retail Deposit Performance")
    d.param("report_date", "Report date", "DATE", "now/d")
    d.param("region", "Region", "STRING", "all")
    d.param("branch", "Branch", "STRING", "all")
    usd = USD_RATE.format(silver=S)
    loc = ["report_date", "region", "branch"]
    br_ok = "(:branch = 'all' OR {c} = :branch)"
    reg_ok = "(:region = 'all' OR {c} = :region)"
    base = f"""
WITH p AS (SELECT {_report_day(f'{G}.dep_mart_trend')} AS d, {usd} AS usd),
f AS (SELECT * FROM {G}.dep_mart_trend
      WHERE {reg_ok.format(c='region')} AND {br_ok.format(c='branchnbr')}),
day AS (SELECT as_of_date, sum(balance_vnd) bal FROM f GROUP BY as_of_date),
mavg AS (SELECT as_of_date, bal, avg(bal) OVER (PARTITION BY date_trunc('MM', as_of_date)
                ORDER BY as_of_date) AS mavg FROM day),
cust AS (
  SELECT c.as_of_date, sum(c.retail_customers) AS retail
  FROM {G}.dep_mart_casa_customer c LEFT JOIN {G}.dim_branch b ON b.branchnbr = c.branchnbr
  WHERE {reg_ok.format(c='b.region')} AND {br_ok.format(c='c.branchnbr')} GROUP BY 1),
pick AS (
  SELECT 0 AS ord, 'Current' AS cmp, p.d AS dd FROM p
  UNION ALL SELECT 1, 'vs Last Day', (SELECT max(as_of_date) FROM day, p
         WHERE as_of_date < p.d) FROM p
  UNION ALL SELECT 2, 'vs Last Month', (SELECT max(as_of_date) FROM day, p
         WHERE as_of_date <= last_day(add_months(p.d, -1))) FROM p
  UNION ALL SELECT 3, 'vs Last Year', (SELECT max(as_of_date) FROM day, p
         WHERE as_of_date <= make_date(year(p.d) - 1, 12, 31)) FROM p),
v AS (SELECT pk.ord, pk.cmp, m.bal / p.usd / 1000 AS bal, m.mavg / p.usd / 1000 AS mavg,
             (SELECT max_by(retail, as_of_date) FROM cust WHERE as_of_date <= pk.dd) AS retail
      FROM pick pk CROSS JOIN p LEFT JOIN mavg m ON m.as_of_date = pk.dd)
"""
    ds_tick = d.dataset("dep_tickers", "Tickers", base + """
SELECT bal AS ending_balance, mavg AS monthly_avg_balance, retail AS retail_customers
FROM v WHERE ord = 0
""", loc)
    ds_cmp = d.dataset("dep_compare", "Tickers vs last day / month / year", base + """
SELECT c.ord, c.cmp AS compare,
       {bal} AS ending_balance, {mavg} AS monthly_avg_balance, {cust} AS retail_customers
FROM v c CROSS JOIN (SELECT * FROM v WHERE ord = 0) cur WHERE c.ord > 0 ORDER BY c.ord
""".format(bal=_arrow_pct("(cur.bal - c.bal) / nullif(c.bal, 0) * 100"),
           mavg=_arrow_pct("(cur.mavg - c.mavg) / nullif(c.mavg, 0) * 100"),
           cust=_arrow_pct("(cur.retail - c.retail) / nullif(c.retail, 0) * 100")), loc)
    ds_gauge = d.dataset("dep_gauges", "Target gauges", base + f"""
, cur AS (
  SELECT sum(balance_vnd) AS all_bal,
         sum(CASE WHEN group_class = '1-CASA' THEN balance_vnd END) AS casa_bal,
         sum(CASE WHEN group_class IN ('2-TD', '3-ISA') THEN balance_vnd END) AS tda_bal
  FROM f, p WHERE f.as_of_date = p.d),
t AS (
  SELECT sum(CASE WHEN metric_code = 'DEPOSIT_BALANCE' THEN target_value END) AS all_t,
         sum(CASE WHEN metric_code = 'DEPOSIT_CASA' THEN target_value END) AS casa_t,
         sum(CASE WHEN metric_code = 'DEPOSIT_TD_ISA' THEN target_value END) AS tda_t
  FROM {S}.ref_targets tg CROSS JOIN p
  LEFT JOIN {G}.dim_branch b ON b.branchnbr = tg.branchnbr
  WHERE tg.period_type = 'YEAR' AND tg.period = CAST(year(p.d) AS STRING)
    AND {reg_ok.format(c='b.region')} AND {br_ok.format(c='tg.branchnbr')})
SELECT cur.all_bal / nullif(t.all_t, 0) * 100 AS all_pct,
       cur.casa_bal / nullif(t.casa_t, 0) * 100 AS casa_pct,
       cur.tda_bal / nullif(t.tda_t, 0) * 100 AS tda_pct
FROM cur CROSS JOIN t
""", loc)
    ds_daily = d.dataset("dep_daily30", "Ending Balance - Last 30 Days", base + """
SELECT date_format(f.as_of_date, 'MM-dd') AS day, sum(f.balance_vnd) / p.usd / 1000 AS balance
FROM f, p WHERE f.as_of_date BETWEEN date_sub(p.d, 29) AND p.d
GROUP BY 1, p.usd
""", loc)
    ds_mom = d.dataset("dep_mom", "Monthly Average Balance (MoM)", base + """
, m AS (SELECT year(as_of_date) AS yr, month(as_of_date) AS mn, group_class,
               avg(bal) AS avg_bal
        FROM (SELECT as_of_date, group_class, sum(balance_vnd) bal FROM f
              GROUP BY as_of_date, group_class)
        GROUP BY 1, 2, 3)
SELECT date_format(make_date(2000, m.mn, 1), 'MMM') AS month,
       concat(m.group_class, CASE WHEN m.yr = year(p.d) THEN ' this year'
                                    ELSE ' last year' END) AS series,
       m.avg_bal / p.usd / 1000 AS avg_bal
FROM m, p WHERE m.yr IN (year(p.d), year(p.d) - 1) AND make_date(m.yr, m.mn, 1) <= p.d
""", loc)
    ds_reg = d.dataset("dep_region", "Regional Deposit Distribution", base + """
SELECT f.region, sum(f.balance_vnd) / p.usd / 1000 AS balance FROM f, p
WHERE f.as_of_date = p.d GROUP BY 1, p.usd
""", loc)
    # kỳ hạn gom nhóm như cột TENNOR của bang5 (nhãn sắp xếp được theo chữ)
    ds_term = d.dataset("dep_term", "Deposit by Term and IR (in Month)", f"""
WITH p AS (SELECT {_report_day(f'{G}.dep_mart_by_tenor')} AS d, {usd} AS usd)
SELECT CASE WHEN t.tenor_months < 1 THEN '00-01M' WHEN t.tenor_months < 6 THEN '01-06M'
            WHEN t.tenor_months < 12 THEN '06-12M' WHEN t.tenor_months < 24 THEN '12-24M'
            ELSE '24M+' END AS tenor,
       sum(t.balance_vnd) / p.usd / 1000 AS balance,
       sum(t.weighted_rate * t.balance_vnd) / nullif(sum(t.balance_vnd), 0) * 100 AS avg_interest
FROM {G}.dep_mart_by_tenor t CROSS JOIN p
LEFT JOIN {G}.dim_branch b ON b.branchnbr = t.branchnbr
WHERE t.as_of_date = p.d AND {reg_ok.format(c='b.region')} AND {br_ok.format(c='t.branchnbr')}
GROUP BY 1, p.usd ORDER BY 1
""", loc)
    ds_casa = d.dataset("dep_casa_customer", "CASA Customer Distribution", f"""
WITH p AS (SELECT {_report_day(f'{G}.dep_mart_casa_customer')} AS d),
c AS (SELECT sum(c.retail_customers) retail, sum(c.casa_active_customers) active,
             sum(c.casa_dormant_customers) dormant
      FROM {G}.dep_mart_casa_customer c CROSS JOIN p
      LEFT JOIN {G}.dim_branch b ON b.branchnbr = c.branchnbr
      WHERE c.as_of_date = p.d AND {reg_ok.format(c='b.region')}
        AND {br_ok.format(c='c.branchnbr')})
SELECT 'Non-CASA' AS segment, retail - active - dormant AS customers FROM c
UNION ALL SELECT 'Active', active FROM c
UNION ALL SELECT 'Dormant', dormant FROM c
""", loc)
    ds_ks = d.dataset("dep_edusave", "Edu-Save New open Account_ Accumulate by Month", f"""
WITH p AS (SELECT {_report_day(f'{G}.dep_mart_trend')} AS d)
SELECT n.open_month AS month, sum(n.new_accounts_ytd) AS cumulative_accounts
FROM {G}.dep_mart_new_accounts n CROSS JOIN p
LEFT JOIN {G}.dim_branch b ON b.branchnbr = n.branchnbr
WHERE n.product_type = 'EDUSAVE' AND n.as_of_date = p.d
  AND n.open_month <= date_format(p.d, 'yyyy-MM')
  AND n.open_month > date_format(add_months(p.d, -12), 'yyyy-MM')
  AND {reg_ok.format(c='b.region')} AND {br_ok.format(c='n.branchnbr')}
GROUP BY 1
""", loc)
    # bảng phân cấp như /api/account-group-table: dòng lớp (1-CASA ...), các nhóm TK, GRAND TOTAL
    ds_prod = d.dataset("dep_by_product", "Balance by Product", base + """
, g AS (SELECT as_of_date, group_class, account_group, sum(balance_vnd) bal
      FROM f GROUP BY 1, 2, 3),
pk AS (SELECT max(CASE WHEN ord = 1 THEN dd END) AS d1, max(CASE WHEN ord = 2 THEN dd END) AS m1,
              max(CASE WHEN ord = 3 THEN dd END) AS y1 FROM pick),
r AS (
  SELECT cur.group_class, cur.account_group, cur.bal, d1.bal AS b_d1, m1.bal AS b_m1,
         y1.bal AS b_y1
  FROM g cur CROSS JOIN p CROSS JOIN pk
  LEFT JOIN g d1 ON d1.account_group = cur.account_group
       AND d1.group_class = cur.group_class AND d1.as_of_date = pk.d1
  LEFT JOIN g m1 ON m1.account_group = cur.account_group
       AND m1.group_class = cur.group_class AND m1.as_of_date = pk.m1
  LEFT JOIN g y1 ON y1.account_group = cur.account_group
       AND y1.group_class = cur.group_class AND y1.as_of_date = pk.y1
  WHERE cur.as_of_date = p.d),
lv AS (
  SELECT group_class AS k, 0 AS lvl, group_class AS product, sum(bal) bal, sum(b_d1) b_d1,
         sum(b_m1) b_m1, sum(b_y1) b_y1 FROM r GROUP BY group_class
  UNION ALL SELECT group_class, 1, concat('    ', account_group), bal, b_d1, b_m1, b_y1 FROM r
  UNION ALL SELECT 'zz', 2, 'GRAND TOTAL', sum(bal), sum(b_d1), sum(b_m1), sum(b_y1) FROM r)
SELECT lv.product, format_number(lv.bal / p.usd / 1000, 0) AS ending_balance,
       concat(format_number(lv.bal / nullif(sum(CASE WHEN lv.lvl = 2 THEN lv.bal END) OVER (),
                                            0) * 100, 1), '%') AS portion,
       format_number((lv.bal - lv.b_d1) / p.usd / 1000, 0) AS diff_last_day,
       format_number((lv.bal - lv.b_m1) / p.usd / 1000, 0) AS diff_last_month,
       format_number((lv.bal - lv.b_y1) / p.usd / 1000, 0) AS diff_last_year
FROM lv CROSS JOIN p ORDER BY lv.k, lv.lvl, lv.product
""", loc)
    ds_rg = d.dataset("dep_region_options", "Region options", f"""
SELECT 'all' AS region UNION ALL SELECT DISTINCT region FROM {G}.dim_branch
""", [])
    ds_br = d.dataset("dep_branch_options", "Branch options", f"""
SELECT 'all' AS branch UNION ALL SELECT DISTINCT branchnbr FROM {G}.dim_branch
""", [])

    c = DEP_COLORS
    d.text("# Retail Deposit Performance\nUnit: USD 1,000", 0, 0, 3, 2)
    d.param_filter("report_date", "Report date", "filter-date-picker", 3, 0)
    d.param_filter("region", "Region", "filter-single-select", 4, 0,
                   options_ds=ds_rg, options_col="region")
    d.param_filter("branch", "Branch", "filter-single-select", 5, 0,
                   options_ds=ds_br, options_col="branch")

    d.text("### Overview", 0, 2)
    for i, (col, title, key, fmt) in enumerate([
            ("ending_balance", "Ending Balance", "bal", "int"),
            ("monthly_avg_balance", "Monthly Avg Balance", "mavg", "int"),
            ("retail_customers", "Retail Customers", "cust", "int")]):
        d.counter(ds_tick, col, title, 2 * i, 3, w=2, color=c["ticker"][key], fmt=fmt)
    d.table(ds_cmp, "", [
        ("compare", "", "text"), ("ending_balance", "Ending Balance", "text"),
        ("monthly_avg_balance", "Monthly Avg Balance", "text"),
        ("retail_customers", "Retail Customers", "text")], 0, 6, 6, 3)

    d.text("### Target Achievement", 0, 9)
    for i, (col, title, key) in enumerate([("all_pct", "All Products Target (%)", "all"),
                                           ("casa_pct", "CASA Target (%)", "casa"),
                                           ("tda_pct", "TDA/ISA Target (%)", "tda")]):
        d.counter(ds_gauge, col, title, 2 * i, 10, w=2, color=c["gauge"][key], fmt="pct")

    d.text("### Balance Trend", 0, 13)
    d.chart("bar", ds_daily, "Ending Balance - Last 30 Days", 0, 14, 6, 7, cat="day",
            val="balance", single_color=c["daily"], labels=False, cat_title="Date",
            val_title="Balance (K USD)")
    d.chart("bar", ds_mom, "Monthly Average Balance (MoM)", 0, 21, 6, 8, cat="month",
            series="series", val="avg_bal", layout="group", colors=c["mom"], labels=False,
            cat_title="Month", val_title="Avg Balance (K USD)")

    d.text("### Deposit Mix", 0, 29)
    d.chart("pie", ds_reg, "Regional Deposit Distribution", 0, 30, 2, 7, cat="region",
            val="balance", colors=c["region"], cat_title="Region", val_title="Balance (K USD)")
    d.combo(ds_term, "Deposit by Term and IR (in Month)", 2, 30, 4, 7, cat="tenor",
            bar="balance", line="avg_interest", bar_title="Balance (K USD)",
            line_title="Avg Interest (%)", bar_color=c["term_bar"], line_color=c["term_line"])

    d.text("### Customer & Product Insights", 0, 37)
    d.chart("pie", ds_casa, "CASA Customer Distribution", 0, 38, 2, 7, cat="segment",
            val="customers", colors=c["casa"], cat_title="Segment", val_title="Customers")
    d.chart("bar", ds_ks, "Edu-Save New open Account_ Accumulate by Month", 2, 38, 4, 7,
            cat="month", val="cumulative_accounts", single_color=c["edusave"],
            cat_title="Month", val_title="Accounts")
    d.table(ds_prod, "Balance by Product (K USD)", [
        ("product", "Balance by Product", "text"), ("ending_balance", "Ending Balance", "num"),
        ("portion", "Portion", "text"), ("diff_last_day", "+/- Last Day", "num"),
        ("diff_last_month", "+/- Last Month", "num"),
        ("diff_last_year", "+/- Last Year", "num")], 0, 45, 6, 9)
    return d.to_json()
