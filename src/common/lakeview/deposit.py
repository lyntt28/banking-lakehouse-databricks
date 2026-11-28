"""Dashboard Retail Deposit (DEPOSIT): chỉ sửa file này khi đổi dashboard này."""

from __future__ import annotations

from common.lakeview import vega
from common.lakeview.core import (
    NEUTRAL,
    S1,
    S1_LIGHT,
    S2,
    S2_LIGHT,
    S3,
    S3_LIGHT,
    S4,
    S5,
    S6,
    Dash,
    _q,
    kpi_cards_sql,
    lit,
)

# ============================================================ 3. RETAIL DEPOSIT (DEPOSIT)
DEP_COLORS = {   # bảng màu chung (core.SERIES): CASA = S1, TD = S2, ISA = S3 ở mọi biểu đồ
    "gauge": {"all": S1, "casa": S1, "tda": S2},
    "ticker": {"bal": S3, "mavg": S1, "cust": S2},
    "daily": S1,
    "mom": {"1-CASA last year": S1_LIGHT, "1-CASA this year": S1,
            "2-TD last year": S2_LIGHT, "2-TD this year": S2,
            "3-ISA last year": S3_LIGHT, "3-ISA this year": S3},
    "region": {"North": S1, "Central": S2, "South 1": S3, "South 2": S4, "South 3": S5,
               "North 1": S1, "North 2": S6, "Others": NEUTRAL},
    "term_bar": S1, "term_line": S2,
    "casa": {"Active": S1, "Non-CASA": S2, "Dormant": NEUTRAL},
    "edusave": S1,
}


NATIONALITIES = ["VIETNAMESE", "FOREIGN", "OTHER"]
MONTHLY_TITLE = "Monthly Ending Balance - Year Comparison"   # API gốc: SUM(bal) cuối tháng


def deposit_sql(catalog: str) -> dict[str, str]:
    """SQL các dataset Deposit. Mọi visual dùng CHUNG 1 ngày báo cáo: ngày publish gần nhất
    <= :report_date trong `gold_serving.deposit_report_calendar` (batch đã kích hoạt), rồi
    chỉ lọc đúng snapshot cần đọc trong mart - không tổng hợp lịch sử để tìm ngày.

    Tiền tệ: số dư lưu VND; hiển thị nghìn USD theo tỷ giá USD gần nhất <= ngày báo cáo ĐÃ
    CHỌN (resolved), cùng 1 tỷ giá cho mọi kỳ so sánh. Thiếu dữ liệu -> NULL (không hiện 0).
    Tỷ lệ / tỷ trọng tính lại từ tử số / mẫu số đã cộng."""
    G, S = _q(catalog, "gold_serving"), _q(catalog, "silver")
    CAL = f"{G}.deposit_report_calendar"
    cur = f"""
WITH cur AS (
  SELECT c.report_date AS d, c.previous_date, c.previous_month_date, c.previous_year_date,
         (SELECT max_by(x.rate_to_vnd, x.rate_date) FROM {S}.ref_fx_rates x
          WHERE x.currencycd = 'USD' AND x.rate_date <= c.report_date) AS usd
  FROM {CAL} c
  WHERE c.report_date = (SELECT max(report_date) FROM {CAL} WHERE report_date <= :report_date))"""

    def loc(a: str = "f") -> str:
        return (f"(:region = 'all' OR {a}.region = :region) "
                f"AND (:branch = 'all' OR {a}.branchnbr = :branch)")

    prod = "(:product = 'all' OR f.group_class = :product)"
    T, M = f"{G}.dep_mart_trend", f"{G}.dep_mart_monthly"
    CASA, TEN, NEW = (f"{G}.dep_mart_casa_customer", f"{G}.dep_mart_by_tenor",
                      f"{G}.dep_mart_new_accounts")
    k = "/ cur.usd / 1000"
    return {
        "dep_tickers": cur + f"""
, t AS (SELECT sum(f.balance_vnd) AS bal,
               CASE WHEN bool_and(f.monthly_avg_complete)
                    THEN sum(f.monthly_avg_balance_vnd) END AS mavg
        FROM {T} f JOIN cur ON f.as_of_date = cur.d WHERE {loc()}),
c AS (SELECT sum(f.retail_customers) AS retail
      FROM {CASA} f JOIN cur ON f.as_of_date = cur.d WHERE {loc()})
SELECT cur.d AS report_date, t.bal {k} AS ending_balance, t.mavg {k} AS monthly_avg_balance,
       c.retail AS retail_customers
FROM cur CROSS JOIN t CROSS JOIN c""",
        # chênh lệch TUYỆT ĐỐI (nghìn USD / số KH) so với ngày so sánh thực tế
        "dep_compare": cur + f"""
, t AS (SELECT sum(f.diff_d1) AS b1, sum(f.diff_m1) AS b2, sum(f.diff_y1) AS b3,
               CASE WHEN bool_and(f.monthly_avg_complete)
                    THEN sum(f.monthly_avg_balance_vnd) END AS a0,
               sum(f.monthly_avg_balance_d1) AS a1, sum(f.monthly_avg_balance_m1) AS a2,
               sum(f.monthly_avg_balance_y1) AS a3
        FROM {T} f JOIN cur ON f.as_of_date = cur.d WHERE {loc()}),
per AS (SELECT stack(3, 1, 'vs Last Day', previous_date, 2, 'vs Last Month',
                     previous_month_date, 3, 'vs Last Year', previous_year_date)
               AS (ord, cmp, dd) FROM cur),
cc AS (SELECT f.as_of_date, sum(f.retail_customers) AS r FROM {CASA} f
       WHERE f.as_of_date IN (SELECT d FROM cur UNION ALL SELECT dd FROM per) AND {loc()}
       GROUP BY f.as_of_date)
SELECT per.ord, concat(per.cmp, ' (', coalesce(date_format(per.dd, 'yyyy-MM-dd'), 'N/A'), ')')
           AS compare,
       round(CASE per.ord WHEN 1 THEN t.b1 WHEN 2 THEN t.b2 ELSE t.b3 END {k}, 0)
           AS ending_balance,
       round((t.a0 - CASE per.ord WHEN 1 THEN t.a1 WHEN 2 THEN t.a2 ELSE t.a3 END) {k}, 0)
           AS monthly_avg_balance,
       c0.r - cp.r AS retail_customers
FROM per CROSS JOIN t CROSS JOIN cur
LEFT JOIN cc c0 ON c0.as_of_date = cur.d LEFT JOIN cc cp ON cp.as_of_date = per.dd
ORDER BY per.ord""",
        # hoàn thành = tăng trưởng từ cuối năm trước / chỉ tiêu tăng trưởng năm (chi nhánh ->
        # vùng / toàn hàng = tổng); chưa có snapshot năm trước -> NULL
        "dep_gauges": cur + f"""
, g AS (SELECT sum(f.diff_y1) AS all_g,
               sum(CASE WHEN f.group_class = '1-CASA' THEN f.diff_y1 END) AS casa_g,
               sum(CASE WHEN f.group_class IN ('2-TD', '3-ISA') THEN f.diff_y1 END) AS tda_g
        FROM {T} f JOIN cur ON f.as_of_date = cur.d WHERE {loc()}),
tg AS (
  SELECT sum(CASE WHEN f.metric_code = 'DEPOSIT_GROWTH_ALL' THEN f.target_value END) AS all_t,
         sum(CASE WHEN f.metric_code = 'DEPOSIT_GROWTH_CASA' THEN f.target_value END) AS casa_t,
         sum(CASE WHEN f.metric_code = 'DEPOSIT_GROWTH_TD_ISA' THEN f.target_value END)
             AS tda_t
  FROM (SELECT t.*, b.region FROM {S}.ref_targets t
        LEFT JOIN {G}.dim_branch b ON b.branchnbr = t.branchnbr) f CROSS JOIN cur
  WHERE f.period_type = 'YEAR' AND f.period = CAST(year(cur.d) AS STRING) AND {loc()})
SELECT g.all_g / nullif(tg.all_t, 0) * 100 AS all_pct,
       g.casa_g / nullif(tg.casa_t, 0) * 100 AS casa_pct,
       g.tda_g / nullif(tg.tda_t, 0) * 100 AS tda_pct
FROM g CROSS JOIN tg""",
        "dep_daily30": cur + f"""
SELECT date_format(f.as_of_date, 'MM-dd') AS day, f.as_of_date,
       sum(f.balance_vnd) {k} AS balance
FROM {T} f JOIN cur ON f.as_of_date BETWEEN date_sub(cur.d, 29) AND cur.d
WHERE {loc()} AND {prod}
GROUP BY f.as_of_date, cur.usd ORDER BY f.as_of_date""",
        # số dư CUỐI THÁNG (tháng hiện tại = ngày báo cáo) năm nay vs năm trước
        "dep_monthly": cur + f"""
SELECT date_format(to_date(concat(f.month_id, '-01')), 'MMM') AS month,
       concat(f.group_class, CASE WHEN substr(f.month_id, 1, 4) = CAST(year(cur.d) AS STRING)
                                  THEN ' this year' ELSE ' last year' END) AS series,
       sum(f.ending_balance_vnd) {k} AS ending_balance
FROM {M} f JOIN cur ON f.as_of_date = cur.d
WHERE {loc()}
GROUP BY f.month_id, f.group_class, cur.d, cur.usd""",
        "dep_region": cur + f"""
SELECT coalesce(f.region, 'Others') AS region, sum(f.balance_vnd) {k} AS balance
FROM {T} f JOIN cur ON f.as_of_date = cur.d
WHERE {loc()} AND {prod}
GROUP BY 1, cur.usd""",
        "dep_term": cur + f"""
SELECT f.tenor_bucket AS tenor, min(f.tenor_sort_order) AS sort_order,
       sum(f.balance_vnd) {k} AS balance,
       sum(f.interest_weighted_numerator) / nullif(sum(f.interest_weighted_denominator), 0)
           * 100 AS avg_interest
FROM {TEN} f JOIN cur ON f.as_of_date = cur.d
WHERE {loc()}
GROUP BY f.tenor_bucket, cur.usd ORDER BY sort_order""",
        "dep_casa_customer": cur + f"""
, c AS (SELECT sum(f.non_casa_customers) AS non_casa, sum(f.casa_active_customers) AS active,
               sum(f.casa_dormant_customers) AS dormant
        FROM {CASA} f JOIN cur ON f.as_of_date = cur.d WHERE {loc()})
SELECT 'Non-CASA' AS segment, non_casa AS customers FROM c
UNION ALL SELECT 'Active', active FROM c
UNION ALL SELECT 'Dormant', dormant FROM c""",
        # K-Study: tổng năm trước + luỹ kế từng tháng năm nay (cộng dồn sau khi lọc vùng / CN)
        "dep_edusave": cur + f"""
, n AS (SELECT f.open_month, sum(f.new_accounts) AS n FROM {NEW} f JOIN cur
        ON f.as_of_date = cur.d
        WHERE f.product_type = 'EDUSAVE' AND {loc()} GROUP BY f.open_month)
SELECT concat(CAST(year(cur.d) - 1 AS STRING), ' Total') AS month,
       sum(n.n) AS cumulative_accounts
FROM n CROSS JOIN cur WHERE substr(n.open_month, 1, 4) = CAST(year(cur.d) - 1 AS STRING)
GROUP BY cur.d
UNION ALL
SELECT n.open_month, sum(n.n) OVER (ORDER BY n.open_month) FROM n CROSS JOIN cur
WHERE substr(n.open_month, 1, 4) = CAST(year(cur.d) AS STRING)""",
        # dòng chi tiết / tổng lớp / GRAND TOTAL bằng GROUPING SETS (1 lần quét)
        "dep_by_product": cur + f"""
, r AS (SELECT f.group_class, f.account_group, sum(f.balance_vnd) AS bal,
               sum(f.diff_d1) AS d1, sum(f.diff_m1) AS m1, sum(f.diff_y1) AS y1,
               grouping(f.group_class) AS g_class, grouping(f.account_group) AS g_group
        FROM {T} f JOIN cur ON f.as_of_date = cur.d
        WHERE {loc()} AND (:currency = 'all' OR f.currencycd = :currency)
          AND (:nationality = 'all' OR f.nationality = :nationality)
        GROUP BY GROUPING SETS ((f.group_class, f.account_group), (f.group_class), ()))
SELECT CASE WHEN r.g_class = 1 THEN 'GRAND TOTAL' WHEN r.g_group = 1 THEN r.group_class
            ELSE concat('    ', r.account_group) END AS product,
       CASE WHEN r.g_class = 1 THEN 0 WHEN r.g_group = 1 THEN 1 ELSE 2 END AS level,
       round(r.bal {k}, 0) AS ending_balance,
       round(r.bal / nullif(max(CASE WHEN r.g_class = 1 THEN r.bal END) OVER (), 0) * 100, 1)
           AS portion,
       round(r.d1 {k}, 0) AS diff_last_day, round(r.m1 {k}, 0) AS diff_last_month,
       round(r.y1 {k}, 0) AS diff_last_year
FROM r CROSS JOIN cur
ORDER BY r.g_class, r.group_class, r.g_group DESC, r.account_group""",
        "dep_region_options": f"""
SELECT 'all' AS region UNION ALL SELECT DISTINCT region FROM {G}.dim_branch""",
        "dep_branch_options": f"""
SELECT 'all' AS branch UNION ALL SELECT DISTINCT branchnbr FROM {G}.dim_branch""",
        "dep_product_options": f"""
SELECT 'all' AS product UNION ALL
SELECT DISTINCT product_group FROM {G}.dim_product WHERE product_family = 'DEPOSIT'""",
        "dep_currency_options": """
SELECT * FROM VALUES ('all'), ('VND'), ('USD') AS t(currency)""",
        "dep_nationality_options": "\nSELECT * FROM VALUES ('all'), "
        + ", ".join(f"('{n}')" for n in NATIONALITIES) + " AS t(nationality)",
    }


def _with_kpi_cards(sql: dict[str, str]) -> dict[str, str]:
    """Thẻ KPI (Custom Viz): giá trị từ dep_tickers + chênh lệch tuyệt đối D/M/Y từ dep_compare."""
    cols = ["ending_balance", "monthly_avg_balance", "retail_customers"]
    piv = ", ".join(f"max(CASE WHEN ord = {o} THEN {c} END) AS {c}_{o}"
                    for c in cols for o in (1, 2, 3))
    fmt = {"ending_balance": ",.0f", "monthly_avg_balance": ",.0f", "retail_customers": ",.0f"}
    titles = {"ending_balance": "Ending Balance", "monthly_avg_balance": "Monthly Avg Balance",
              "retail_customers": "Retail Customers"}
    sql["dep_kpi_cards"] = kpi_cards_sql(
        {"k": sql["dep_tickers"], "c": f"SELECT {piv} FROM ({sql['dep_compare']})"},
        [{"kpi": lit(c), "title": lit(titles[c]), "value": f"k.{c}", "value_fmt": lit(fmt[c]),
          "cmp1": f"c.{c}_1", "cmp1_label": lit("Last Day"), "cmp2": f"c.{c}_2",
          "cmp2_label": lit("Last Month"), "cmp3": f"c.{c}_3", "cmp3_label": lit("Last Year"),
          "cmp_unit": lit(""), "cmp_fmt": lit(",.0f"),
          "note1": lit("Unit: USD 1,000" if c != "retail_customers" else "Customers"),
          "note2": "concat('Report date ', date_format(k.report_date, 'yyyy-MM-dd'))"}
         for c in cols])
    return sql


def deposit_dashboard(catalog: str) -> dict:
    """1 trang: header + bộ lọc, các mục có tiêu đề. Số dư: nghìn USD ('Unit: USD 1,000').
    Bộ lọc áp dụng theo grain từng visual (như dashboard gốc), không ép mọi filter cho mọi
    visual: Product -> Daily / Regional; Currency + Nationality -> Balance by Product."""
    d = Dash("retail_deposit", "Retail Deposit Performance")
    d.param("report_date", "Report date", "DATE", "now/d")
    for p in ("region", "branch", "product", "currency", "nationality"):
        d.param(p, p.capitalize(), "STRING", "all")
    sql = _with_kpi_cards(deposit_sql(catalog))
    loc = ["report_date", "region", "branch"]
    ps = {"dep_tickers": loc, "dep_compare": loc, "dep_gauges": loc,
          "dep_daily30": loc + ["product"], "dep_monthly": loc,
          "dep_region": loc + ["product"], "dep_term": loc, "dep_casa_customer": loc,
          "dep_edusave": loc, "dep_by_product": loc + ["currency", "nationality"],
          "dep_kpi_cards": loc}
    titles = {"dep_tickers": "Tickers", "dep_compare": "Tickers vs last day / month / year",
              "dep_gauges": "Target gauges (YTD growth / growth target)",
              "dep_daily30": "Ending Balance - Last 30 Days", "dep_monthly": MONTHLY_TITLE,
              "dep_region": "Regional Deposit Distribution",
              "dep_term": "Deposit by Term and IR (in Month)",
              "dep_casa_customer": "CASA Customer Distribution",
              "dep_edusave": "Edu-Save New open Account_ Accumulate by Month",
              "dep_by_product": "Balance by Product",
              "dep_kpi_cards": "KPI cards (Custom Viz)"}
    ds = {n: d.dataset(n, t, sql[n], ps[n]) for n, t in titles.items()}
    opt = {n: d.dataset(f"dep_{n}_options", f"{n.capitalize()} options",
                        sql[f"dep_{n}_options"], [])
           for n in ("region", "branch", "product", "currency", "nationality")}

    c = DEP_COLORS

    def header_and_filters(page_filters: tuple[str, ...]) -> None:
        """Header + bộ lọc. Mỗi bộ lọc tự gắn vào đúng các dataset có tham số đó (Product chỉ
        Daily / Regional; Currency + Nationality chỉ Balance by Product)."""
        d.text("# Retail Deposit Performance\nUnit: USD 1,000", 0, 0, 3, 2)
        d.param_filter("report_date", "Report date", "filter-date-picker", 3, 0)
        for i, n in enumerate(page_filters):
            x, y = (4 + i, 0) if i < 2 else (3 + i - 2, 2)
            d.param_filter(n, n.capitalize(), "filter-single-select", x, y,
                           options_ds=opt[n], options_col=n)

    # 1 trang: Overview & Performance rồi Customer & Product Insights (2 tab của bản gốc gộp lại)
    Y = 2   # chừa 1 hàng cho bộ lọc
    d.text("### Overview", 0, 2, 3)
    # Thẻ KPI (Custom Viz): giá trị + chênh lệch Last Day / Month / Year (thay ô + bảng)
    for i, (kpi, key) in enumerate([("ending_balance", "bal"), ("monthly_avg_balance", "mavg"),
                                    ("retail_customers", "cust")]):
        d.custom_viz(ds["dep_kpi_cards"], "", vega.kpi_card(kpi, c["ticker"][key]),
                     2 * i, 3 + Y, 2, 4)
    d.text("### Target Achievement", 0, 7 + Y)
    # Gauge bán nguyệt (Custom Viz) như bản gốc
    for i, (col, title, key) in enumerate([("all_pct", "All Products Target (%)", "all"),
                                           ("casa_pct", "CASA Target (%)", "casa"),
                                           ("tda_pct", "TDA/ISA Target (%)", "tda")]):
        d.custom_viz(ds["dep_gauges"], title, vega.gauge(c["gauge"][key], col),
                     2 * i, 8 + Y, 2, 3)
    d.text("### Balance Trend", 0, 11 + Y)
    d.chart("bar", ds["dep_daily30"], "Ending Balance - Last 30 Days", 0, 12 + Y, 6, 7,
            cat="day", val="balance", single_color=c["daily"], labels=False,
            cat_title="Date", val_title="Balance (K USD)")
    d.custom_viz(ds["dep_monthly"], MONTHLY_TITLE, vega.grouped_stacked_years(
        "month", "_year", "_class", "ending_balance",
        {"1-CASA": c["mom"]["1-CASA this year"], "2-TD": c["mom"]["2-TD this year"],
         "3-ISA": c["mom"]["3-ISA this year"]}, series="series"), 0, 19 + Y, 6, 8)
    d.text("### Deposit Mix", 0, 27 + Y)
    d.chart("pie", ds["dep_region"], "Regional Deposit Distribution", 0, 28 + Y, 2, 7,
            cat="region", val="balance", colors=c["region"], cat_title="Region",
            val_title="Balance (K USD)")
    d.custom_viz(ds["dep_term"], "Deposit by Term and IR (in Month)", vega.dual_axis_combo(
        "tenor", "balance", "avg_interest", "Balance (K USD)", "Avg Interest (%)",
        c["term_bar"], c["term_line"], x_sort=None, line_fmt=".2f"), 2, 28 + Y, 4, 7)
    d.text("### Customer & Product Insights", 0, 35 + Y)
    d.chart("pie", ds["dep_casa_customer"], "CASA Customer Distribution", 0, 36 + Y, 2, 7,
            cat="segment", val="customers", colors=c["casa"], cat_title="Segment",
            val_title="Customers")
    d.chart("bar", ds["dep_edusave"], "Edu-Save New open Account_ Accumulate by Month",
            2, 36 + Y, 4, 7, cat="month", val="cumulative_accounts",
            single_color=c["edusave"], cat_title="Month", val_title="Accounts")
    d.table(ds["dep_by_product"], "Balance by Product (K USD)", [
        ("product", "Balance by Product", "text"), ("ending_balance", "Ending Balance", "num"),
        ("portion", "Portion (%)", "num"), ("diff_last_day", "+/- Last Day", "num"),
        ("diff_last_month", "+/- Last Month", "num"),
        ("diff_last_year", "+/- Last Year", "num")], 0, 43 + Y, 6, 9)
    header_and_filters(("region", "branch", "product", "currency", "nationality"))
    return d.to_json()
