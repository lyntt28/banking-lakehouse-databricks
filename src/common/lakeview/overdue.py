"""Dashboard Loan Overdue: chỉ sửa file này khi đổi dashboard này."""

from __future__ import annotations

from common.lakeview.core import (
    S1,
    S2,
    SERIES,
    SEVERITY,
    TEXT,
    USD_RATE,
    Dash,
    _q,
    kpi_cards_sql,
    lit,
)

# ============================================================ 2. OVERDUE (RAMC)
# Nguồn: gold.card_mart_overdue_daily (file credit_card_overdue_YYYYMMDD.txt), logic theo
# dashboard Chart.js gốc (đã xóa khỏi repo): tỷ lệ = sum(ovd_bal) / sum(port_bal),
# lọc chi nhánh theo mng_br, không lọc loại sản phẩm (All).
OVD_COLORS = {   # bảng màu chung (core.SERIES)
    "amount": S1, "ratio": S2, "fx": TEXT,
    "ticker": {"ratio": TEXT, "overdue": TEXT, "ending": TEXT},
    "products": SERIES,
}


DEBT_GROUPS = ["1-10", "11-90", "91-180", "181-360", "361+"]   # theo file nguồn (DEL_GRP)
DEBT_COLORS = dict(zip(DEBT_GROUPS, SEVERITY, strict=True))   # nhẹ -> nặng
EXCLUDED_BRANCH = "8125"          # API gốc: bỏ CN 8125 khỏi treemap chi nhánh
BRANCH_MIN_OVERDUE_USD = 10_000   # API gốc: chỉ CN có dư nợ quá hạn > 10.000 USD


def overdue_sql(catalog: str) -> dict[str, str]:
    """SQL các dataset Overdue. Nguyên tắc: chọn ngày từ lịch báo cáo (nhỏ) TRƯỚC, rồi mới lọc
    đúng các snapshot cần đọc trong mart, rồi mới tổng hợp. Không quét lịch sử mart để tìm ngày.

    Lịch `gold_serving.overdue_report_calendar` chỉ chứa ngày có batch Overdue đã kích hoạt.
    Ngày báo cáo = ngày publish gần nhất <= :report_date (không bao giờ đọc tương lai).
    Tỷ lệ = sum(ovd_bal) / sum(port_bal) * 100 trên tử / mẫu đã tổng hợp (không cộng tỷ lệ);
    mẫu = 0 -> NULL. Kỳ so sánh không có dữ liệu -> NULL / 'N/A', không coi là 0.
    USD = VND / tỷ giá USD gần nhất <= :report_date (cùng 1 tỷ giá cho mọi kỳ, như bản gốc)."""
    G = _q(catalog, "gold_serving")   # view batch đang dùng (common.serving)
    T, CAL = f"{G}.card_mart_overdue_daily", f"{G}.overdue_report_calendar"
    usd = USD_RATE.format(silver=_q(catalog, "silver"))
    # lịch: dòng của ngày báo cáo hiện tại (1 dòng) + tỷ giá
    cal = f"""
WITH cal AS (SELECT report_date, previous_date, previous_month_date, previous_year_date
             FROM {CAL} WHERE report_date <= :report_date),
cur AS (SELECT c.report_date, c.previous_date, c.previous_month_date, c.previous_year_date,
               {usd} AS usd
        FROM cal c WHERE c.report_date = (SELECT max(report_date) FROM cal))"""
    br = "(:branch = 'all' OR f.mng_br = :branch)"

    def agg(dates: str, extra_cols: str = "", extra_group: str = "") -> str:
        """Tổng hợp mart CHỈ trên tập ngày `dates` (CTE có cột d), có lọc chi nhánh."""
        return f"""
SELECT f.as_of_date{extra_cols}, sum(f.ovd_bal) AS ovd, sum(f.port_bal) AS port
FROM {T} f JOIN {dates} x ON f.as_of_date = x.d
WHERE {br}
GROUP BY f.as_of_date{extra_group}"""

    periods = cal + f"""
, per AS (
  SELECT stack(4, 0, 'CURRENT', 'Current', report_date,
                  1, 'D1', 'vs Last Day', previous_date,
                  2, 'M1', 'vs Last Month', previous_month_date,
                  3, 'Y1', 'vs Last Year', previous_year_date) AS (ord, period, cmp, d)
  FROM cur),
v AS ({agg("(SELECT DISTINCT d FROM per WHERE d IS NOT NULL)")}),
k AS (
  SELECT per.ord, per.period, per.cmp, per.d AS actual_date, v.port AS port_bal, v.ovd AS ovd_bal,
         v.ovd / nullif(v.port, 0) * 100 AS overdue_ratio,
         v.ovd / cur.usd AS overdue_usd, v.port / cur.usd AS ending_balance_usd, cur.usd
  FROM per CROSS JOIN cur LEFT JOIN v ON v.as_of_date = per.d)"""

    def arrow(e: str, places: int, unit: str = "") -> str:
        return (f"CASE WHEN {e} IS NULL THEN 'N/A' ELSE concat(CASE WHEN {e} > 0 THEN '▲ ' "
                f"WHEN {e} < 0 THEN '▼ ' ELSE '▪ ' END, format_number(abs({e}), {places}), "
                f"'{unit}') END")

    month_snaps = lambda cond: f"""
, m AS (SELECT max(c.report_date) AS d FROM cal c, cur
        WHERE {cond} GROUP BY date_trunc('MONTH', c.report_date))"""  # noqa: E731
    return {
        # 3 KPI + tỷ giá + ngày thực tế đang hiển thị (1 dòng: kỳ CURRENT)
        "ovd_kpi": periods + """
SELECT actual_date, usd AS fx_vnd_usd, overdue_ratio, overdue_usd,
       ending_balance_usd AS ending_usd
FROM k WHERE period = 'CURRENT'""",
        # D-1 / M-1 / Y-1: chênh lệch so với CURRENT + ngày thực tế của kỳ so sánh
        "ovd_compare": periods + f"""
, c AS (SELECT * FROM k WHERE period = 'CURRENT')
SELECT k.ord, k.period, k.actual_date,
       concat(k.cmp, ' (', coalesce(date_format(k.actual_date, 'yyyy-MM-dd'), 'N/A'), ')') AS cmp,
       round(c.overdue_ratio - k.overdue_ratio, 2) AS ratio_pp,
       round(c.overdue_usd - k.overdue_usd, 0) AS overdue_usd,
       round(c.ending_balance_usd - k.ending_balance_usd, 0) AS ending_usd,
       {arrow("round(c.overdue_ratio - k.overdue_ratio, 2)", 2, " pp")} AS ratio_txt,
       {arrow("round(c.overdue_usd - k.overdue_usd, 0)", 0)} AS overdue_txt,
       {arrow("round(c.ending_balance_usd - k.ending_balance_usd, 0)", 0)} AS ending_txt
FROM k CROSS JOIN c WHERE k.period <> 'CURRENT'
ORDER BY k.ord""",
        # 5 ngày đã publish gần nhất <= ngày báo cáo (chọn ngày từ lịch, KHÔNG theo chi nhánh)
        "ovd_daily_trend": cal + f"""
, dd AS (SELECT c.report_date AS d FROM cal c ORDER BY c.report_date DESC LIMIT 5),
v AS ({agg("dd")})
SELECT date_format(dd.d, 'MM-dd') AS day, dd.d AS actual_date,
       v.ovd / cur.usd / 1000 AS overdue_k_usd,
       v.ovd / nullif(v.port, 0) * 100 AS overdue_ratio
FROM dd CROSS JOIN cur LEFT JOIN v ON v.as_of_date = dd.d
ORDER BY dd.d""",
        # 6 tháng tính cả tháng đang chọn: mỗi tháng 1 snapshot (ngày cuối có data, tháng hiện
        # tại = ngày báo cáo). Dư nợ là số dư (stock) -> KHÔNG cộng các ngày trong tháng.
        "ovd_monthly_trend": cal + month_snaps(
            "c.report_date > last_day(add_months(cur.report_date, -6))") + f"""
, v AS ({agg("m")})
SELECT date_format(m.d, 'yyyy-MM') AS month, m.d AS actual_date,
       v.ovd / cur.usd / 1000 AS overdue_k_usd,
       v.ovd / nullif(v.port, 0) * 100 AS overdue_ratio
FROM m CROSS JOIN cur LEFT JOIN v ON v.as_of_date = m.d
ORDER BY m.d""",
        # Stacked bar: tháng 1 năm báo cáo -> tháng báo cáo; MỘT ngày chốt chung cho mọi nhóm
        # nợ trong tháng; bỏ nhóm '0' (không quá hạn).
        "ovd_debt_groups": cal + month_snaps(
            "year(c.report_date) = year(cur.report_date)") + f"""
, v AS ({agg("m", ", f.del_grp", ", f.del_grp")})
SELECT date_format(v.as_of_date, 'yyyy-MM') AS month, v.as_of_date AS actual_date,
       v.del_grp, v.ovd / cur.usd / 1000 AS overdue_k_usd
FROM v CROSS JOIN cur
WHERE v.del_grp <> '0'
ORDER BY month, v.del_grp""",
        # Theo nhóm SP, snapshot hiện tại: amount (pie) + ratio
        "ovd_product": cal + f"""
, v AS ({agg("(SELECT report_date AS d FROM cur)", ", coalesce(f.product_group, 'Other') AS pg",
             ", coalesce(f.product_group, 'Other')")})
SELECT pg AS product_group, ovd / cur.usd / 1000 AS overdue_k_usd,
       ovd / nullif(port, 0) * 100 AS overdue_ratio
FROM v CROSS JOIN cur""",
        "ovd_product_top5": cal + f"""
, v AS ({agg("(SELECT report_date AS d FROM cur)", ", coalesce(f.product_group, 'Other') AS pg",
             ", coalesce(f.product_group, 'Other')")})
SELECT pg AS product_group, ovd / nullif(port, 0) * 100 AS overdue_ratio
FROM v WHERE port > 0
ORDER BY overdue_ratio DESC, product_group LIMIT 5""",
        # Treemap chi nhánh của API gốc: bỏ CN 8125, overdue > 10.000 USD, Top 5 theo tỷ lệ
        "ovd_branch_top5": cal + f"""
, v AS ({agg("(SELECT report_date AS d FROM cur)", ", f.mng_br", ", f.mng_br")})
SELECT v.mng_br AS branch, v.ovd / cur.usd AS overdue_usd,
       v.ovd / cur.usd / 1000 AS overdue_k_usd,
       v.ovd / nullif(v.port, 0) * 100 AS overdue_ratio
FROM v CROSS JOIN cur
WHERE v.mng_br <> '{EXCLUDED_BRANCH}' AND v.ovd / cur.usd > {BRANCH_MIN_OVERDUE_USD}
ORDER BY overdue_ratio DESC, branch LIMIT 5""",
        # danh sách CN: chỉ đọc snapshot publish mới nhất (không quét toàn lịch sử)
        "ovd_branch_options": f"""
SELECT 'all' AS branch UNION ALL
SELECT DISTINCT mng_br FROM {T}
WHERE as_of_date = (SELECT max(report_date) FROM {CAL}) AND mng_br IS NOT NULL""",
    }


def _with_kpi_cards(sql: dict[str, str]) -> dict[str, str]:
    """Thẻ KPI (Custom Viz): giá trị từ ovd_kpi + chênh lệch D-1 / M-1 / Y-1 từ ovd_compare."""
    cols = [("overdue_ratio", "ratio_pp", "Overdue Ratio", ".2f", " pp", ".2f"),
            ("overdue_usd", "overdue_usd", "Overdue Balance (USD)", ",.0f", "", ",.0f"),
            ("ending_usd", "ending_usd", "Ending Balance (USD)", ",.0f", "", ",.0f")]
    piv = ", ".join(f"max(CASE WHEN period = '{p}' THEN {d} END) AS {d}_{p}"
                    for _, d, *_ in cols for p in ("D1", "M1", "Y1"))
    sql["ovd_kpi_cards"] = kpi_cards_sql(
        {"k": sql["ovd_kpi"], "c": f"SELECT {piv} FROM ({sql['ovd_compare']})"},
        [{"kpi": lit(v), "title": lit(t), "value": f"k.{v}", "value_fmt": lit(vf),
          "cmp1": f"c.{d}_D1", "cmp1_label": lit("vs Last Day"), "cmp2": f"c.{d}_M1",
          "cmp2_label": lit("vs Last Month"), "cmp3": f"c.{d}_Y1",
          "cmp3_label": lit("vs Last Year"), "cmp_unit": lit(u), "cmp_fmt": lit(cf),
          "note1": "concat('Report date ', date_format(k.actual_date, 'yyyy-MM-dd'))",
          "note2": "concat('FX ', format_number(k.fx_vnd_usd, 0), ' VND/USD')"}
         for v, d, t, vf, u, cf in cols])
    return sql


def overdue_dashboard(catalog: str) -> dict:
    d = Dash("overdue", "Retail Loan Overdue Tracking")
    d.param("report_date", "Report date", "DATE", "now/d")
    d.param("branch", "Branch", "STRING", "all")
    sql = _with_kpi_cards(overdue_sql(catalog))
    rb = ["report_date", "branch"]
    titles = {"ovd_kpi": "KPI (current) + FX", "ovd_compare": "KPI vs Last Day / Month / Year",
              "ovd_daily_trend": "Overdue Trend (Last 5 Days)",
              "ovd_monthly_trend": "Overdue Trend (Monthly)",
              "ovd_debt_groups": "Overdue Amount by Debt Groups",
              "ovd_product": "Overdue by Product Groups",
              "ovd_product_top5": "Overdue Ratio by Product Groups - Top5",
              "ovd_branch_top5": "Overdue by Branch - Top5",
              "ovd_kpi_cards": "KPI cards (Custom Viz)"}
    ds = {n: d.dataset(n, t, sql[n], rb) for n, t in titles.items()}
    ds_br = d.dataset("ovd_branch_options", "Branch options", sql["ovd_branch_options"], [])

    c = OVD_COLORS
    d.text("# RETAIL LOAN OVERDUE TRACKING", 0, 0, 3, 2)
    d.counter(ds["ovd_kpi"], "fx_vnd_usd", "FX (VND/USD)", 3, 0, w=1, h=2, color=c["fx"],
              fmt="num")
    # main-stats repeat(3, 1fr)
    for i, (col, title, key, fmt) in enumerate([
            ("overdue_ratio", "Overdue Ratio", "ratio", "pct"),
            ("overdue_usd", "Overdue Balance (USD)", "overdue", "int"),
            ("ending_usd", "Ending Balance (USD)", "ending", "int")]):
        d.counter(ds["ovd_kpi"], col, title, 2 * i, 2, w=2, color=c["ticker"][key], fmt=fmt)
    # vs Last Day / Month / Year (kèm ngày thực tế của kỳ so sánh)
    for i, (col, title) in enumerate([("ratio_txt", "Overdue Ratio"),
                                      ("overdue_txt", "Overdue Balance (USD)"),
                                      ("ending_txt", "Ending Balance (USD)")]):
        d.table(ds["ovd_compare"], f"{title} vs Last Day / Month / Year",
                [("cmp", "Compare", "text"), (col, "Change", "text")], 2 * i, 5, 2, 3)
    # top-charts-grid 1fr 1.5fr -> 2 + 4
    combo = dict(bar="overdue_k_usd", line="overdue_ratio",
                 bar_title="Overdue Amount (1000 USD)", line_title="Overdue Ratio (%)",
                 bar_color=c["amount"], line_color=c["ratio"])
    d.combo(ds["ovd_daily_trend"], "Overdue Trend (Last 5 Days)", 0, 8, 2, 7, cat="day",
            **combo)
    d.combo(ds["ovd_monthly_trend"], "Overdue Trend (Monthly)", 2, 8, 4, 7, cat="month",
            **combo)
    # stacked bar tháng x nhóm nợ (như Chart.js gốc)
    d.chart("bar", ds["ovd_debt_groups"], "Overdue Amount by Debt Groups", 0, 15, 6, 7,
            cat="month", series="del_grp", val="overdue_k_usd", colors=DEBT_COLORS,
            layout="stack", val_title="Overdue Amount (1000 USD)")
    # product-groups-grid 1fr 1fr
    d.chart("pie", ds["ovd_product"], "Overdue Amount by Product Groups", 0, 22, 3, 7,
            cat="product_group", val="overdue_k_usd", val_title="1000 USD")
    d.chart("bar", ds["ovd_product_top5"], "Overdue Ratio by Product Groups", 3, 22, 3, 7,
            cat="product_group", val="overdue_ratio", horizontal=True,
            single_color=c["amount"], sort_desc=True, val_title="Overdue Ratio (%)")
    # Treemap gốc -> bar ngang Top 5 (Lakeview: giữ nguyên quy tắc nghiệp vụ, xem overdue_sql)
    d.chart("bar", ds["ovd_branch_top5"], "Top 5 Branches by Overdue Ratio", 0, 29, 3, 7,
            cat="branch", val="overdue_ratio", horizontal=True, single_color=c["ratio"],
            sort_desc=True, val_title="Overdue Ratio (%)")
    d.chart("bar", ds["ovd_branch_top5"], "Top 5 Branches - Overdue Amount", 3, 29, 3, 7,
            cat="branch", val="overdue_k_usd", horizontal=True, single_color=c["amount"],
            sort_desc=True, val_title="Overdue Amount (1000 USD)")
    d.param_filter("report_date", "Report date", "filter-date-picker", 4, 0)
    d.param_filter("branch", "Branch", "filter-single-select", 5, 0,
                   options_ds=ds_br, options_col="branch")
    return d.to_json()
