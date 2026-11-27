"""Dashboard Loan Overdue: chỉ sửa file này khi đổi dashboard này."""

from __future__ import annotations

from common.lakeview.core import USD_RATE, Dash, _q, _report_day

# ============================================================ 2. OVERDUE (RAMC)
# Nguồn: gold.card_mart_overdue_daily (file credit_card_overdue_YYYYMMDD.txt), logic theo
# dashboard Chart.js gốc (đã xóa khỏi repo): tỷ lệ = sum(ovd_bal) / sum(port_bal),
# lọc chi nhánh theo mng_br, không lọc loại sản phẩm (All).
OVD_COLORS = {   # từ dashboard Chart.js gốc
    "amount": "#3498DB", "ratio": "#E67E22", "fx": "#0077B6",
    "ticker": {"ratio": "#48bb78", "overdue": "#4299e1", "ending": "#ed8936"},
    "products": ["#3498DB", "#E74C3C", "#2ECC71", "#F39C12", "#9B59B6", "#1ABC9C", "#E67E22",
                 "#34495E", "#E91E63", "#00BCD4"],
}


def overdue_dashboard(catalog: str) -> dict:
    G = _q(catalog, "gold")
    T = f"{G}.card_mart_overdue_daily"
    d = Dash("overdue", "Retail Loan Overdue Tracking")
    d.param("report_date", "Report date", "DATE", "now/d")
    d.param("branch", "Branch", "STRING", "all")
    usd = USD_RATE.format(silver=_q(catalog, "silver"))
    base = f"""
WITH p AS (SELECT {_report_day(T)} AS d, {usd} AS usd),
f AS (SELECT * FROM {T} WHERE :branch = 'all' OR mng_br = :branch),
day AS (SELECT as_of_date, sum(ovd_bal) ovd, sum(port_bal) port FROM f GROUP BY as_of_date)
"""
    ds_fx = d.dataset("ovd_fx", "FX rate", f"SELECT {usd} AS fx_vnd_usd", ["report_date"])
    ds_tick = d.dataset("ovd_tickers", "Tickers", base + """
SELECT x.ovd / nullif(x.port, 0) * 100 AS overdue_ratio,
       x.ovd / p.usd AS overdue_usd, x.port / p.usd AS ending_usd
FROM day x, p WHERE x.as_of_date = p.d
""", ["report_date", "branch"])
    # get_comparison_dates: D-1, cuối tháng trước, 31/12 năm trước -> ngày có data gần nhất <=
    ds_cmp = d.dataset("ovd_compare", "Tickers vs last day / month / year", base + """
, pick AS (
  SELECT 1 AS ord, 'vs Last Day' AS cmp,
         (SELECT max(as_of_date) FROM day, p WHERE as_of_date <= date_sub(p.d, 1)) AS dd FROM p
  UNION ALL SELECT 2, 'vs Last Month', (SELECT max(as_of_date) FROM day, p
         WHERE as_of_date <= last_day(add_months(p.d, -1))) FROM p
  UNION ALL SELECT 3, 'vs Last Year', (SELECT max(as_of_date) FROM day, p
         WHERE as_of_date <= make_date(year(p.d) - 1, 12, 31)) FROM p),
cur AS (SELECT x.* FROM day x, p WHERE x.as_of_date = p.d)
SELECT pk.ord, pk.cmp, v.ratio_pp, v.overdue_usd, v.ending_usd,
  -- như dashboard gốc: ▲ / ▼ / ▪ + trị tuyệt đối (USD làm tròn, % 2 số lẻ)
  concat(CASE WHEN v.ratio_pp > 0 THEN '▲ ' WHEN v.ratio_pp < 0 THEN '▼ ' ELSE '▪ ' END,
         format_number(abs(v.ratio_pp), 2), '%') AS ratio_txt,
  concat(CASE WHEN v.overdue_usd > 0 THEN '▲ ' WHEN v.overdue_usd < 0 THEN '▼ ' ELSE '▪ ' END,
         format_number(abs(v.overdue_usd), 0)) AS overdue_txt,
  concat(CASE WHEN v.ending_usd > 0 THEN '▲ ' WHEN v.ending_usd < 0 THEN '▼ ' ELSE '▪ ' END,
         format_number(abs(v.ending_usd), 0)) AS ending_txt
FROM pick pk CROSS JOIN cur CROSS JOIN p LEFT JOIN day c ON c.as_of_date = pk.dd
LATERAL VIEW inline(array(struct(
  round((cur.ovd / nullif(cur.port, 0) - c.ovd / nullif(c.port, 0)) * 100, 2) AS ratio_pp,
  round((cur.ovd - c.ovd) / p.usd, 0) AS overdue_usd,
  round((cur.port - c.port) / p.usd, 0) AS ending_usd))) v
ORDER BY pk.ord
""", ["report_date", "branch"])
    # 5 ngày có data gần nhất (chọn ngày KHÔNG theo chi nhánh, giá trị theo chi nhánh)
    ds_5d = d.dataset("ovd_last5", "Overdue Trend (Last 5 Days)", base + f"""
, dd AS (SELECT DISTINCT as_of_date FROM {T}, p WHERE as_of_date <= p.d
         ORDER BY as_of_date DESC LIMIT 5)
SELECT date_format(dd.as_of_date, 'MM-dd') AS day,
       coalesce(x.ovd, 0) / p.usd / 1000 AS overdue_k_usd,
       coalesce(x.ovd / nullif(x.port, 0) * 100, 0) AS overdue_ratio
FROM dd CROSS JOIN p LEFT JOIN day x ON x.as_of_date = dd.as_of_date
""", ["report_date", "branch"])
    # 6 tháng: tháng trước lấy ngày cuối có data, tháng hiện tại lấy ngày gần nhất <= report_date
    months = f"""
, m AS (SELECT date_format(as_of_date, 'yyyy-MM') AS month, max(as_of_date) AS last_d
        FROM {T}, p WHERE as_of_date <= p.d
          AND as_of_date > last_day(add_months(p.d, -6)) GROUP BY 1)
"""
    ds_mon = d.dataset("ovd_monthly", "Overdue Trend (Monthly)", base + months + """
SELECT m.month, coalesce(x.ovd, 0) / p.usd / 1000 AS overdue_k_usd,
       coalesce(x.ovd / nullif(x.port, 0) * 100, 0) AS overdue_ratio
FROM m CROSS JOIN p LEFT JOIN day x ON x.as_of_date = m.last_d
""", ["report_date", "branch"])
    # như Last 5 Days nhưng theo tháng, từ tháng 1 năm của report_date (ngày cuối mỗi tháng)
    ds_dg = d.dataset("ovd_debt_groups", "Overdue Amount by Debt Groups", base + f"""
, m AS (SELECT date_format(as_of_date, 'yyyy-MM') AS month, max(as_of_date) AS last_d
        FROM {T}, p WHERE as_of_date <= p.d AND year(as_of_date) = year(p.d) GROUP BY 1)
SELECT m.month, coalesce(x.ovd, 0) / p.usd / 1000 AS overdue_k_usd,
       coalesce(x.ovd / nullif(x.port, 0) * 100, 0) AS overdue_ratio
FROM m CROSS JOIN p LEFT JOIN day x ON x.as_of_date = m.last_d
""", ["report_date", "branch"])
    ds_pa = d.dataset("ovd_product_amount", "Overdue Amount by Product Groups", base + """
SELECT coalesce(f.product_group, 'Other') AS product_group,
       sum(f.ovd_bal) / p.usd / 1000 AS overdue_k_usd
FROM f, p WHERE f.as_of_date = p.d GROUP BY 1, p.usd
""", ["report_date", "branch"])
    ds_pr = d.dataset("ovd_product_ratio", "Overdue Ratio by Product Groups - Top5", base + """
SELECT product_group, overdue_ratio FROM (
  SELECT coalesce(f.product_group, 'Other') AS product_group,
         coalesce(sum(f.ovd_bal) / nullif(sum(f.port_bal), 0) * 100, 0) AS overdue_ratio
  FROM f, p WHERE f.as_of_date = p.d GROUP BY 1)
ORDER BY overdue_ratio DESC LIMIT 5
""", ["report_date", "branch"])
    ds_br = d.dataset("ovd_branch_options", "Branch options", f"""
SELECT 'all' AS branch UNION ALL
SELECT DISTINCT mng_br FROM {T} WHERE mng_br IS NOT NULL
""", [])

    c = OVD_COLORS
    d.text("# RETAIL LOAN OVERDUE TRACKING", 0, 0, 3, 2)
    d.counter(ds_fx, "fx_vnd_usd", "FX (VND/USD)", 3, 0, w=1, h=2, color=c["fx"], fmt="num")
    # main-stats repeat(3, 1fr)
    for i, (col, title, key, fmt) in enumerate([
            ("overdue_ratio", "Overdue Ratio", "ratio", "pct"),
            ("overdue_usd", "Overdue Balance (USD)", "overdue", "int"),
            ("ending_usd", "Ending Balance (USD)", "ending", "int")]):
        d.counter(ds_tick, col, title, 2 * i, 2, w=2, color=c["ticker"][key], fmt=fmt)
    # vs Last Day / Month / Year ngay dưới từng ticker (như stat-trends của bản gốc)
    for i, (col, title) in enumerate([("ratio_txt", "Overdue Ratio"),
                                      ("overdue_txt", "Overdue Balance (USD)"),
                                      ("ending_txt", "Ending Balance (USD)")]):
        d.table(ds_cmp, f"{title} vs Last Day / Month / Year",
                [("cmp", "Compare", "text"), (col, "Change", "text")], 2 * i, 5, 2, 3)
    # top-charts-grid 1fr 1.5fr -> 2 + 4
    d.combo(ds_5d, "Overdue Trend (Last 5 Days)", 0, 8, 2, 7, cat="day",
            bar="overdue_k_usd", line="overdue_ratio", bar_title="Overdue Amount (1000 USD)",
            line_title="Overdue Ratio (%)", bar_color=c["amount"], line_color=c["ratio"])
    d.combo(ds_mon, "Overdue Trend (Monthly)", 2, 8, 4, 7, cat="month",
            bar="overdue_k_usd", line="overdue_ratio", bar_title="Overdue Amount (1000 USD)",
            line_title="Overdue Ratio (%)", bar_color=c["amount"], line_color=c["ratio"])
    # debt-groups-grid 1fr
    d.combo(ds_dg, "Overdue Amount by Debt Groups", 0, 15, 6, 7, cat="month",
            bar="overdue_k_usd", line="overdue_ratio", bar_title="Overdue Amount (1000 USD)",
            line_title="Overdue Ratio (%)", bar_color=c["amount"], line_color=c["ratio"])
    # product-groups-grid 1fr 1fr
    d.chart("pie", ds_pa, "Overdue Amount by Product Groups", 0, 22, 3, 7,
            cat="product_group", val="overdue_k_usd", val_title="1000 USD")
    d.chart("bar", ds_pr, "Overdue Ratio by Product Groups", 3, 22, 3, 7,
            cat="product_group", val="overdue_ratio", horizontal=True,
            single_color=c["amount"], sort_desc=True, val_title="Overdue Ratio (%)")
    d.param_filter("report_date", "Report date", "filter-date-picker", 4, 0)
    d.param_filter("branch", "Branch", "filter-single-select", 5, 0,
                   options_ds=ds_br, options_col="branch")
    return d.to_json()
