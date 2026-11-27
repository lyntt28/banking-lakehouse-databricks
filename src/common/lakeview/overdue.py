"""Dashboard Loan Overdue (LOAN): chỉ sửa file này khi đổi dashboard này."""

from __future__ import annotations

from common.lakeview.core import USD_RATE, Dash, _q, _report_day

# ============================================================ 2. OVERDUE (LOAN)
OVD_COLORS = {   # từ dashboard Chart.js gốc (đã xóa khỏi repo)
    "amount": "#3498DB", "ratio": "#E67E22",
    "ticker": {"ratio": "#48bb78", "overdue": "#4299e1", "ending": "#ed8936"},
    "debt_groups": {"Group 2": "#90CAF9", "Group 3": "#42A5F5", "Group 4": "#1E88E5",
                    "Group 5": "#0D47A1"},
    "products": ["#3498DB", "#E74C3C", "#2ECC71", "#F39C12", "#9B59B6", "#1ABC9C", "#E67E22",
                 "#34495E", "#E91E63", "#00BCD4"],
    "top5": ["#3498DB", "#E74C3C", "#2ECC71", "#F39C12", "#9B59B6"],
}


# nhóm sản phẩm vay (products.product_group), màu theo thứ tự như modernColors gốc
LOAN_PRODUCT_GROUPS = ["Nhom SP tai tro TMTN", "Nhom SP tai tro du an va dau tu TSCD",
                       "Nhom SP cho vay tieu dung"]


def overdue_dashboard(catalog: str) -> dict:
    G = _q(catalog, "gold")
    d = Dash("overdue", "Retail Loan Overdue Tracking")
    d.param("report_date", "Report date", "DATE", "now/d")
    d.param("branch", "Branch", "STRING", "all")
    usd = USD_RATE.format(silver=_q(catalog, "silver"))
    base = f"""
WITH p AS (SELECT {_report_day(f'{G}.loan_mart_overdue_daily')} AS d, {usd} AS usd),
f AS (SELECT * FROM {G}.loan_mart_overdue_daily WHERE :branch = 'all' OR branchnbr = :branch),
day AS (SELECT as_of_date, sum(overdue_balance) ovd, sum(portfolio_balance) port
        FROM f GROUP BY as_of_date)
"""
    ds_tick = d.dataset("ovd_tickers", "Tickers", base + """
SELECT x.ovd / nullif(x.port, 0) * 100 AS overdue_ratio,
       x.ovd / p.usd AS overdue_usd, x.port / p.usd AS ending_usd
FROM day x, p WHERE x.as_of_date = p.d
""", ["report_date", "branch"])
    ds_cmp = d.dataset("ovd_compare", "Tickers vs last day / month / year", base + """
, pick AS (
  SELECT 1 AS ord, 'vs Last Day' AS cmp,
         (SELECT max(as_of_date) FROM day, p WHERE as_of_date < p.d) AS dd FROM p
  UNION ALL SELECT 2, 'vs Last Month', (SELECT max(as_of_date) FROM day, p
         WHERE as_of_date <= last_day(add_months(p.d, -1))) FROM p
  UNION ALL SELECT 3, 'vs Last Year', (SELECT max(as_of_date) FROM day, p
         WHERE as_of_date <= make_date(year(p.d) - 1, 12, 31)) FROM p),
cur AS (SELECT x.* FROM day x, p WHERE x.as_of_date = p.d)
SELECT pk.ord, pk.cmp,
  (cur.ovd / nullif(cur.port, 0) - c.ovd / nullif(c.port, 0)) * 100 AS ratio_pp,
  (cur.ovd - c.ovd) / p.usd AS overdue_usd, (cur.port - c.port) / p.usd AS ending_usd
FROM pick pk CROSS JOIN cur CROSS JOIN p LEFT JOIN day c ON c.as_of_date = pk.dd
ORDER BY pk.ord
""", ["report_date", "branch"])
    ds_5d = d.dataset("ovd_last5", "Overdue Trend (Last 5 Days)", base + """
SELECT date_format(x.as_of_date, 'dd/MM') AS day, x.ovd / p.usd / 1000 AS overdue_k_usd,
       x.ovd / nullif(x.port, 0) * 100 AS overdue_ratio
FROM day x, p WHERE x.as_of_date IN (
  SELECT as_of_date FROM day, p WHERE as_of_date <= p.d ORDER BY as_of_date DESC LIMIT 5)
""", ["report_date", "branch"])
    ds_mon = d.dataset("ovd_monthly", "Overdue Trend (Monthly)", base + """
, m AS (SELECT date_format(as_of_date, 'yyyy-MM') AS month, max(as_of_date) AS last_d
        FROM day, p WHERE as_of_date <= p.d GROUP BY 1)
SELECT m.month, x.ovd / p.usd / 1000 AS overdue_k_usd,
       x.ovd / nullif(x.port, 0) * 100 AS overdue_ratio
FROM m JOIN day x ON x.as_of_date = m.last_d CROSS JOIN p
WHERE m.month > date_format(add_months(p.d, -6), 'yyyy-MM')
""", ["report_date", "branch"])
    ds_dg = d.dataset("ovd_debt_groups", "Overdue Amount by Debt Groups", base + """
, m AS (SELECT date_format(as_of_date, 'yyyy-MM') AS month, max(as_of_date) AS last_d
        FROM day, p WHERE as_of_date <= p.d GROUP BY 1)
SELECT m.month, concat('Group ', f.debt_group) AS debt_group,
       sum(f.overdue_balance) / p.usd / 1000 AS overdue_k_usd
FROM m JOIN f ON f.as_of_date = m.last_d CROSS JOIN p
WHERE f.debt_group >= 2 AND m.month > date_format(add_months(p.d, -6), 'yyyy-MM')
GROUP BY m.month, f.debt_group, p.usd
""", ["report_date", "branch"])
    ds_pa = d.dataset("ovd_product_amount", "Overdue Amount by Product Groups", base + """
SELECT coalesce(f.product_group, 'Other') AS product_group,
       sum(f.overdue_balance) / p.usd / 1000 AS overdue_k_usd
FROM f, p WHERE f.as_of_date = p.d GROUP BY 1, p.usd
""", ["report_date", "branch"])
    ds_pr = d.dataset("ovd_product_ratio", "Overdue Ratio by Product Groups", base + """
SELECT coalesce(f.product_group, 'Other') AS product_group,
       sum(f.overdue_balance) / nullif(sum(f.portfolio_balance), 0) * 100 AS overdue_ratio
FROM f, p WHERE f.as_of_date = p.d GROUP BY 1
""", ["report_date", "branch"])
    ds_top = d.dataset("ovd_branch_top5", "Overdue Ratio by Branches - Top5", f"""
WITH p AS (SELECT {_report_day(f'{G}.loan_mart_overdue_daily')} AS d),
b AS (SELECT o.branchnbr, coalesce(db.br_name, o.branchnbr) AS branch,
             sum(o.overdue_balance) / nullif(sum(o.portfolio_balance), 0) * 100 AS ratio
      FROM {G}.loan_mart_overdue_daily o CROSS JOIN p
      LEFT JOIN {G}.dim_branch db ON db.branchnbr = o.branchnbr
      WHERE o.as_of_date = p.d GROUP BY 1, 2)
SELECT concat('#', rn) AS rank, concat('#', rn, ' ', branch) AS branch, ratio
FROM (SELECT *, row_number() OVER (ORDER BY ratio DESC) AS rn FROM b) WHERE rn <= 5
""", ["report_date"])
    ds_br = d.dataset("ovd_branch_options", "Branch options", f"""
SELECT 'all' AS branch UNION ALL
SELECT DISTINCT branchnbr FROM {G}.dim_branch
""", [])

    c = OVD_COLORS
    d.text("# RETAIL LOAN OVERDUE TRACKING", 0, 0, 4, 2)
    # main-stats repeat(3, 1fr)
    for i, (col, title, key, fmt) in enumerate([
            ("overdue_ratio", "Overdue Ratio", "ratio", "pct"),
            ("overdue_usd", "Overdue Balance (USD)", "overdue", "compact"),
            ("ending_usd", "Ending Balance (USD)", "ending", "compact")]):
        d.counter(ds_tick, col, title, 2 * i, 2, w=2, color=c["ticker"][key], fmt=fmt)
    d.table(ds_cmp, "Comparison", [
        ("cmp", "", "text"), ("ratio_pp", "Overdue Ratio (pp)", "pct"),
        ("overdue_usd", "Overdue Balance (USD)", "num"),
        ("ending_usd", "Ending Balance (USD)", "num")], 0, 5, 6, 3)
    # top-charts-grid 1fr 1.5fr -> 2 + 4
    d.combo(ds_5d, "Overdue Trend (Last 5 Days)", 0, 8, 2, 7, cat="day",
            bar="overdue_k_usd", line="overdue_ratio", bar_title="Overdue Amount (1000 USD)",
            line_title="Overdue Ratio (%)", bar_color=c["amount"], line_color=c["ratio"])
    d.combo(ds_mon, "Overdue Trend (Monthly)", 2, 8, 4, 7, cat="month",
            bar="overdue_k_usd", line="overdue_ratio", bar_title="Overdue Amount (1000 USD)",
            line_title="Overdue Ratio (%)", bar_color=c["amount"], line_color=c["ratio"])
    # debt-groups-grid 1fr
    d.chart("bar", ds_dg, "Overdue Amount by Debt Groups", 0, 15, 6, 7, cat="month",
            series="debt_group", val="overdue_k_usd", layout="stack",
            colors=c["debt_groups"], val_title="1000 USD")
    # product-groups-grid 1fr 1fr
    d.chart("pie", ds_pa, "Overdue Amount by Product Groups", 0, 22, 3, 7,
            cat="product_group", val="overdue_k_usd",
            colors=dict(zip(LOAN_PRODUCT_GROUPS + ["Other"], c["products"][:4], strict=True)))
    d.chart("bar", ds_pr, "Overdue Ratio by Product Groups", 3, 22, 3, 7,
            cat="product_group", val="overdue_ratio", horizontal=True,
            single_color=c["amount"], sort_desc=True)
    # branches-grid 1fr
    d.chart("bar", ds_top, "Overdue Ratio by Branches - Top5", 0, 29, 6, 6, cat="branch",
            val="ratio", horizontal=True, series="rank", sort_desc=True,
            colors={f"#{i + 1}": col for i, col in enumerate(c["top5"])})
    d.param_filter("report_date", "Report date", "filter-date-picker", 4, 0)
    d.param_filter("branch", "Branch", "filter-single-select", 5, 0,
                   options_ds=ds_br, options_col="branch")
    return d.to_json()
