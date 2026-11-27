"""Dashboard Card Business: chỉ sửa file này khi đổi dashboard này."""

from __future__ import annotations

from common.lakeview.core import USD_RATE, Dash, _arrow_pct, _q, _report_day

# ============================================================ 1. CARD BUSINESS
CARD_COLORS = {   # từ dashboard Chart.js gốc (đã xóa khỏi repo)
    "kpi": ["#1F2937", "#2F5597", "#C55A11", "#2F5597", "#5B9BD5"],
    "smart_credit": "#4472C4", "branches": "#B4C7E7", "new_issuance": "#2F5597",
    "termination": "#A5A5A5",
    "base": "#B4C7E7", "using": "#A5A5A5", "balance": "#4472C4", "revolving": "#BF9000",
    "volume": "#4472C4", "ticket": "#70AD47", "risk": "#2F5597",
    "sector": {"Purchasing": "#2F5597", "Installment": "#BF9000", "Cash Advance": "#7F7F7F",
               "Flexi": "#B4C7E7"},
    "location": {"Domestic": "#2F5597", "X-Border": "#BF9000"},
    "channel": {"POS (Offline)": "#2F5597", "Online": "#BF9000"},
}


def card_dashboard(catalog: str) -> dict:
    G = _q(catalog, "gold")
    d = Dash("card_business", "Card Business Dashboard")
    d.param("report_date", "Report date", "DATE", "now/d")
    d.param("customer_type", "Customer type", "STRING", "consumer")
    usd = USD_RATE.format(silver=_q(catalog, "silver"))
    base = f"""
WITH p AS (SELECT {_report_day(f'{G}.card_mart_kpi_daily')} AS d, {usd} AS usd),
f AS (
  SELECT k.* FROM {G}.card_mart_kpi_daily k
  WHERE :customer_type = 'all'
     OR k.cust_type = CASE :customer_type WHEN 'consumer' THEN 'CN' ELSE 'TC' END),
day AS (SELECT as_of_date, sum(eop_members) eop, sum(using_members) usg,
               sum(new_members) new_m, sum(closed_members) closed_m, sum(spend_amt) spend,
               sum(balance) bal, sum(revolving_balance) rev, sum(purchase_amt) purchase,
               sum(installment_amt) installment, sum(cash_advance_amt) cash,
               sum(flexi_amt) flexi, sum(domestic_amt) domestic, sum(xborder_amt) xborder,
               sum(pos_amt) pos, sum(online_amt) online,
               sum(dpd_1p_balance) dpd1, sum(dpd_10p_balance) dpd10,
               sum(dpd_30p_balance) dpd30, sum(npl_90p_balance) npl
        FROM f GROUP BY as_of_date),
mon AS (SELECT date_format(as_of_date, 'yyyyMM') AS ym, max(as_of_date) AS last_d,
               sum(new_m) new_m, sum(closed_m) closed_m, sum(spend) spend
        FROM day GROUP BY 1),
m7 AS (SELECT m.ym, m.new_m, m.closed_m, m.spend, x.eop, x.usg, x.bal, x.rev
       FROM mon m JOIN day x ON x.as_of_date = m.last_d CROSS JOIN p
       WHERE m.last_d <= p.d AND m.ym > date_format(add_months(p.d, -7), 'yyyyMM'))
"""
    ds_kpi = d.dataset("card_kpi", "Overview KPI", base + """
, cur AS (
  SELECT
    (SELECT sum(new_m) FROM day, p WHERE date_trunc('MM', as_of_date) = date_trunc('MM', p.d)
       AND as_of_date <= p.d) AS new_acquisition,
    x.eop AS customer_base, x.usg AS using_customer,
    (SELECT sum(spend) FROM day, p WHERE date_trunc('MM', as_of_date) = date_trunc('MM', p.d)
       AND as_of_date <= p.d) / p.usd / 1000 AS sales_volume_k_usd,
    x.bal / p.usd / 1000 AS balance_k_usd,
    x.dpd1 / nullif(x.bal, 0) * 100 AS dpd_1p, x.dpd10 / nullif(x.bal, 0) * 100 AS dpd_10p,
    x.dpd30 / nullif(x.bal, 0) * 100 AS dpd_30p, x.npl / nullif(x.bal, 0) * 100 AS npl
  FROM day x CROSS JOIN p WHERE x.as_of_date = p.d)
SELECT * FROM cur
""", ["report_date", "customer_type"])

    ds_cmp = d.dataset("card_kpi_compare", "Overview KPI vs last month / year / target", base + f"""
, pick AS (
  SELECT 'cur' AS k, p.d AS dd FROM p
  UNION ALL SELECT 'lm', (SELECT max(as_of_date) FROM day, p
                          WHERE as_of_date <= last_day(add_months(p.d, -1))) FROM p
  UNION ALL SELECT 'ly', (SELECT max(as_of_date) FROM day, p
                          WHERE as_of_date <= add_months(p.d, -12)) FROM p),
v AS (
  SELECT pk.k,
    (SELECT sum(new_m) FROM day WHERE date_trunc('MM', as_of_date) = date_trunc('MM', pk.dd)
       AND as_of_date <= pk.dd) AS new_acq,
    x.eop, x.usg,
    (SELECT sum(spend) FROM day WHERE date_trunc('MM', as_of_date) = date_trunc('MM', pk.dd)
       AND as_of_date <= pk.dd) AS spend,
    x.bal
  FROM pick pk LEFT JOIN day x ON x.as_of_date = pk.dd),
t AS (
  SELECT metric_code, period_type, sum(target_value) AS target
  FROM {_q(catalog, "silver")}.ref_targets, p
  WHERE (period_type = 'MONTH' AND period = date_format(p.d, 'yyyy-MM'))
     OR (period_type = 'YEAR' AND period = CAST(year(p.d) AS STRING))
  GROUP BY 1, 2),
ytd AS (SELECT sum(new_m) AS new_ytd, sum(spend) AS spend_ytd FROM day, p
        WHERE year(as_of_date) = year(p.d) AND as_of_date <= p.d),
k AS (
  SELECT 1 AS ord, 'New Acquisition' AS kpi, c.new_acq AS cur, lm.new_acq AS lm,
         ly.new_acq AS ly, 'CARD_NEW_ACQ' AS code, ytd.new_ytd AS ytd_val
  FROM v c, v lm, v ly, ytd WHERE c.k = 'cur' AND lm.k = 'lm' AND ly.k = 'ly'
  UNION ALL SELECT 2, 'Customer Base', c.eop, lm.eop, ly.eop, NULL, NULL
  FROM v c, v lm, v ly WHERE c.k = 'cur' AND lm.k = 'lm' AND ly.k = 'ly'
  UNION ALL SELECT 3, 'Using Customer', c.usg, lm.usg, ly.usg, NULL, NULL
  FROM v c, v lm, v ly WHERE c.k = 'cur' AND lm.k = 'lm' AND ly.k = 'ly'
  UNION ALL SELECT 4, 'Sales Volume (K USD)', c.spend, lm.spend, ly.spend,
         'CARD_SALES_VOLUME', ytd.spend_ytd
  FROM v c, v lm, v ly, ytd WHERE c.k = 'cur' AND lm.k = 'lm' AND ly.k = 'ly'
  UNION ALL SELECT 5, 'Balance (K USD)', c.bal, lm.bal, ly.bal, NULL, NULL
  FROM v c, v lm, v ly WHERE c.k = 'cur' AND lm.k = 'lm' AND ly.k = 'ly')
, r AS (
  SELECT k.ord,
    (k.cur - k.lm) / nullif(k.lm, 0) * 100 AS mom,
    (k.cur - k.ly) / nullif(k.ly, 0) * 100 AS yoy,
    k.cur / nullif(tm.target, 0) * 100 AS tgt_m,
    k.ytd_val / nullif(ty.target, 0) * 100 AS tgt_y
  FROM k
  LEFT JOIN t tm ON tm.metric_code = k.code AND tm.period_type = 'MONTH'
  LEFT JOIN t ty ON ty.metric_code = k.code AND ty.period_type = 'YEAR'),
cmp_rows AS (
  SELECT x.line, x.compare, r.ord, x.val FROM r
  LATERAL VIEW inline(array(
    named_struct('line', 1, 'compare', 'vs Last Month', 'val', {{mom}}),
    named_struct('line', 2, 'compare', 'vs Last Year', 'val', {{yoy}}),
    named_struct('line', 3, 'compare', '% of Month Target', 'val',
                 CASE WHEN tgt_m IS NULL THEN '-' ELSE concat(format_number(tgt_m, 0), '%') END),
    named_struct('line', 4, 'compare', '% of Year Target', 'val',
                 CASE WHEN tgt_y IS NULL THEN '-' ELSE concat(format_number(tgt_y, 0), '%') END)
  )) x)
SELECT compare,
  max(CASE WHEN ord = 1 THEN val END) AS new_acquisition,
  max(CASE WHEN ord = 2 THEN val END) AS customer_base,
  max(CASE WHEN ord = 3 THEN val END) AS using_customer,
  max(CASE WHEN ord = 4 THEN val END) AS sales_volume,
  max(CASE WHEN ord = 5 THEN val END) AS balance
FROM cmp_rows GROUP BY line, compare ORDER BY line
""".format(mom=_arrow_pct("mom"), yoy=_arrow_pct("yoy")), ["report_date", "customer_type"])

    ds_new = d.dataset("card_new_acq", "New Acquisition by channel", f"""
WITH p AS (SELECT {_report_day(f'{G}.card_mart_kpi_daily')} AS d)
SELECT date_format(k.as_of_date, 'yyyyMM') AS month,
       CASE k.acquisition_channel WHEN 'SMART_CREDIT' THEN 'Smart Credit'
            ELSE 'Branches' END AS channel,
       sum(k.new_members) AS cards
FROM {G}.card_mart_kpi_daily k CROSS JOIN p
WHERE k.as_of_date <= p.d
  AND date_format(k.as_of_date, 'yyyyMM') > date_format(add_months(p.d, -7), 'yyyyMM')
  AND (:customer_type = 'all'
       OR k.cust_type = CASE :customer_type WHEN 'consumer' THEN 'CN' ELSE 'TC' END)
GROUP BY 1, 2
""", ["report_date", "customer_type"])
    ds_inout = d.dataset("card_in_out", "In & Out", base + """
SELECT ym AS month, 'New Issuance' AS series, new_m AS cards FROM m7
UNION ALL SELECT ym, 'Termination', closed_m FROM m7
""", ["report_date", "customer_type"])
    ds_base = d.dataset("card_base_using", "Customer Base & Using Mem", base + """
SELECT ym AS month, 'Customer Base' AS series, eop AS members FROM m7
UNION ALL SELECT ym, 'Using Mem', usg FROM m7
""", ["report_date", "customer_type"])
    ds_bal = d.dataset("card_balance", "Balance (K USD)", base + """
SELECT m7.ym AS month, 'Balance' AS series, m7.bal / p.usd / 1000 AS k_usd FROM m7, p
UNION ALL SELECT m7.ym, 'Revolving', m7.rev / p.usd / 1000 FROM m7, p
""", ["report_date", "customer_type"])
    ds_vol = d.dataset("card_volume", "Sales Volume (K USD)", base + """
SELECT m7.ym AS month, m7.spend / p.usd / 1000 AS k_usd FROM m7, p
""", ["report_date", "customer_type"])
    ds_spend = d.dataset("card_ticket", "Spend per cus (USD)", base + """
SELECT m7.ym AS month, m7.spend / p.usd / nullif(m7.usg, 0) AS ticket_size FROM m7, p
""", ["report_date", "customer_type"])
    # 3 donut cần 3 dataset con (widget không tự lọc theo cột)
    mix = {}
    for donut in ("sector", "location", "channel"):
        mix[donut] = d.dataset(f"card_mix_{donut}", f"Spend by {donut}", base + f"""
, x AS (SELECT day.* FROM day, p WHERE day.as_of_date = p.d),
all_mix AS (
  SELECT 'sector' AS donut, 'Purchasing' AS slice, purchase AS amount FROM x
  UNION ALL SELECT 'sector', 'Installment', installment FROM x
  UNION ALL SELECT 'sector', 'Cash Advance', cash FROM x
  UNION ALL SELECT 'sector', 'Flexi', flexi FROM x
  UNION ALL SELECT 'location', 'Domestic', domestic FROM x
  UNION ALL SELECT 'location', 'X-Border', xborder FROM x
  UNION ALL SELECT 'channel', 'POS (Offline)', pos FROM x
  UNION ALL SELECT 'channel', 'Online', online FROM x)
SELECT slice, amount FROM all_mix WHERE donut = '{donut}'
""", ["report_date", "customer_type"])
    ds_risk = d.dataset("card_risk_compare", "Delinquency vs last day / month", base + """
, pk AS (SELECT p.d AS cur,
                (SELECT max(as_of_date) FROM day, p WHERE as_of_date < p.d) AS d1,
                (SELECT max(as_of_date) FROM day, p
                 WHERE as_of_date <= last_day(add_months(p.d, -1))) AS m1 FROM p),
rt AS (SELECT as_of_date, dpd1 / nullif(bal, 0) * 100 AS r1, dpd10 / nullif(bal, 0) * 100 AS r10,
              dpd30 / nullif(bal, 0) * 100 AS r30, npl / nullif(bal, 0) * 100 AS rn FROM day),
v AS (SELECT c.r1 - d.r1 AS d_r1, c.r10 - d.r10 AS d_r10, c.r30 - d.r30 AS d_r30,
             c.rn - d.rn AS d_rn, c.r1 - m.r1 AS m_r1, c.r10 - m.r10 AS m_r10,
             c.r30 - m.r30 AS m_r30, c.rn - m.rn AS m_rn
      FROM pk JOIN rt c ON c.as_of_date = pk.cur
      LEFT JOIN rt d ON d.as_of_date = pk.d1 LEFT JOIN rt m ON m.as_of_date = pk.m1)
SELECT 1 AS line, 'vs Last Day' AS compare, {d1} AS dpd_1p, {d10} AS dpd_10p,
       {d30} AS dpd_30p, {dn} AS npl FROM v
UNION ALL
SELECT 2, 'vs Last Month', {m1}, {m10}, {m30}, {mn} FROM v
ORDER BY line
""".format(d1=_arrow_pct("d_r1", " pp"), d10=_arrow_pct("d_r10", " pp"),
           d30=_arrow_pct("d_r30", " pp"), dn=_arrow_pct("d_rn", " pp"),
           m1=_arrow_pct("m_r1", " pp"), m10=_arrow_pct("m_r10", " pp"),
           m30=_arrow_pct("m_r30", " pp"), mn=_arrow_pct("m_rn", " pp")),
        ["report_date", "customer_type"])
    ds_ct = d.dataset("card_customer_types", "Customer type options", """
SELECT explode(array('consumer', 'corporate', 'all')) AS customer_type
""", [])

    c = CARD_COLORS
    # Bố cục theo card_dashboard_final.html: header + bộ lọc, Overview (5 thẻ KPI + dòng so
    # sánh ngay dưới), New Issuance vs. Termination, Sales Performance (2x2), Spending
    # Distribution (3 donut), Delinquency (4 chỉ số + thay đổi). Lưới Lakeview = 6 cột.
    d.text("# CARD BUSINESS DASHBOARD\n**Performance & Growth**", 0, 0, 4, 2)
    d.text("### 🟧 Overview", 0, 2)
    for i, (col, title, fmt) in enumerate([
            ("new_acquisition", "New Acquisition", "int"),
            ("customer_base", "Customer Base", "int"),
            ("using_customer", "Using Customer", "int"),
            ("sales_volume_k_usd", "Sales Volume (K USD)", "int"),
            ("balance_k_usd", "Balance (K USD)", "int")]):
        d.counter(ds_kpi, col, title, i, 3, color=c["kpi"][i], fmt=fmt)
    d.text("**Report scope**\n\nMoM / YoY và % chỉ tiêu ở bảng bên dưới; "
           "lọc Consumer / Corporate / Total ở góc phải.", 5, 3, 1, 3)
    d.table(ds_cmp, "", [
        ("compare", "", "text"), ("new_acquisition", "New Acquisition", "text"),
        ("customer_base", "Customer Base", "text"), ("using_customer", "Using Customer", "text"),
        ("sales_volume", "Sales Volume", "text"), ("balance", "Balance", "text")], 0, 6, 6, 3)

    y = 9
    d.text("### 🟧 New Issuance vs. Termination", 0, y)
    d.chart("bar", ds_new, "New Acquisition", 0, y + 1, 3, 7, cat="month", series="channel",
            val="cards", layout="stack",
            colors={"Smart Credit": c["smart_credit"], "Branches": c["branches"]})
    d.chart("bar", ds_inout, "In & Out", 3, y + 1, 3, 7, cat="month", series="series",
            val="cards", layout="group",
            colors={"New Issuance": c["new_issuance"], "Termination": c["termination"]})

    y = 17
    d.text("### 🟧 Sales Performance", 0, y)
    d.chart("bar", ds_base, "Customer Base & Using Mem", 0, y + 1, 3, 7, cat="month",
            series="series", val="members", layout="group", labels=False,
            colors={"Customer Base": c["base"], "Using Mem": c["using"]})
    d.chart("bar", ds_bal, "Balance (K USD)", 3, y + 1, 3, 7, cat="month", series="series",
            val="k_usd", layout="stack", labels=False,
            colors={"Balance": c["balance"], "Revolving": c["revolving"]})
    d.chart("bar", ds_vol, "Sales Volume (K USD)", 0, y + 8, 3, 7, cat="month", val="k_usd",
            single_color=c["volume"], labels=False)
    d.chart("line", ds_spend, "Spend per cus (USD)", 3, y + 8, 3, 7, cat="month",
            val="ticket_size", single_color=c["ticket"], labels=False)

    y = 33
    d.text("### 🟧 Spending Distribution", 0, y)
    for i, (donut, title) in enumerate([("sector", "By Sectors"), ("location", "By Locations"),
                                        ("channel", "By Channels")]):
        d.chart("pie", mix[donut], title, 2 * i, y + 1, 2, 6, cat="slice", val="amount",
                colors=c[donut])

    y = 40
    d.text("### 🟧 Delinquency", 0, y)
    for i, (col, title) in enumerate([("dpd_1p", "DPD 1+ (%)"), ("dpd_10p", "DPD 10+ (%)"),
                                      ("dpd_30p", "DPD 30+ (%)"), ("npl", "NPL (%)")]):
        d.counter(ds_kpi, col, title, 1 + i, y + 1, color=c["risk"], fmt="pct")
    d.table(ds_risk, "", [
        ("compare", "", "text"), ("dpd_1p", "DPD 1+", "text"), ("dpd_10p", "DPD 10+", "text"),
        ("dpd_30p", "DPD 30+", "text"), ("npl", "NPL", "text")], 1, y + 4, 4, 2)
    # ---- bộ lọc (đặt sau khi có đủ dataset)
    d.param_filter("report_date", "Report date", "filter-date-picker", 4, 0)
    d.param_filter("customer_type", "Customer type", "filter-single-select", 5, 0,
                   options_ds=ds_ct, options_col="customer_type")
    return d.to_json()
