"""Dashboard Card Business: chỉ sửa file này khi đổi dashboard này."""

from __future__ import annotations

from common.lakeview import vega
from common.lakeview.core import NEUTRAL, S1, S2, S3, S4, S5, S6, TEXT, Dash, _q, kpi_cards_sql, lit

# ============================================================ 1. CARD BUSINESS
CARD_COLORS = {   # bảng màu chung (core.SERIES): màu theo thực thể, giống nhau ở 3 dashboard
    "kpi": [TEXT, S1, S5, S3, S6],   # màu số KPI như bản gốc
    "smart_credit": S1, "branches": S2, "new_issuance": S1, "termination": NEUTRAL,
    "base": S1, "using": S2, "balance": S1, "revolving": S2,
    "volume": S1, "ticket": S2, "risk": TEXT,
    "sector": {"Purchasing": S1, "Installment": S2, "Cash Advance": S3, "Flexi": S4},
    "location": {"Domestic": S1, "X-Border": S2},
    "channel": {"POS (Offline)": S1, "Online": S2},
}


KPI_CODES = [("new_acquisition", "NEW_ACQ", "flow"), ("customer_base", "CUSTOMER_BASE", "stock"),
             ("using_customer", "USING", "month"), ("sales_volume", "SALES_VOLUME", "flow"),
             ("balance", "BALANCE", "stock")]


def card_sql(catalog: str) -> dict[str, str]:
    """SQL dataset Card. Ngày báo cáo = ngày publish gần nhất <= :report_date (lịch
    `gold_serving.card_report_calendar`), đọc `card_mart_kpi_monthly` tại ĐÚNG ngày đó (mart
    đã chốt tồn / MTD / YTD / thẻ dùng distinct theo ngày này -> chọn giữa tháng vẫn có tháng
    hiện tại, không đọc tương lai). Luỹ kế thiếu ngày -> NULL (N/A), không hiện 0.

    So sánh: vs Last Month = tháng trước (cuối tháng); vs Last Year-end = tháng 12 năm trước
    (như API gốc, KHÔNG phải YoY cùng kỳ). Tỷ lệ rủi ro so sánh bằng điểm % (pp).
    Chỉ tiêu: cùng loại KH với số thực hiện (mã CARD_<KPI>_CN / _TC)."""
    G, S = _q(catalog, "gold_serving"), _q(catalog, "silver")
    CAL, M, D = (f"{G}.card_report_calendar", f"{G}.card_mart_kpi_monthly",
                 f"{G}.card_mart_kpi_daily")
    ct = "CASE :customer_type WHEN 'consumer' THEN 'CN' ELSE 'TC' END"
    cust = f"(:customer_type = 'all' OR f.cust_type = {ct})"
    cur = f"""
WITH cur AS (
  SELECT c.report_date AS d, c.previous_date, c.previous_month_date,
         date_format(c.report_date, 'yyyy-MM') AS cm,
         date_format(add_months(c.report_date, -1), 'yyyy-MM') AS lm,
         concat(CAST(year(c.report_date) - 1 AS STRING), '-12') AS ye,
         (SELECT max_by(x.rate_to_vnd, x.rate_date) FROM {S}.ref_fx_rates x
          WHERE x.currencycd = 'USD' AND x.rate_date <= c.report_date) AS usd
  FROM {CAL} c
  WHERE c.report_date = (SELECT max(report_date) FROM {CAL} WHERE report_date <= :report_date))"""

    def flow(c: str, flag: str = "flow_complete") -> str:
        return f"CASE WHEN bool_and(f.{flag}) THEN sum(f.{c}) END"

    month = cur + f"""
, m AS (
  SELECT f.year_month, max(f.snapshot_date) AS snapshot_date,
         {flow("new_members")} AS new_m, {flow("closed_members")} AS closed_m,
         {flow("using_members")} AS usg, {flow("spend_amt")} AS spend,
         {flow("new_members_ytd", "ytd_complete")} AS new_ytd,
         {flow("spend_amt_ytd", "ytd_complete")} AS spend_ytd,
         {flow("purchase_amt")} AS purchase, {flow("installment_amt")} AS installment,
         {flow("cash_advance_amt")} AS cash, {flow("flexi_amt")} AS flexi,
         {flow("domestic_amt")} AS domestic, {flow("xborder_amt")} AS xborder,
         {flow("pos_amt")} AS pos, {flow("online_amt")} AS online,
         sum(f.eop_members) AS eop, sum(f.balance) AS bal, sum(f.revolving_balance) AS rev,
         sum(f.dpd_1p_balance) AS dpd1, sum(f.dpd_10p_balance) AS dpd10,
         sum(f.dpd_30p_balance) AS dpd30, sum(f.npl_90p_balance) AS npl
  FROM {M} f JOIN cur ON f.as_of_date = cur.d
  WHERE {cust}
  GROUP BY f.year_month),
six AS (SELECT m.* FROM m CROSS JOIN cur
        WHERE m.year_month > date_format(add_months(cur.d, -6), 'yyyy-MM'))"""
    k = "/ cur.usd / 1000"
    tg_cols = ",\n".join(
        f"  sum(CASE WHEN t.code = 'CARD_{code}' AND t.period_type = 'MONTH' "
        f"THEN t.target_value END)"
        f" AS {col}_tm,\n"
        f"  sum(CASE WHEN t.code = 'CARD_{code}' AND t.period_type = 'YEAR' "
        f"THEN t.target_value END)"
        f" AS {col}_ty" for col, code, _ in KPI_CODES)
    mix_rows = {
        "sector": [("Purchasing", "purchase"), ("Installment", "installment"),
                   ("Cash Advance", "cash"), ("Flexi", "flexi")],
        "location": [("Domestic", "domestic"), ("X-Border", "xborder")],
        "channel": [("POS (Offline)", "pos"), ("Online", "online")]}
    out = {
        "card_kpi": month + f"""
SELECT cur.d AS report_date, m.new_m AS new_acquisition, m.eop AS customer_base,
       m.usg AS using_customer, m.spend {k} AS sales_volume_k_usd, m.bal {k} AS balance_k_usd,
       m.dpd1 / nullif(m.bal, 0) * 100 AS dpd_1p, m.dpd10 / nullif(m.bal, 0) * 100 AS dpd_10p,
       m.dpd30 / nullif(m.bal, 0) * 100 AS dpd_30p, m.npl / nullif(m.bal, 0) * 100 AS npl,
       -- tỷ lệ của ngày có dữ liệu liền trước (ô Comparison của counter rủi ro)
       p.dpd_1p_prev, p.dpd_10p_prev, p.dpd_30p_prev, p.npl_prev,
       -- tháng trước (ô Comparison của 5 KPI; luỹ kế tháng thiếu ngày -> NULL)
       lm.new_m AS new_acquisition_lm, lm.eop AS customer_base_lm, lm.usg AS using_customer_lm,
       lm.spend {k} AS sales_volume_k_usd_lm, lm.bal {k} AS balance_k_usd_lm
FROM cur LEFT JOIN m ON m.year_month = cur.cm
LEFT JOIN m lm ON lm.year_month = cur.lm
LEFT JOIN (
  SELECT sum(f.dpd_1p_balance) / nullif(sum(f.balance), 0) * 100 AS dpd_1p_prev,
         sum(f.dpd_10p_balance) / nullif(sum(f.balance), 0) * 100 AS dpd_10p_prev,
         sum(f.dpd_30p_balance) / nullif(sum(f.balance), 0) * 100 AS dpd_30p_prev,
         sum(f.npl_90p_balance) / nullif(sum(f.balance), 0) * 100 AS npl_prev
  FROM {D} f JOIN cur c2 ON f.as_of_date = c2.previous_date WHERE {cust}) p ON true""",
        # % thay đổi tương đối (số) + % hoàn thành chỉ tiêu; thiếu dữ liệu -> NULL
        "card_kpi_compare": month + f"""
, t AS (SELECT substr(f.metric_code, 1, length(f.metric_code) - 3) AS code, f.period_type,
               f.target_value
        FROM {S}.ref_targets f CROSS JOIN cur
        WHERE left(f.metric_code, 5) = 'CARD_'
          AND (:customer_type = 'all' OR right(f.metric_code, 2) = {ct})
          AND ((f.period_type = 'MONTH' AND f.period = cur.cm)
               OR (f.period_type = 'YEAR' AND f.period = CAST(year(cur.d) AS STRING)))),
tg AS (SELECT
{tg_cols}
  FROM t),
c AS (SELECT * FROM m CROSS JOIN cur WHERE m.year_month = cur.cm),
p AS (SELECT m.*, 1 AS line FROM m CROSS JOIN cur WHERE m.year_month = cur.lm
      UNION ALL SELECT m.*, 2 FROM m CROSS JOIN cur WHERE m.year_month = cur.ye)
SELECT l.line, concat(CASE l.line WHEN 1 THEN 'vs Last Month (' ELSE 'vs Last Year-end (' END,
                      l.ym, ')') AS compare,
       round((c.new_m - p.new_m) / nullif(p.new_m, 0) * 100, 1) AS new_acquisition,
       round((c.eop - p.eop) / nullif(p.eop, 0) * 100, 1) AS customer_base,
       round((c.usg - p.usg) / nullif(p.usg, 0) * 100, 1) AS using_customer,
       round((c.spend - p.spend) / nullif(p.spend, 0) * 100, 1) AS sales_volume,
       round((c.bal - p.bal) / nullif(p.bal, 0) * 100, 1) AS balance
FROM (SELECT 1 AS line, lm AS ym FROM cur UNION ALL SELECT 2, ye FROM cur) l
CROSS JOIN cur LEFT JOIN c ON true LEFT JOIN p ON p.line = l.line
UNION ALL
SELECT 3, '% of Month Target',
       round(c.new_m / nullif(tg.new_acquisition_tm, 0) * 100, 1),
       round(c.eop / nullif(tg.customer_base_tm, 0) * 100, 1),
       round(c.usg / nullif(tg.using_customer_tm, 0) * 100, 1),
       round(c.spend / nullif(tg.sales_volume_tm, 0) * 100, 1),
       round(c.bal / nullif(tg.balance_tm, 0) * 100, 1)
FROM tg LEFT JOIN c ON true
UNION ALL
-- năm: luỹ kế (flow) YTD / chỉ tiêu năm; tồn: hiện tại / chỉ tiêu cuối năm; using: không có
SELECT 4, '% of Year Target',
       round(c.new_ytd / nullif(tg.new_acquisition_ty, 0) * 100, 1),
       round(c.eop / nullif(tg.customer_base_ty, 0) * 100, 1), CAST(NULL AS DOUBLE),
       round(c.spend_ytd / nullif(tg.sales_volume_ty, 0) * 100, 1),
       round(c.bal / nullif(tg.balance_ty, 0) * 100, 1)
FROM tg LEFT JOIN c ON true
ORDER BY 1""",
        "card_new_acq": cur + f"""
SELECT f.year_month AS month,
       CASE f.acquisition_channel WHEN 'SMART_CREDIT' THEN 'Smart Credit' ELSE 'Branches' END
           AS channel,
       {flow("new_members")} AS cards
FROM {M} f JOIN cur ON f.as_of_date = cur.d
WHERE {cust} AND f.year_month > date_format(add_months(cur.d, -6), 'yyyy-MM')
GROUP BY 1, 2""",
        "card_in_out": month + """
SELECT year_month AS month, 'New Issuance' AS series, new_m AS cards FROM six
UNION ALL SELECT year_month, 'Termination', closed_m FROM six""",
        "card_base_using": month + """
SELECT year_month AS month, 'Customer Base' AS series, eop AS members FROM six
UNION ALL SELECT year_month, 'Using Mem', usg FROM six""",
        # 2 cột riêng (Revolving là 1 phần của Balance -> KHÔNG xếp chồng)
        "card_balance": month + f"""
SELECT six.year_month AS month, 'Balance' AS series, six.bal {k} AS k_usd
FROM six CROSS JOIN cur
UNION ALL SELECT six.year_month, 'Revolving', six.rev {k} FROM six CROSS JOIN cur""",
        "card_volume": month + f"""
SELECT six.year_month AS month, six.spend {k} AS k_usd FROM six CROSS JOIN cur""",
        # chi tiêu / thẻ hiện hữu (EOP), như API gốc
        "card_ticket": month + """
SELECT six.year_month AS month, six.spend / cur.usd / nullif(six.eop, 0) AS spend_per_member
FROM six CROSS JOIN cur""",
        # điểm % so với ngày trước / cuối tháng trước, trên mart ngày (đúng 3 ngày)
        "card_risk_compare": cur + f"""
, rt AS (SELECT f.as_of_date, sum(f.dpd_1p_balance) / nullif(sum(f.balance), 0) * 100 AS r1,
                sum(f.dpd_10p_balance) / nullif(sum(f.balance), 0) * 100 AS r10,
                sum(f.dpd_30p_balance) / nullif(sum(f.balance), 0) * 100 AS r30,
                sum(f.npl_90p_balance) / nullif(sum(f.balance), 0) * 100 AS rn
         FROM {D} f
         WHERE f.as_of_date IN (SELECT d FROM cur UNION ALL SELECT previous_date FROM cur
                                UNION ALL SELECT previous_month_date FROM cur) AND {cust}
         GROUP BY f.as_of_date),
l AS (SELECT 1 AS line, 'vs Last Day' AS cmp, previous_date AS dd FROM cur
      UNION ALL SELECT 2, 'vs Last Month', previous_month_date FROM cur)
SELECT l.line, concat(l.cmp, ' (', coalesce(date_format(l.dd, 'yyyy-MM-dd'), 'N/A'), ') pp')
           AS compare,
       round(c.r1 - p.r1, 2) AS dpd_1p, round(c.r10 - p.r10, 2) AS dpd_10p,
       round(c.r30 - p.r30, 2) AS dpd_30p, round(c.rn - p.rn, 2) AS npl
FROM l CROSS JOIN cur LEFT JOIN rt c ON c.as_of_date = cur.d LEFT JOIN rt p ON p.as_of_date = l.dd
ORDER BY l.line""",
        "card_customer_types": """
SELECT explode(array('consumer', 'corporate', 'all')) AS customer_type""",
    }
    for donut, rows in mix_rows.items():
        pairs = ", ".join(f"'{label}', m.{col}" for label, col in rows)
        out[f"card_mix_{donut}"] = month + f"""
SELECT stack({len(rows)}, {pairs}) AS (slice, amount)
FROM m CROSS JOIN cur WHERE m.year_month = cur.cm"""
    piv = ", ".join(f"max(CASE WHEN line = {ln} THEN {col} END) AS {col}_{ln}"
                    for col, _, _ in KPI_CODES for ln in (1, 2, 3, 4))
    value_col = {"sales_volume": "sales_volume_k_usd", "balance": "balance_k_usd"}

    def note(col: str, ln: int, what: str) -> str:
        return (f"coalesce(concat(format_number(c.{col}_{ln}, 0), '% of {what}'), "
                f"'{what}: N/A')")

    out["card_kpi_cards"] = kpi_cards_sql(
        {"k": out["card_kpi"], "c": f"SELECT {piv} FROM ({out['card_kpi_compare']})"},
        [{"kpi": lit(col), "title": lit(title), "value": f"k.{value_col.get(col, col)}",
          "value_fmt": lit(",.0f"), "cmp1": f"c.{col}_1", "cmp1_label": lit("MoM"),
          "cmp2": f"c.{col}_2", "cmp2_label": lit("vs YE"), "cmp_unit": lit("%"),
          "cmp_fmt": lit(",.1f"), "note1": note(col, 3, "Month Target"),
          "note2": note(col, 4, "Year Target")}
         for (col, _, _), title in zip(KPI_CODES, ["New Acquisition", "Customer Base",
                                                   "Using Customer", "Sales Volume (K USD)",
                                                   "Balance (K USD)"], strict=True)])
    return out


def card_dashboard(catalog: str) -> dict:
    d = Dash("card_business", "Card Business Dashboard")
    d.param("report_date", "Report date", "DATE", "now/d")
    d.param("customer_type", "Customer type", "STRING", "consumer")
    sql = card_sql(catalog)
    rc = ["report_date", "customer_type"]
    titles = {"card_kpi": "Overview KPI",
              "card_kpi_compare": "Overview KPI vs last month / year-end / target",
              "card_new_acq": "New Acquisition by channel", "card_in_out": "In & Out",
              "card_base_using": "Customer Base & Using Mem", "card_balance": "Balance (K USD)",
              "card_volume": "Sales Volume (K USD)", "card_ticket": "Spend per Member (USD)",
              "card_mix_sector": "Spend by sector", "card_mix_location": "Spend by location",
              "card_mix_channel": "Spend by channel",
              "card_risk_compare": "Delinquency vs last day / month (pp)",
              "card_kpi_cards": "KPI cards (Custom Viz)"}
    ds = {n: d.dataset(n, t, sql[n], rc) for n, t in titles.items()}
    ds_ct = d.dataset("card_customer_types", "Customer type options",
                      sql["card_customer_types"], [])

    c = CARD_COLORS
    # Bố cục theo card_dashboard_final.html: header + bộ lọc, Overview (5 thẻ KPI + dòng so
    # sánh ngay dưới), New Issuance vs. Termination, Sales Performance (2x2), Spending
    # Distribution (3 donut, MTD), Delinquency (4 chỉ số + thay đổi pp). Lưới = 6 cột.
    d.text("# CARD BUSINESS DASHBOARD\n**Performance & Growth**", 0, 0, 4, 2)
    d.text("### 🟧 Overview", 0, 2)
    # Thẻ KPI (Custom Viz): giá trị + MoM / vs YE xanh-đỏ + 2 dòng % chỉ tiêu (như bản gốc)
    for i, kpi in enumerate(["new_acquisition", "customer_base", "using_customer",
                             "sales_volume", "balance"]):
        d.custom_viz(ds["card_kpi_cards"], "", vega.kpi_card(kpi, c["kpi"][i], n_cmp=2),
                     i, 3, 1, 4)
    d.text("**Report scope**\n\nNew / Using / Sales = MTD; Base / Balance = tại ngày báo cáo. "
           "Using = số thẻ có giao dịch (card-level).", 5, 3, 1, 4)

    y = 7   # Delinquency ngay dưới Overview
    d.text("### 🟧 Delinquency", 0, y)
    for i, (col, title) in enumerate([("dpd_1p", "DPD 1+ (%)"), ("dpd_10p", "DPD 10+ (%)"),
                                      ("dpd_30p", "DPD 30+ (%)"), ("npl", "NPL (%)")]):
        d.counter(ds["card_kpi"], col, title, 1 + i, y + 1, color=c["risk"], fmt="pct",
                  compare=(f"{col}_prev", "vs Last Day"))

    y = 11
    d.text("### 🟧 New Issuance vs. Termination", 0, y)
    d.custom_viz(ds["card_new_acq"], "New Acquisition", vega.stacked_with_total(
        "month", "channel", "cards", {"Smart Credit": c["smart_credit"],
                                      "Branches": c["branches"]}), 0, y + 1, 3, 7)
    d.custom_viz(ds["card_in_out"], "In & Out", vega.bars(
        "month", "cards", "series", {"New Issuance": c["new_issuance"],
                                     "Termination": c["termination"]}, labels=True),
        3, y + 1, 3, 7)

    y = 19
    d.text("### 🟧 Sales Performance", 0, y)
    d.custom_viz(ds["card_base_using"], "Customer Base & Using Mem", vega.bars(
        "month", "members", "series", {"Customer Base": c["base"], "Using Mem": c["using"]}),
        0, y + 1, 3, 7)
    d.custom_viz(ds["card_balance"], "Balance (K USD)", vega.bars(
        "month", "k_usd", "series", {"Balance": c["balance"], "Revolving": c["revolving"]}),
        3, y + 1, 3, 7)
    d.custom_viz(ds["card_volume"], "Sales Volume (K USD)", vega.bars(
        "month", "k_usd", color=c["volume"], labels=True), 0, y + 8, 3, 7)
    d.custom_viz(ds["card_ticket"], "Spend per Member (USD)", vega.line(
        "month", "spend_per_member", c["ticket"]), 3, y + 8, 3, 7)

    y = 35
    d.text("### 🟧 Spending Distribution (MTD)", 0, y)
    for i, (donut, title) in enumerate([("sector", "By Sectors"), ("location", "By Locations"),
                                        ("channel", "By Channels")]):
        d.custom_viz(ds[f"card_mix_{donut}"], title, vega.donut("slice", "amount", c[donut]),
                     2 * i, y + 1, 2, 6)

    d.param_filter("report_date", "Report date", "filter-date-picker", 4, 0)
    d.param_filter("customer_type", "Customer type", "filter-single-select", 5, 0,
                   options_ds=ds_ct, options_col="customer_type")
    return d.to_json()
