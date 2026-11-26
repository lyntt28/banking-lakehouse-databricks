"""3 dashboard Databricks AI/BI (Lakeview) dựng lại từ dashboard Flask + Chart.js gốc.

Bố cục (thứ tự, vị trí, tỉ lệ rộng), loại biểu đồ và mã màu chép từ code gốc
(reference/dashboards/*/dashboard.html, *.js, styles.css). Lưới Lakeview rộng 6 cột:
    - hàng 2 biểu đồ 1fr 1fr      -> 3 + 3
    - hàng 1fr 1.5fr (Overdue)    -> 2 + 4 (gần nhất trên lưới 6 cột)
    - hàng 1fr 2fr (Deposit)      -> 2 + 4
Những gì Lakeview không vẽ được y hệt (ghi rõ trong docs/gold_dashboards.md):
    gradient màu cột, đường "Total" chồng lên cột chồng, đồng hồ (gauge) -> counter %,
    2 chồng cột / tháng (năm nay vs năm trước) -> cột nhóm.

Mỗi dataset nhận tham số :report_date (DATE, mặc định hôm nay -> lấy ngày có dữ liệu gần
nhất <= ngày chọn), Card thêm :customer_type, Overdue thêm :branch, Deposit thêm :region.
`build_all(catalog)` trả về {tên file: dict JSON}; `write_all()` ghi ra thư mục dashboards/.
"""

from __future__ import annotations

import json
from pathlib import Path

# ============================================================ khung JSON
_FIELD_ID = 0


class Dash:
    def __init__(self, name: str, title: str):
        self.name, self.title = name, title
        self.datasets: list[dict] = []
        self.layout: list[dict] = []
        self.params: dict[str, dict] = {}   # keyword -> định nghĩa tham số
        self._n = 0

    # ---------------- dataset
    def dataset(self, name: str, title: str, sql: str, params: list[str]) -> str:
        self.datasets.append({
            "name": name, "displayName": title,
            "queryLines": [line + "\n" for line in sql.strip().splitlines()],
            "parameters": [self.params[p] for p in params],
        })
        return name

    def param(self, keyword: str, title: str, dtype: str, default: str) -> None:
        self.params[keyword] = {
            "displayName": title, "keyword": keyword, "dataType": dtype,
            "defaultSelection": {"values": {"dataType": dtype, "values": [{"value": default}]}},
        }

    # ---------------- widget
    def _wid(self, prefix: str) -> str:
        self._n += 1
        return f"{prefix}_{self._n:02d}"

    def place(self, widget: dict, x: int, y: int, w: int, h: int) -> None:
        assert 0 <= x and x + w <= 6, (widget.get("name"), x, w)
        self.layout.append({"widget": widget, "position": {"x": x, "y": y, "width": w,
                                                           "height": h}})

    def text(self, md: str, x: int, y: int, w: int = 6, h: int = 1) -> None:
        self.place({"name": self._wid("text"), "multilineTextboxSpec": {"lines": [md]}},
                   x, y, w, h)

    def _query(self, ds: str, fields: list[tuple[str, str]], agg: bool) -> list[dict]:
        return [{"name": "main_query", "query": {
            "datasetName": ds,
            "fields": [{"name": n, "expression": e} for n, e in fields],
            "disaggregated": not agg}}]

    def counter(self, ds: str, col: str, title: str, x: int, y: int, w: int = 1, h: int = 3,
                color: str | None = None, fmt: str | None = None) -> None:
        enc = {"fieldName": col, "displayName": title}
        if fmt:
            enc["format"] = _number_format(fmt)
        if color:
            enc["style"] = {"color": color}
        self.place({"name": self._wid("counter"),
                    "queries": self._query(ds, [(col, f"`{col}`")], agg=False),
                    "spec": {"version": 2, "widgetType": "counter",
                             "encodings": {"value": enc},
                             "frame": {"showTitle": True, "title": title}}}, x, y, w, h)

    def chart(self, kind: str, ds: str, title: str, x: int, y: int, w: int, h: int, *,
              cat: str, val: str, series: str | None = None, colors: dict | None = None,
              single_color: str | None = None, layout: str | None = None,
              horizontal: bool = False, cat_title: str = "", val_title: str = "",
              sort_desc: bool = False, labels: bool = True) -> None:
        """kind: bar | line | pie. Dữ liệu dạng dài: (cat, [series], val)."""
        agg = f"sum({val})"
        fields = [(cat, f"`{cat}`"), (agg, f"SUM(`{val}`)")]
        if series:
            fields.insert(1, (series, f"`{series}`"))
        if kind == "pie":
            enc = {"angle": {"fieldName": agg, "scale": {"type": "quantitative"},
                             "displayName": val_title or val},
                   "color": {"fieldName": cat, "scale": _cat_scale(colors),
                             "displayName": cat_title or cat}}
        else:
            c_enc = {"fieldName": cat, "scale": {"type": "categorical"},
                     "displayName": cat_title or cat}
            if sort_desc:
                c_enc["scale"]["sort"] = {"by": "y-reversed" if not horizontal else "x-reversed"}
            v_enc = {"fieldName": agg, "scale": {"type": "quantitative"},
                     "displayName": val_title or val}
            enc = {"x": v_enc, "y": c_enc} if horizontal else {"x": c_enc, "y": v_enc}
            if series:
                enc["color"] = {"fieldName": series, "scale": _cat_scale(colors),
                                "displayName": "Series"}
        if labels:
            enc["label"] = {"show": True}
        spec = {"version": 3, "widgetType": kind, "encodings": enc,
                "frame": {"showTitle": True, "title": title}}
        if layout:
            spec["mark"] = {"layout": layout}
        if single_color:
            spec.setdefault("mark", {})["colors"] = [single_color]
        self.place({"name": self._wid(kind), "queries": self._query(ds, fields, agg=True),
                    "spec": spec}, x, y, w, h)

    def combo(self, ds: str, title: str, x: int, y: int, w: int, h: int, *, cat: str,
              bar: str, line: str, bar_title: str, line_title: str, bar_color: str,
              line_color: str) -> None:
        """Cột (trục trái) + đường (trục phải), như Chart.js bar + type:'line', yAxisID y1."""
        fb, fl = f"sum({bar})", f"sum({line})"
        fields = [(cat, f"`{cat}`"), (fb, f"SUM(`{bar}`)"), (fl, f"SUM(`{line}`)")]
        spec = {"version": 1, "widgetType": "combo", "encodings": {
            "x": {"fieldName": cat, "scale": {"type": "categorical"}, "displayName": cat},
            "y": {"scale": {"type": "quantitative"},
                  "primary": {"fields": [{"fieldName": fb, "displayName": bar_title}]},
                  "secondary": {"fields": [{"fieldName": fl, "displayName": line_title}]}},
            "color": {"scale": {"type": "categorical", "mappings": [
                {"value": bar_title, "color": bar_color},
                {"value": line_title, "color": line_color}]}},
            "label": {"show": True}},
            "frame": {"showTitle": True, "title": title}}
        self.place({"name": self._wid("combo"), "queries": self._query(ds, fields, agg=True),
                    "spec": spec}, x, y, w, h)

    def table(self, ds: str, title: str, cols: list[tuple[str, str, str]], x: int, y: int,
              w: int, h: int) -> None:
        """cols: (cột, tiêu đề, định dạng: text | int | num | pct).

        Spec table version 2 (cột chỉ cần fieldName + displayName): spec v1 kiểu cũ bị
        Databricks báo "Invalid widget definition is imported". Định dạng số làm trong SQL."""
        enc = [{"fieldName": c, "displayName": t or c} for c, t, _ in cols]
        self.place({"name": self._wid("table"),
                    "queries": self._query(ds, [(c, f"`{c}`") for c, _, _ in cols], agg=False),
                    "spec": {"version": 2, "widgetType": "table",
                             "encodings": {"columns": enc},
                             "frame": {"showTitle": True, "title": title}}}, x, y, w, h)

    def param_filter(self, keyword: str, title: str, kind: str, x: int, y: int, w: int = 1,
                     h: int = 2, options_ds: str | None = None,
                     options_col: str | None = None) -> None:
        """Bộ lọc gắn vào tham số `keyword` của mọi dataset có tham số đó."""
        queries, fields = [], []
        if options_ds:
            queries.append({"name": "options", "query": {
                "datasetName": options_ds,
                "fields": [{"name": options_col, "expression": f"`{options_col}`"}],
                "disaggregated": False}})
            fields.append({"fieldName": options_col, "displayName": title,
                           "queryName": "options"})
        for d in self.datasets:
            if any(p["keyword"] == keyword for p in d["parameters"]):
                qn = f"param_{d['name']}_{keyword}"
                queries.append({"name": qn, "query": {
                    "datasetName": d["name"],
                    "parameters": [{"name": keyword, "keyword": keyword}],
                    "disaggregated": False}})
                fields.append({"parameterName": keyword, "queryName": qn})
        self.place({"name": self._wid("filter"), "queries": queries,
                    "spec": {"version": 2, "widgetType": kind,
                             "encodings": {"fields": fields},
                             "frame": {"showTitle": True, "title": title}}}, x, y, w, h)

    def to_json(self) -> dict:
        return {"datasets": self.datasets,
                "pages": [{"name": self.name, "displayName": self.title,
                           "layout": self.layout, "pageType": "PAGE_TYPE_CANVAS"}],
                "uiSettings": {"theme": {"widgetHeaderAlignment": "CENTER"}}}


def _cat_scale(colors: dict | None) -> dict:
    s = {"type": "categorical"}
    if colors:
        s["mappings"] = [{"value": k, "color": v} for k, v in colors.items()]
    return s


def _number_format(fmt: str) -> dict:
    return {"pct": {"type": "number-plain", "decimalPlaces": {"type": "exact", "places": 2},
                    "suffix": "%"},
            "int": {"type": "number-plain", "decimalPlaces": {"type": "exact", "places": 0}},
            "num": {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 3}},
            "compact": {"type": "number-plain", "abbreviation": "compact",
                        "decimalPlaces": {"type": "max", "places": 1}}}[fmt]


# ============================================================ SQL dùng chung
def _q(catalog: str, schema: str) -> str:
    """catalog rỗng -> 'schema' (bundle đặt catalog qua dataset_catalog theo môi trường)."""
    return f"{catalog}.{schema}" if catalog else schema



def _report_day(table: str, col: str = "as_of_date") -> str:
    """Ngày có dữ liệu gần nhất <= :report_date."""
    return f"(SELECT max({col}) FROM {table} WHERE {col} <= :report_date)"


USD_RATE = ("(SELECT max_by(rate_to_vnd, rate_date) FROM {silver}.ref_fx_rates "
            "WHERE currencycd = 'USD' AND rate_date <= :report_date)")


# ============================================================ 1. CARD BUSINESS
CARD_COLORS = {   # reference/dashboards/card/dashboard.js
    "kpi": ["#166534", "#604A7B", "#943126", "#2E86C1", "#138D75"],
    "smart_credit": "#20538D", "branches": "#8DB4E2", "termination": "#A5A5A5",
    "base": "#7B8AD7", "using": "#D586BD", "balance": "#20558A", "revolving": "#E2B13C",
    "volume": "#3B82F6", "ticket": "#5B9D4A",
    "sector": {"Purchasing": "#5B8FC4", "Installment": "#EDD07A", "Cash Advance": "#FFB366",
               "Flexi": "#E5CA6A"},
    "location": {"Domestic": "#9BA8E8", "X-Border": "#E4A8D0"},
    "channel": {"POS (Offline)": "#5B8FC4", "Online": "#8BC47A"},
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
mon AS (SELECT date_format(as_of_date, 'yyyy-MM') AS ym, max(as_of_date) AS last_d,
               sum(new_m) new_m, sum(closed_m) closed_m, sum(spend) spend
        FROM day GROUP BY 1),
m7 AS (SELECT m.ym, m.new_m, m.closed_m, m.spend, x.eop, x.usg, x.bal, x.rev
       FROM mon m JOIN day x ON x.as_of_date = m.last_d CROSS JOIN p
       WHERE m.last_d <= p.d AND m.ym > date_format(add_months(p.d, -7), 'yyyy-MM'))
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
SELECT k.ord, k.kpi,
  (k.cur - k.lm) / nullif(k.lm, 0) * 100 AS vs_last_month_pct,
  (k.cur - k.ly) / nullif(k.ly, 0) * 100 AS vs_last_year_pct,
  k.cur / nullif(tm.target, 0) * 100 AS month_target_pct,
  k.ytd_val / nullif(ty.target, 0) * 100 AS year_target_pct
FROM k
LEFT JOIN t tm ON tm.metric_code = k.code AND tm.period_type = 'MONTH'
LEFT JOIN t ty ON ty.metric_code = k.code AND ty.period_type = 'YEAR'
ORDER BY k.ord
""", ["report_date", "customer_type"])

    ds_new = d.dataset("card_new_acq", "New Acquisition by channel", f"""
WITH p AS (SELECT {_report_day(f'{G}.card_mart_kpi_daily')} AS d)
SELECT date_format(k.as_of_date, 'yyyy-MM') AS month,
       CASE k.acquisition_channel WHEN 'SMART_CREDIT' THEN 'Smart Credit'
            ELSE 'Branches' END AS channel,
       sum(k.new_members) AS cards
FROM {G}.card_mart_kpi_daily k CROSS JOIN p
WHERE k.as_of_date <= p.d
  AND date_format(k.as_of_date, 'yyyy-MM') > date_format(add_months(p.d, -7), 'yyyy-MM')
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
    ds_ct = d.dataset("card_customer_types", "Customer type options", """
SELECT explode(array('consumer', 'corporate', 'all')) AS customer_type
""", [])

    c = CARD_COLORS
    # ---- header + bộ lọc (header gốc: tiêu đề trái, ngày + loại KH phải)
    d.text("# CARD BUSINESS DASHBOARD | Performance & Growth", 0, 0, 4, 2)
    # ---- Overview: 5 thẻ KPI (repeat(5, 1fr))
    d.text("## Overview", 0, 2)
    for i, (col, title, fmt) in enumerate([
            ("new_acquisition", "New Acquisition", "int"),
            ("customer_base", "Customer Base", "int"),
            ("using_customer", "Using Customer", "int"),
            ("sales_volume_k_usd", "Sales Volume (K USD)", "compact"),
            ("balance_k_usd", "Balance (K USD)", "compact")]):
        d.counter(ds_kpi, col, title, i, 3, color=c["kpi"][i], fmt=fmt)
    d.table(ds_cmp, "vs Last Month / Last Year / Target", [
        ("kpi", "KPI", "text"), ("vs_last_month_pct", "vs Last Month (%)", "pct"),
        ("vs_last_year_pct", "vs Last Year (%)", "pct"),
        ("month_target_pct", "% of Month Target", "pct"),
        ("year_target_pct", "% of Year Target", "pct")], 0, 6, 6, 4)
    # ---- New Issuance vs. Termination (chart-grid-2)
    y = 10
    d.text("## New Issuance vs. Termination", 0, y)
    d.chart("bar", ds_new, "New Acquisition", 0, y + 1, 3, 6, cat="month", series="channel",
            val="cards", layout="stack",
            colors={"Smart Credit": c["smart_credit"], "Branches": c["branches"]})
    d.chart("bar", ds_inout, "In & Out", 3, y + 1, 3, 6, cat="month", series="series",
            val="cards", layout="group",
            colors={"New Issuance": c["smart_credit"], "Termination": c["termination"]})
    # ---- Sales Performance (2 hàng chart-grid-2)
    y = 17
    d.text("## Sales Performance", 0, y)
    d.chart("bar", ds_base, "Customer Base & Using Mem", 0, y + 1, 3, 6, cat="month",
            series="series", val="members", layout="group",
            colors={"Customer Base": c["base"], "Using Mem": c["using"]})
    d.chart("bar", ds_bal, "Balance (K USD)", 3, y + 1, 3, 6, cat="month", series="series",
            val="k_usd", layout="group",
            colors={"Balance": c["balance"], "Revolving": c["revolving"]})
    d.chart("bar", ds_vol, "Sales Volume (K USD)", 0, y + 7, 3, 6, cat="month", val="k_usd",
            single_color=c["volume"])
    d.chart("line", ds_spend, "Spend per cus (USD)", 3, y + 7, 3, 6, cat="month",
            val="ticket_size", single_color=c["ticket"])
    # ---- Spending Distribution (donut-grid repeat(3, 1fr))
    y = 31
    d.text("## Spending Distribution", 0, y)
    for i, (donut, title) in enumerate([("sector", "By Sectors"), ("location", "By Locations"),
                                        ("channel", "By Channels")]):
        d.chart("pie", mix[donut], title, 2 * i, y + 1, 2, 6, cat="slice", val="amount",
                colors=c[donut])
    # ---- Delinquency (delinquency-grid repeat(4, 1fr)) - căn giữa trên lưới 6 cột
    y = 38
    d.text("## Delinquency", 0, y)
    for i, (col, title) in enumerate([("dpd_1p", "DPD 1+ (%)"), ("dpd_10p", "DPD 10+ (%)"),
                                      ("dpd_30p", "DPD 30+ (%)"), ("npl", "NPL (%)")]):
        d.counter(ds_kpi, col, title, 1 + i, y + 1, fmt="pct")
    # ---- bộ lọc (đặt sau khi có đủ dataset)
    d.param_filter("report_date", "Report date", "filter-date-picker", 4, 0)
    d.param_filter("customer_type", "Customer type", "filter-single-select", 5, 0,
                   options_ds=ds_ct, options_col="customer_type")
    return d.to_json()


# ============================================================ 2. OVERDUE (RAMC)
# Nguồn: gold.card_mart_overdue_daily (file credit_card_overdue_YYYYMMDD.txt), logic theo
# reference/dashboards/overdue/api_server.py: tỷ lệ = sum(ovd_bal) / sum(port_bal),
# lọc chi nhánh theo mng_br, không lọc loại sản phẩm (All).
OVD_COLORS = {   # reference/dashboards/overdue/dashboardv200.js
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
SELECT pk.ord, pk.cmp,
  round((cur.ovd / nullif(cur.port, 0) - c.ovd / nullif(c.port, 0)) * 100, 2) AS ratio_pp,
  round((cur.ovd - c.ovd) / p.usd, 0) AS overdue_usd,
  round((cur.port - c.port) / p.usd, 0) AS ending_usd
FROM pick pk CROSS JOIN cur CROSS JOIN p LEFT JOIN day c ON c.as_of_date = pk.dd
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
    d.table(ds_cmp, "Comparison", [
        ("cmp", "Compare", "text"), ("ratio_pp", "Overdue Ratio (pp)", "pct"),
        ("overdue_usd", "Overdue Balance (USD)", "int"),
        ("ending_usd", "Ending Balance (USD)", "int")], 0, 5, 6, 3)
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


# ============================================================ 3. RETAIL DEPOSIT (RCD)
DEP_COLORS = {   # reference/dashboards/deposit/dashboardv2.js, api_server.py
    "gauge": {"all": "#22AFFF", "casa": "#10B981", "tda": "#F47B20"},
    "daily": "#223A5E",
    "mom": {"1-CASA last year": "#FFE082", "1-CASA this year": "#F4A300",
            "2-TD last year": "#80CBC4", "2-TD this year": "#009688",
            "3-ISA last year": "#90A4AE", "3-ISA this year": "#223A5E"},
    "region": {"South 1": "#223A5E", "South 2": "#1A237E", "North": "#FFE082",
               "Central": "#F47B20"},
    "term_bar": "#009688", "term_line": "#F4A300",
    "casa": {"Non-CASA": "#223A5E", "Active": "#009688", "Dormant": "#b0b0b0"},
    "edusave": "#223A5E",
}


def deposit_dashboard(catalog: str) -> dict:
    G = _q(catalog, "gold")
    d = Dash("retail_deposit", "Retail Deposit Performance")
    d.param("report_date", "Report date", "DATE", "now/d")
    d.param("region", "Region", "STRING", "all")
    base = f"""
WITH p AS (SELECT {_report_day(f'{G}.dep_mart_trend')} AS d),
f AS (SELECT * FROM {G}.dep_mart_trend WHERE :region = 'all' OR region = :region),
day AS (SELECT as_of_date, sum(balance_vnd) bal FROM f GROUP BY as_of_date)
"""
    ds_tick = d.dataset("dep_tickers", "Tickers", base + f"""
, cust AS (
  SELECT c.as_of_date, sum(c.retail_customers) AS retail
  FROM {G}.dep_mart_casa_customer c LEFT JOIN {G}.dim_branch b ON b.branchnbr = c.branchnbr
  WHERE :region = 'all' OR b.region = :region GROUP BY 1)
SELECT x.bal AS ending_balance,
  (SELECT avg(bal) FROM day, p WHERE date_trunc('MM', as_of_date) = date_trunc('MM', p.d)
     AND as_of_date <= p.d) AS monthly_avg_balance,
  (SELECT retail FROM cust, p WHERE cust.as_of_date = (SELECT max(as_of_date) FROM cust
     WHERE as_of_date <= p.d)) AS retail_customers
FROM day x, p WHERE x.as_of_date = p.d
""", ["report_date", "region"])
    ds_cmp = d.dataset("dep_compare", "Tickers vs last day / month / year", base + """
, pick AS (
  SELECT 1 AS ord, 'vs Last Day' AS cmp,
         (SELECT max(as_of_date) FROM day, p WHERE as_of_date < p.d) AS dd FROM p
  UNION ALL SELECT 2, 'vs Last Month', (SELECT max(as_of_date) FROM day, p
         WHERE as_of_date <= last_day(add_months(p.d, -1))) FROM p
  UNION ALL SELECT 3, 'vs Last Year', (SELECT max(as_of_date) FROM day, p
         WHERE as_of_date <= make_date(year(p.d) - 1, 12, 31)) FROM p),
cur AS (SELECT x.bal FROM day x, p WHERE x.as_of_date = p.d)
SELECT pk.ord, pk.cmp, cur.bal - c.bal AS ending_balance_diff,
       (cur.bal - c.bal) / nullif(c.bal, 0) * 100 AS ending_balance_pct
FROM pick pk CROSS JOIN cur LEFT JOIN day c ON c.as_of_date = pk.dd ORDER BY pk.ord
""", ["report_date", "region"])
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
  FROM {_q(catalog, "silver")}.ref_targets tg CROSS JOIN p
  LEFT JOIN {G}.dim_branch b ON b.branchnbr = tg.branchnbr
  WHERE tg.period_type = 'YEAR' AND tg.period = CAST(year(p.d) AS STRING)
    AND (:region = 'all' OR b.region = :region))
SELECT cur.all_bal / nullif(t.all_t, 0) * 100 AS all_pct,
       cur.casa_bal / nullif(t.casa_t, 0) * 100 AS casa_pct,
       cur.tda_bal / nullif(t.tda_t, 0) * 100 AS tda_pct
FROM cur CROSS JOIN t
""", ["report_date", "region"])
    ds_daily = d.dataset("dep_daily30", "Ending Balance - Last 30 Days", base + """
SELECT date_format(as_of_date, 'dd/MM') AS day, bal AS balance FROM day, p
WHERE as_of_date BETWEEN date_sub(p.d, 29) AND p.d
""", ["report_date", "region"])
    ds_mom = d.dataset("dep_mom", "Monthly Average Balance (MoM)", base + """
, m AS (SELECT year(as_of_date) AS yr, month(as_of_date) AS mn, group_class,
               avg(bal) AS avg_bal
        FROM (SELECT as_of_date, group_class, sum(balance_vnd) bal FROM f
              GROUP BY as_of_date, group_class)
        GROUP BY 1, 2, 3)
SELECT date_format(make_date(2000, m.mn, 1), 'MMM') AS month,
       concat(m.group_class, CASE WHEN m.yr = year(p.d) THEN ' this year'
                                    ELSE ' last year' END) AS series,
       m.avg_bal
FROM m, p WHERE m.yr IN (year(p.d), year(p.d) - 1) AND make_date(m.yr, m.mn, 1) <= p.d
""", ["report_date", "region"])
    ds_reg = d.dataset("dep_region", "Regional Deposit Distribution", base + """
SELECT region, sum(balance_vnd) AS balance FROM f, p WHERE f.as_of_date = p.d GROUP BY 1
""", ["report_date", "region"])
    ds_term = d.dataset("dep_term", "Deposit by Term and IR (in Month)", f"""
WITH p AS (SELECT {_report_day(f'{G}.dep_mart_by_tenor')} AS d)
SELECT concat(t.tenor_months, 'M') AS tenor, t.tenor_months,
       sum(t.balance_vnd) AS balance,
       sum(t.weighted_rate * t.balance_vnd) / nullif(sum(t.balance_vnd), 0) * 100 AS avg_interest
FROM {G}.dep_mart_by_tenor t, p WHERE t.as_of_date = p.d GROUP BY 1, 2
ORDER BY t.tenor_months
""", ["report_date"])
    ds_casa = d.dataset("dep_casa_customer", "CASA Customer Distribution", f"""
WITH p AS (SELECT {_report_day(f'{G}.dep_mart_casa_customer')} AS d),
c AS (SELECT sum(c.retail_customers) retail, sum(c.casa_active_customers) active,
             sum(c.casa_dormant_customers) dormant
      FROM {G}.dep_mart_casa_customer c CROSS JOIN p
      LEFT JOIN {G}.dim_branch b ON b.branchnbr = c.branchnbr
      WHERE c.as_of_date = p.d AND (:region = 'all' OR b.region = :region))
SELECT 'Non-CASA' AS segment, retail - active - dormant AS customers FROM c
UNION ALL SELECT 'Active', active FROM c
UNION ALL SELECT 'Dormant', dormant FROM c
""", ["report_date", "region"])
    ds_ks = d.dataset("dep_edusave", "Edu-Save New open Account_ Accumulate by Month", f"""
WITH p AS (SELECT {_report_day(f'{G}.dep_mart_trend')} AS d)
SELECT n.open_month AS month, sum(n.new_accounts_ytd) AS cumulative_accounts
FROM {G}.dep_mart_new_accounts n CROSS JOIN p
LEFT JOIN {G}.dim_branch b ON b.branchnbr = n.branchnbr
WHERE n.product_type = 'EDUSAVE' AND n.open_month <= date_format(p.d, 'yyyy-MM')
  AND n.open_month > date_format(add_months(p.d, -12), 'yyyy-MM')
  AND (:region = 'all' OR b.region = :region)
GROUP BY 1
""", ["report_date", "region"])
    ds_prod = d.dataset("dep_by_product", "Balance by Product", base + """
, g AS (SELECT as_of_date, account_group, sum(balance_vnd) bal FROM f GROUP BY 1, 2),
pk AS (
  SELECT (SELECT max(as_of_date) FROM day, p WHERE as_of_date < p.d) AS d1,
         (SELECT max(as_of_date) FROM day, p
          WHERE as_of_date <= last_day(add_months(p.d, -1))) AS m1,
         (SELECT max(as_of_date) FROM day, p
          WHERE as_of_date <= make_date(year(p.d) - 1, 12, 31)) AS y1),
cur AS (SELECT g.* FROM g, p WHERE g.as_of_date = p.d)
SELECT cur.account_group AS product, cur.bal AS ending_balance,
       CAST(cur.bal AS DOUBLE) / sum(CAST(cur.bal AS DOUBLE)) OVER () * 100 AS portion,
       cur.bal - d1.bal AS diff_last_day, cur.bal - m1.bal AS diff_last_month,
       cur.bal - y1.bal AS diff_last_year
FROM cur CROSS JOIN pk
LEFT JOIN g d1 ON d1.account_group = cur.account_group AND d1.as_of_date = pk.d1
LEFT JOIN g m1 ON m1.account_group = cur.account_group AND m1.as_of_date = pk.m1
LEFT JOIN g y1 ON y1.account_group = cur.account_group AND y1.as_of_date = pk.y1
ORDER BY ending_balance DESC
""", ["report_date", "region"])
    ds_rg = d.dataset("dep_region_options", "Region options", f"""
SELECT 'all' AS region UNION ALL SELECT DISTINCT region FROM {G}.dim_branch
""", [])

    c = DEP_COLORS
    d.text("# Retail Deposit Performance", 0, 0, 4, 2)
    # ticker-row: 3 ô
    for i, (col, title, fmt) in enumerate([
            ("ending_balance", "Ending Balance", "compact"),
            ("monthly_avg_balance", "Monthly Avg Balance", "compact"),
            ("retail_customers", "Retail Customers", "int")]):
        d.counter(ds_tick, col, title, 2 * i, 2, w=2, fmt=fmt)
    d.table(ds_cmp, "Ending Balance vs Last Day / Month / Year", [
        ("cmp", "Compare", "text"), ("ending_balance_diff", "+/- Balance", "num"),
        ("ending_balance_pct", "+/- %", "pct")], 0, 5, 6, 3)
    # gauge-row: 3 đồng hồ -> counter % (Lakeview không có gauge)
    for i, (col, title, key) in enumerate([("all_pct", "All Products Target (%)", "all"),
                                           ("casa_pct", "CASA Target (%)", "casa"),
                                           ("tda_pct", "TDA/ISA Target (%)", "tda")]):
        d.counter(ds_gauge, col, title, 2 * i, 8, w=2, color=c["gauge"][key], fmt="pct")
    # chart-section: 2 biểu đồ toàn chiều rộng
    d.chart("bar", ds_daily, "Ending Balance - Last 30 Days", 0, 11, 6, 7, cat="day",
            val="balance", single_color=c["daily"], labels=False)
    d.chart("bar", ds_mom, "Monthly Average Balance (MoM)", 0, 18, 6, 7, cat="month",
            series="series", val="avg_bal", layout="group", colors=c["mom"], labels=False)
    # triple-table-row 1fr 2fr -> 2 + 4
    d.chart("pie", ds_reg, "Regional Deposit Distribution", 0, 25, 2, 7, cat="region",
            val="balance", colors=c["region"])
    d.combo(ds_term, "Deposit by Term and IR (in Month)", 2, 25, 4, 7, cat="tenor",
            bar="balance", line="avg_interest", bar_title="Balance",
            line_title="Avg Interest", bar_color=c["term_bar"], line_color=c["term_line"])
    d.chart("pie", ds_casa, "CASA Customer Distribution", 0, 32, 2, 7, cat="segment",
            val="customers", colors=c["casa"])
    d.chart("bar", ds_ks, "Edu-Save New open Account_ Accumulate by Month", 2, 32, 4, 7,
            cat="month", val="cumulative_accounts", single_color=c["edusave"])
    # Balance by Product: bảng cuối trang
    d.table(ds_prod, "Balance by Product", [
        ("product", "Balance by Product", "text"), ("ending_balance", "Ending Balance", "num"),
        ("portion", "Portion", "pct"), ("diff_last_day", "+/- Last Day", "num"),
        ("diff_last_month", "+/- Last Month", "num"),
        ("diff_last_year", "+/- Last Year", "num")], 0, 39, 6, 6)
    d.param_filter("report_date", "Report date", "filter-date-picker", 4, 0)
    d.param_filter("region", "Region", "filter-single-select", 5, 0,
                   options_ds=ds_rg, options_col="region")
    return d.to_json()



# ============================================================ 4. OPERATIONS MONITOR (cho DE)
def ops_dashboard(catalog: str) -> dict:
    """Dashboard vận hành: batch nào lỗi, ngày nào, bước nào, file nào chưa về, DQ, đối soát,
    Gold đang phục vụ batch nào. Đọc các bảng ops.* do pipelines/ ghi."""
    OPS = _q(catalog, "ops")
    d = Dash("ops", "Lakehouse Operations Monitor")
    ds_kpi = d.dataset("ops_kpi", "Current state", f"""
WITH pub AS (SELECT max(as_of_date) AS d FROM {OPS}.published_batches),
last_run AS (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY as_of_date, layer
                                        ORDER BY started_at DESC, status DESC) AS rn
                             FROM {OPS}.pipeline_runs) WHERE rn = 1)
SELECT pub.d AS serving_batch,
       datediff(current_date(), pub.d) AS serving_age_days,
       (SELECT count(*) FROM last_run WHERE status = 'FAILED') AS open_failures,
       (SELECT round(avg(CASE WHEN status = 'SUCCESS' THEN 100.0 ELSE 0 END), 1)
        FROM {OPS}.pipeline_runs WHERE status <> 'RUNNING'
          AND started_at >= current_timestamp() - INTERVAL 30 DAYS) AS success_rate_30d
FROM pub
""", [])
    ds_status = d.dataset("ops_status", "Batch status (last 14 days)", f"""
SELECT as_of_date, layer, status, run_id, started_at, finished_at, message
FROM (SELECT *, row_number() OVER (PARTITION BY as_of_date, layer
                                   ORDER BY started_at DESC, status DESC) AS rn
      FROM {OPS}.pipeline_runs WHERE as_of_date >= date_sub(current_date(), 14))
WHERE rn = 1
ORDER BY as_of_date DESC, layer
""", [])
    ds_steps = d.dataset("ops_steps", "Failed steps", f"""
SELECT logged_at, as_of_date, layer, step, rows_in, rows_out, duration_s, error, run_id
FROM {OPS}.task_runs WHERE status = 'FAILED' ORDER BY logged_at DESC LIMIT 50
""", [])
    ds_dur = d.dataset("ops_duration", "Processing time per day (minutes)", f"""
SELECT date_format(as_of_date, 'yyyy-MM-dd') AS day, layer, sum(duration_s) / 60 AS minutes
FROM {OPS}.task_runs WHERE status = 'SUCCESS' AND as_of_date >= date_sub(current_date(), 30)
GROUP BY 1, 2
""", [])
    ds_files = d.dataset("ops_files", "Source files not normal", f"""
SELECT as_of_date, table_name, status, file_count, rows, first_loaded_at
FROM {OPS}.file_manifest
WHERE status IN ('MISSING', 'MULTIPLE_FILES') AND as_of_date >= date_sub(current_date(), 14)
ORDER BY as_of_date DESC, table_name
""", [])
    ds_dq = d.dataset("ops_dq", "DQ failed rows per day", f"""
SELECT date_format(run_ts, 'yyyy-MM-dd') AS day, severity, sum(failed_rows) AS failed_rows
FROM {OPS}.dq_results WHERE run_ts >= current_timestamp() - INTERVAL 30 DAYS AND failed_rows > 0
GROUP BY 1, 2
""", [])
    ds_recon = d.dataset("ops_recon", "Reconciliation issues", f"""
SELECT as_of_date, 'card_kpi' AS check, metric_code AS item, status, diff
FROM {OPS}.recon_card_kpi WHERE status NOT IN ('MATCH', 'WITHIN_TOLERANCE')
UNION ALL
SELECT as_of_date, 'deposit', concat_ws('/', account_group, currencycd, branchnbr), status, diff
FROM {OPS}.recon_deposit WHERE status <> 'MATCH'
ORDER BY as_of_date DESC LIMIT 100
""", [])

    d.text("# LAKEHOUSE OPERATIONS MONITOR\nBatch status, failures, source files, DQ and "
           "reconciliation. Gold only serves batches that passed the reconciliation gate.",
           0, 0, 6, 2)
    d.counter(ds_kpi, "serving_batch", "Gold serving batch", 0, 2, w=2)
    d.counter(ds_kpi, "serving_age_days", "Serving age (days)", 2, 2)
    d.counter(ds_kpi, "open_failures", "Open failures", 3, 2, color="#D62728")
    d.counter(ds_kpi, "success_rate_30d", "Success rate 30d (%)", 4, 2, w=2)
    d.table(ds_status, "Batch status (last 14 days)", [
        ("as_of_date", "Data date", "text"), ("layer", "Layer", "text"),
        ("status", "Status", "text"), ("message", "Message", "text"),
        ("run_id", "Run ID", "text")], 0, 5, 6, 6)
    d.table(ds_steps, "Failed steps (latest 50)", [
        ("as_of_date", "Data date", "text"), ("layer", "Layer", "text"),
        ("step", "Step", "text"), ("error", "Error", "text"),
        ("duration_s", "Duration (s)", "num"), ("run_id", "Run ID", "text")], 0, 11, 6, 6)
    d.chart("bar", ds_dur, "Processing time per day (minutes)", 0, 17, 3, 6, cat="day",
            series="layer", val="minutes", layout="stack", val_title="minutes")
    d.chart("bar", ds_dq, "DQ failed rows per day", 3, 17, 3, 6, cat="day",
            series="severity", val="failed_rows", layout="stack", val_title="rows")
    d.table(ds_files, "Source files missing / sent more than once (14 days)", [
        ("as_of_date", "Data date", "text"), ("table_name", "Table", "text"),
        ("status", "Status", "text"), ("file_count", "Files", "int"),
        ("rows", "Rows", "int")], 0, 23, 3, 6)
    d.table(ds_recon, "Reconciliation issues", [
        ("as_of_date", "Data date", "text"), ("check", "Check", "text"),
        ("item", "Item", "text"), ("status", "Status", "text"), ("diff", "Diff", "num")],
        3, 23, 3, 6)
    return d.to_json()

# ============================================================ xuất file
DASHBOARDS = {
    "card_business.lvdash.json": card_dashboard,
    "loan_overdue.lvdash.json": overdue_dashboard,
    "retail_deposit.lvdash.json": deposit_dashboard,
    "ops_monitor.lvdash.json": ops_dashboard,
}


def build_all(catalog: str = "") -> dict[str, dict]:
    return {name: fn(catalog) for name, fn in DASHBOARDS.items()}


def write_all(out_dir: str | Path, catalog: str = "") -> list[Path]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, body in build_all(catalog).items():
        p = out / name
        p.write_text(json.dumps(body, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        paths.append(p)
    return paths


if __name__ == "__main__":
    import sys

    for path in write_all(sys.argv[1] if len(sys.argv) > 1 else "dashboards",
                          sys.argv[2] if len(sys.argv) > 2 else ""):
        print(path)
