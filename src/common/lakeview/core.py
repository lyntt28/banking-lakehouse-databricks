"""Phần dùng chung cho mọi dashboard Lakeview: khung JSON (Dash), format số, SQL chung.

Sửa file này ảnh hưởng TẤT CẢ dashboard; sửa 1 dashboard thì vào file riêng của nó."""

from __future__ import annotations

# ============================================================ khung JSON
_FIELD_ID = 0


class Dash:
    def __init__(self, name: str, title: str):
        self.name, self.title = name, title
        self.datasets: list[dict] = []
        self.pages: list[dict] = []        # [{name, displayName, layout}]; rỗng = 1 trang
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

    # ---------------- trang
    def page(self, name: str, title: str) -> None:
        """Bắt đầu trang mới; widget đặt sau lệnh này thuộc trang đó (như tab HTML gốc)."""
        self.layout = []
        self.pages.append({"name": name, "displayName": title, "layout": self.layout})

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
                color: str | None = None, fmt: str | None = None,
                compare: tuple[str, str] | None = None) -> None:
        """compare = (cột giá trị so sánh, nhãn): ô Comparison của counter (so với kỳ trước)."""
        enc = {"fieldName": col, "displayName": title}
        if fmt:
            enc["format"] = _number_format(fmt)
        if color:
            enc["style"] = {"color": color}
        encodings = {"value": enc}
        fields = [(col, f"`{col}`")]
        if compare:
            encodings["target"] = {"fieldName": compare[0], "displayName": compare[1]}
            fields.append((compare[0], f"`{compare[0]}`"))
        self.place({"name": self._wid("counter"),
                    "queries": self._query(ds, fields, agg=False),
                    "spec": {"version": 2, "widgetType": "counter",
                             "encodings": encodings,
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

        Spec bảng version 2 (định dạng Databricks hiện tại): mỗi cột chỉ cần fieldName +
        displayName. Spec version 1 cũ đòi nhiều trường bắt buộc -> "Invalid widget definition".
        Định dạng số làm ở SQL (round) thay vì trong spec.
        """
        enc = [{"fieldName": c, "displayName": t or c} for c, t, _ in cols]
        self.place({"name": self._wid("table"),
                    "queries": self._query(ds, [(c, f"`{c}`") for c, _, _ in cols], agg=False),
                    "spec": {"version": 2, "widgetType": "table",
                             "encodings": {"columns": enc},
                             "frame": {"showTitle": bool(title), "title": title}}}, x, y, w, h)

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
        on_page = {q["query"]["datasetName"] for item in self.layout
                   for q in item["widget"].get("queries", [])}
        for d in self.datasets:
            if self.pages and d["name"] not in on_page:
                continue          # nhiều trang: chỉ gắn dataset của trang hiện tại
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
        pages = self.pages or [{"name": self.name, "displayName": self.title,
                                "layout": self.layout}]
        return {"datasets": self.datasets,
                "pages": [{**pg, "pageType": "PAGE_TYPE_CANVAS"} for pg in pages],
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
    """Ngày có dữ liệu gần nhất <= :report_date VÀ <= batch Gold đã publish: publish đang chạy dở /
    bị rollback thì dashboard vẫn ở batch hợp lệ trước đó (cùng schema cha với `ops`)."""
    ops = table.rsplit(".", 2)[0] + ".ops" if table.count(".") >= 2 else "ops"
    return (f"(SELECT max({col}) FROM {table} WHERE {col} <= :report_date "
            f"AND {col} <= (SELECT max(as_of_date) FROM {ops}.published_batches))")


USD_RATE = ("(SELECT max_by(rate_to_vnd, rate_date) FROM {silver}.ref_fx_rates "
            "WHERE currencycd = 'USD' AND rate_date <= :report_date)")


def _arrow_pct(expr: str, unit: str = "%") -> str:
    """Số thay đổi -> chuỗi '▲ 1.2%' / '▼ 0.9%' (bảng v2 không tự định dạng số)."""
    return (f"CASE WHEN {expr} IS NULL THEN '-' ELSE concat(CASE WHEN {expr} >= 0 THEN '▲ ' "
            f"ELSE '▼ ' END, format_number(abs({expr}), 1), '{unit}') END")


# ============================================================ thẻ KPI (Custom Viz)
KPI_CARD_COLS = ["kpi", "title", "value", "value_fmt", "cmp1", "cmp1_label", "cmp2",
                 "cmp2_label", "cmp3", "cmp3_label", "cmp_unit", "cmp_fmt", "note1", "note2"]


def kpi_cards_sql(sources: dict[str, str], rows: list[dict[str, str]]) -> str:
    """Dataset dạng dài cho thẻ KPI (common.lakeview.vega.kpi_card): 1 dòng / KPI.

    sources: alias -> SQL của dataset ĐÃ CÓ (dùng lại nguyên logic, không tính lại);
    mỗi alias phải trả đúng 1 dòng. rows: mỗi KPI là dict cột (KPI_CARD_COLS) -> biểu thức
    SQL trên các alias; cột thiếu = NULL."""
    joins = " CROSS JOIN ".join(f"({sql.strip()}) {alias}" for alias, sql in sources.items())
    casts = {"value": "DOUBLE", "cmp1": "DOUBLE", "cmp2": "DOUBLE", "cmp3": "DOUBLE"}

    def cell(r: dict, c: str) -> str:
        return f"CAST({r.get(c, 'NULL')} AS {casts.get(c, 'STRING')})"

    body = ",\n  ".join(", ".join(cell(r, c) for c in KPI_CARD_COLS) for r in rows)
    return (f"SELECT stack({len(rows)},\n  {body}\n) AS ({', '.join(KPI_CARD_COLS)})\n"
            f"FROM {joins}")


def lit(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


# ============================================================ bảng màu CHUNG cho mọi dashboard
# Một bảng màu cho cả 3 dashboard (đã kiểm tra: dải sáng, độ bão hoà, tách màu cho người mù
# màu ΔE >= 8, phân biệt mắt thường ΔE >= 15). Gán theo THỨ TỰ cố định, màu đi theo thực thể
# (CASA luôn là S1...), không theo thứ hạng. Màu chữ / số KPI dùng màu chữ, không dùng màu series.
S1, S2, S3, S4, S5, S6 = "#2a9d8f", "#8b6fd6", "#e0659a", "#d4a017", "#4a86e8", "#e07a5f"
SERIES = [S1, S2, S3, S4, S5, S6]   # tông pastel đậm 1 bậc: bạc hà, tím lavender, hồng, vàng...
NEUTRAL = "#9e9e9e"          # "Khác", phân khúc phụ (vd Dormant)
TEXT = "#1a1a1a"             # số trên thẻ KPI
GOOD, BAD = "#2e7d32", "#c62828"   # trạng thái: tốt / xấu (luôn kèm ▲▼ + chữ, không chỉ màu)
# tông nhạt của S1..S3 cho "năm trước" (cùng thực thể, khác kỳ)
S1_LIGHT, S2_LIGHT, S3_LIGHT = "#a8d8d2", "#cdbff0", "#f5c1d6"
# thang tuần tự 1 màu (nhạt -> đậm) cho thứ bậc mức độ, vd nhóm nợ 1-10 ... 361+
SEVERITY = ["#fbd5e3", "#f3a3c4", "#e0659a", "#b83f74", "#7a2450"]
