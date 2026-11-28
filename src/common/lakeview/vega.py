"""Spec Vega-Lite cho Custom Visualization của AI/BI dashboard (Public Preview).

Tài liệu: https://docs.databricks.com/aws/en/dashboards/manage/visualizations/custom-visualizations
- Tạo trong UI: chọn dataset -> Custom Viz (Advanced) -> khai báo Fields (mỗi field có Name)
  -> dán spec. Spec tham chiếu cột bằng "field": "<Name>" / datum.<Name>.
- Theme dashboard (font, lưới, nền) được kế thừa; màu cố định ở đây lấy từ HTML gốc.
- Không hỗ trợ treemap; cross-filter mặc định chỉ cho spec 1 view (không layer).

Cách khai báo widget Custom Viz trong `.lvdash.json` CHƯA được Databricks công bố -> module
này chỉ sinh spec (và file docs/vega/*.json để dán vào UI), KHÔNG tự sinh thuộc tính JSON của
widget. Sau khi tạo 1 widget trong UI và export dashboard, có thể đọc đúng cấu trúc rồi mới
tự động hoá. Tên field = tên cột của dataset tương ứng (xem `FIELDS`).

Mọi spec không có "data": dữ liệu do dashboard truyền từ dataset."""

from __future__ import annotations

SCHEMA = "https://vega.github.io/schema/vega-lite/v5.json"
GREEN, RED, GREY = "#2e7d32", "#c62828", "#9e9e9e"   # = core.GOOD / BAD / NEUTRAL


def _base(**kw) -> dict:
    return {"$schema": SCHEMA, "width": "container", "height": "container",
            "autosize": {"type": "fit", "contains": "padding"}, **kw}


def _signed(expr: str, unit: str = "", digits: int = 1) -> str:
    """Biểu thức Vega: '▲ 1.2%' / '▼ 0.9%' / 'N/A' (NULL không hiện thành 0)."""
    return (f"(isValid({expr}) ? (({expr}) > 0 ? '▲ ' : ({expr}) < 0 ? '▼ ' : '▪ ') + "
            f"format(abs({expr}), ',.{digits}f') + '{unit}' : 'N/A')")


# ---------------------------------------------------------------- KPI card
KPI_FIELDS = ["kpi", "title", "value", "value_fmt", "cmp1", "cmp1_label", "cmp2", "cmp2_label",
              "cmp3", "cmp3_label", "cmp_unit", "cmp_fmt", "note1", "note2"]


def kpi_card(kpi: str, color: str = "#2f5597", lower_is_better: bool = False) -> dict:
    """Thẻ KPI như HTML gốc: tiêu đề, số lớn, tối đa 3 so sánh xanh / đỏ, 2 dòng ghi chú.

    Dataset dạng dài *_kpi_cards (1 dòng / KPI, cột = KPI_FIELDS); spec lọc dòng `kpi`.
    value_fmt / cmp_fmt: định dạng d3 (vd ',.0f', '.2f'); cmp_unit: '%', ' pp' hoặc ''.
    So sánh NULL -> 'N/A' xám; nhãn so sánh rỗng -> ẩn. `lower_is_better` (tỷ lệ quá hạn,
    nợ xấu): tăng -> đỏ."""
    up, down = (RED, GREEN) if lower_is_better else (GREEN, RED)

    def cmp_text(n: int, x: float) -> dict:
        f = f"datum.cmp{n}"
        signed = (f"(isValid({f}) ? (({f}) > 0 ? '▲ ' : ({f}) < 0 ? '▼ ' : '▪ ') + "
                  f"format(abs({f}), datum.cmp_fmt) + datum.cmp_unit : 'N/A')")
        return {"mark": {"type": "text", "align": "left", "fontSize": 12, "fontWeight": "bold",
                         "x": {"expr": f"width * {x}"}, "y": 80},
                "encoding": {
                    "text": {"value": {"expr": f"isValid(datum.cmp{n}_label) && "
                                               f"datum.cmp{n}_label != '' ? {signed} + ' ' + "
                                               f"datum.cmp{n}_label : ''"}},
                    "color": {"condition": [{"test": f"isValid({f}) && {f} > 0", "value": up},
                                            {"test": f"isValid({f}) && {f} < 0", "value": down}],
                              "value": GREY}}}

    note = lambda field, y: {  # noqa: E731
        "mark": {"type": "text", "align": "left", "fontSize": 10, "color": "#888", "x": 4,
                 "y": y}, "encoding": {"text": {"field": field}}}
    return _base(
        transform=[{"filter": f"datum.kpi == '{kpi}'"}],
        layer=[
            {"mark": {"type": "text", "align": "left", "baseline": "top", "fontSize": 12,
                      "fontWeight": "bold", "color": "#666", "x": 4, "y": 4},
             "encoding": {"text": {"field": "title"}}},
            {"mark": {"type": "text", "align": "left", "baseline": "top", "fontSize": 32,
                      "fontWeight": "bold", "color": color, "x": 4, "y": 26},
             "encoding": {"text": {"value": {"expr": "isValid(datum.value) ? "
                                                     "format(datum.value, datum.value_fmt) "
                                                     ": 'N/A'"}}}},
            cmp_text(1, 0.02), cmp_text(2, 0.35), cmp_text(3, 0.68),
            note("note1", 104), note("note2", 120),
        ], config={"view": {"stroke": None}})


# ---------------------------------------------------------------- gauge (Deposit)
def gauge(color: str, field: str = "pct") -> dict:
    """Đồng hồ bán nguyệt % hoàn thành chỉ tiêu (1 cột `field`). Cung kẹp trong [0, 120]%,
    số ở giữa là giá trị thật (có thể âm / > 100). NULL -> 'N/A'."""
    clamp = f"clamp(isValid(datum.{field}) ? datum.{field} : 0, 0, 120)"
    theta = f"-PI / 2 + PI * {clamp} / 120"
    return _base(layer=[
        {"mark": {"type": "arc", "innerRadius": {"expr": "min(width, height * 2) * 0.32"},
                  "outerRadius": {"expr": "min(width, height * 2) * 0.45"},
                  "theta": {"expr": "-PI / 2"}, "theta2": {"expr": "PI / 2"},
                  "color": "#e5e7eb", "y": {"expr": "height * 0.9"}}},
        {"mark": {"type": "arc", "innerRadius": {"expr": "min(width, height * 2) * 0.32"},
                  "outerRadius": {"expr": "min(width, height * 2) * 0.45"},
                  "theta": {"expr": "-PI / 2"}, "theta2": {"expr": theta},
                  "color": color, "y": {"expr": "height * 0.9"}}},
        {"mark": {"type": "text", "fontSize": 26, "fontWeight": "bold", "color": color,
                  "y": {"expr": "height * 0.78"}},
         "encoding": {"text": {"value": {"expr": f"isValid(datum.{field}) ? "
                                                 f"format(datum.{field}, '.1f') + '%' "
                                                 ": 'N/A'"}}}},
        {"mark": {"type": "text", "fontSize": 10, "color": "#888", "y": {"expr": "height * 0.97"},
                  "x": {"expr": "width / 2 - min(width, height * 2) * 0.385"}, "text": "0%"}},
        {"mark": {"type": "text", "fontSize": 10, "color": "#888", "y": {"expr": "height * 0.97"},
                  "x": {"expr": "width / 2 + min(width, height * 2) * 0.385"}, "text": "120%"}},
    ], config={"view": {"stroke": None}})


# ---------------------------------------------------------------- combo 2 trục
def dual_axis_combo(x: str, bar: str, line: str, bar_title: str, line_title: str,
                    bar_color: str, line_color: str, x_sort: str | None = None,
                    bar_fmt: str = ",.0f", line_fmt: str = ".2f") -> dict:
    """Cột (trục trái) + đường có điểm (trục phải), nhãn số trên cột và trên điểm.
    Native combo có 'Enable dual axis' -> chỉ dùng spec này khi cần nhãn / kiểu như gốc."""
    xe = {"field": x, "type": "ordinal", "title": None, "sort": x_sort,
          "axis": {"labelAngle": 0}}
    return _base(
        encoding={"x": xe},
        layer=[
            {"layer": [
                {"mark": {"type": "bar", "color": bar_color, "cornerRadiusEnd": 2,
                          "width": {"band": 0.6}, "tooltip": True},
                 "encoding": {"y": {"field": bar, "type": "quantitative", "title": bar_title,
                                    "axis": {"titleColor": bar_color}}}},
                {"mark": {"type": "text", "baseline": "top", "dy": 4, "fontSize": 10,
                          "color": "white", "fontWeight": "bold"},
                 "encoding": {"y": {"field": bar, "type": "quantitative"},
                              "text": {"field": bar, "format": bar_fmt}}}]},
            {"layer": [
                {"mark": {"type": "line", "color": line_color, "strokeWidth": 2.5,
                          "point": {"filled": True, "size": 50, "color": line_color},
                          "tooltip": True},
                 "encoding": {"y": {"field": line, "type": "quantitative", "title": line_title,
                                    "scale": {"zero": False, "padding": 30},
                                    "axis": {"orient": "right", "titleColor": line_color}}}},
                {"mark": {"type": "text", "dy": -12, "fontSize": 10, "fontWeight": "bold",
                          "color": line_color},
                 "encoding": {"y": {"field": line, "type": "quantitative",
                                    "scale": {"zero": False, "padding": 30},
                                    "axis": {"orient": "right"}},
                              "text": {"field": line, "format": line_fmt}}}]},
        ],
        resolve={"scale": {"y": "independent"}},
        config={"legend": {"orient": "bottom"}})


# ---------------------------------------------------------------- stacked + total line
def stacked_with_total(x: str, series: str, value: str, colors: dict[str, str],
                       total_color: str = "#7f7f7f") -> dict:
    """Card New Acquisition: cột xếp chồng theo kênh + đường Total nét đứt (như Chart.js)."""
    return _base(
        encoding={"x": {"field": x, "type": "ordinal", "title": None,
                        "axis": {"labelAngle": 0}}},
        layer=[
            {"mark": {"type": "bar", "width": {"band": 0.7}, "tooltip": True},
             "encoding": {"y": {"field": value, "type": "quantitative", "aggregate": "sum",
                                "title": None, "stack": "zero"},
                          "color": {"field": series, "type": "nominal", "title": None,
                                    "scale": {"domain": list(colors),
                                              "range": list(colors.values())}}}},
            {"transform": [{"aggregate": [{"op": "sum", "field": value, "as": "_total"}],
                            "groupby": [x]}],
             "layer": [
                 {"mark": {"type": "line", "strokeDash": [6, 4], "strokeWidth": 2,
                           "color": total_color},
                  "encoding": {"y": {"field": "_total", "type": "quantitative"}}},
                 {"mark": {"type": "text", "dy": -8, "fontSize": 10, "color": "#444"},
                  "encoding": {"y": {"field": "_total", "type": "quantitative"},
                               "text": {"field": "_total", "format": ",.0f"}}}]},
        ],
        config={"legend": {"orient": "bottom"}})


# ---------------------------------------------------------------- 2 năm x xếp chồng
def grouped_stacked_years(month: str, year: str, stack: str, value: str,
                          colors: dict[str, str], series: str | None = None) -> dict:
    """Deposit 'Monthly Ending Balance - Year Comparison': mỗi tháng 2 cột (năm trước / năm
    nay, xOffset), mỗi cột xếp chồng theo lớp sản phẩm. `colors` theo `stack`; năm trước
    nhạt hơn (opacity)."""
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
              "Dec"]
    extra = {}
    if series:   # dataset gộp 'lớp + năm' trong 1 cột (vd '1-CASA this year') -> tách
        extra["transform"] = [
            {"calculate": f"split(datum.{series}, ' ')[0]", "as": stack},
            {"calculate": f"indexof(datum.{series}, 'this year') >= 0 ? 'This year' "
                          f": 'Last year'", "as": year}]
    return _base(
        **extra,
        mark={"type": "bar", "tooltip": True},
        encoding={
            "x": {"field": month, "type": "ordinal", "sort": months, "title": None,
                  "axis": {"labelAngle": 0}},
            "xOffset": {"field": year, "type": "ordinal", "sort": "ascending"},
            "y": {"field": value, "type": "quantitative", "aggregate": "sum", "stack": "zero",
                  "title": "Ending Balance (K USD)"},
            "color": {"field": stack, "type": "nominal", "title": None,
                      "scale": {"domain": list(colors), "range": list(colors.values())}},
            "opacity": {"field": year, "type": "ordinal", "title": "Year",
                        "scale": {"range": [0.45, 1.0]}},
        },
        config={"legend": {"orient": "bottom"}})


# ---------------------------------------------------------------- donut
def donut(category: str, value: str, colors: dict[str, str], center: str | None = None) -> dict:
    """Donut có % ở giữa = tỷ trọng lát `center` (mặc định lát đầu tiên trong `colors`)."""
    first = center or next(iter(colors))
    return _base(
        transform=[{"joinaggregate": [{"op": "sum", "field": value, "as": "_tot"}]},
                   {"calculate": f"datum.{value} / datum._tot", "as": "_share"}],
        layer=[
            {"mark": {"type": "arc", "innerRadius": {"expr": "min(width, height) * 0.30"},
                      "outerRadius": {"expr": "min(width, height) * 0.42"}, "tooltip": True},
             "encoding": {"theta": {"field": value, "type": "quantitative", "stack": True},
                          "color": {"field": category, "type": "nominal", "title": None,
                                    "scale": {"domain": list(colors),
                                              "range": list(colors.values())},
                                    "legend": {"orient": "bottom", "columns": 2,
                                               "labelLimit": 120}},
                          "order": {"field": category, "sort": "ascending"}}},
            {"transform": [{"filter": f"datum.{category} == '{first}'"}],
             "mark": {"type": "text", "fontSize": 16, "fontWeight": "bold",
                      "color": colors[first]},
             "encoding": {"text": {"field": "_share", "format": ".1%"}}},
        ],
        config={"view": {"stroke": None}})


# ---------------------------------------------------------------- bảng có màu xanh / đỏ
def signed_table(label: str, columns: list[tuple[str, str]], unit: str = "",
                 lower_is_better: bool = False) -> dict:
    """Bảng so sánh với số tăng xanh / giảm đỏ (ngược lại khi `lower_is_better`, vd tỷ lệ
    nợ quá hạn), NULL 'N/A'. columns: (field, tiêu đề)."""
    up, down = (RED, GREEN) if lower_is_better else (GREEN, RED)
    fold = [c for c, _ in columns]
    titles = {c: t for c, t in columns}
    return _base(
        transform=[{"fold": fold, "as": ["_col", "_val"]},
                   {"calculate": "{" + ", ".join(f"'{c}': '{t}'" for c, t in titles.items())
                    + "}[datum._col]", "as": "_title"}],
        mark={"type": "text", "fontSize": 12, "fontWeight": "bold"},
        encoding={
            "y": {"field": label, "type": "nominal", "title": None, "sort": None,
                  "axis": {"labelFontSize": 12, "labelLimit": 320, "domain": False,
                           "ticks": False}},
            "x": {"field": "_title", "type": "nominal", "title": None,
                  "sort": [t for _, t in columns],
                  "axis": {"orient": "top", "labelAngle": 0, "domain": False, "ticks": False}},
            "text": {"value": {"expr": _signed("datum._val", unit, 2 if unit else 0)}},
            "color": {"condition": [{"test": "isValid(datum._val) && datum._val > 0",
                                     "value": up},
                                    {"test": "isValid(datum._val) && datum._val < 0",
                                     "value": down}], "value": GREY},
        },
        config={"view": {"stroke": None}, "axis": {"grid": False}})
