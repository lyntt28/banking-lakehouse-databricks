"""Danh mục widget Custom Viz (Vega-Lite) có trong 3 dashboard nghiệp vụ.

Generator (card/deposit/overdue.py) chèn trực tiếp widget `custom-vega-viz` vào
`.lvdash.json` (định dạng đã kiểm chứng trên workspace, xem `Dash.custom_viz`). Module này
đọc ngược các widget đó và `write_specs` xuất spec ra `dashboards/custom_viz/<dashboard>/`
để CI biên dịch bằng Vega-Lite (tools/compile_vega_specs.js) và để tra cứu Fields."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CustomViz:
    dashboard: str
    page: str
    name: str
    title: str
    dataset: str
    spec: dict

    @property
    def fields(self) -> list[str]:
        """Cột dataset mà spec tham chiếu (khai báo trong Fields, Name = tên cột)."""
        return spec_fields(self.spec)


def spec_fields(spec: dict) -> list[str]:
    """Tên cột spec đọc từ dataset: "field": "x" và datum.x / datum["x"] trong biểu thức,
    trừ cột spec tự tạo (calculate / aggregate / joinaggregate / fold)."""
    text = json.dumps(spec)
    used = set(re.findall(r'"field": "([^"_][^"]*)"', text))
    used |= set(re.findall(r"datum\.([A-Za-z][A-Za-z0-9_]*)", text))
    for fold in re.findall(r'"fold": \[([^\]]*)\]', text):
        used |= set(re.findall(r'"([^"]+)"', fold))
    made = set(re.findall(r'"as": "([^"]+)"', text))
    made |= {x for pair in re.findall(r'"as": \["([^"]+)", "([^"]+)"\]', text) for x in pair}
    return sorted(used - made)


def catalog() -> list[CustomViz]:
    """Mọi widget Custom Viz lấy thẳng từ dashboard do generator sinh (1 nguồn duy nhất,
    không khai báo lặp). Tên file = dataset + tiêu đề (thẻ KPI không tiêu đề: mã KPI)."""
    from common.lakeview import build_all

    out = []
    for fname, body in build_all("").items():
        dash = fname.removesuffix(".lvdash.json")
        for page in body["pages"]:
            for item in page["layout"]:
                spec = item["widget"].get("spec", {})
                if spec.get("widgetType") != "custom-vega-viz":
                    continue
                vl = json.loads(spec["jsonSpec"]["spec"])
                vl.pop("data", None)
                title = spec["frame"]["title"]["value"]
                if not title:
                    m = re.search(r"datum\.kpi == '(\w+)'", spec["jsonSpec"]["spec"])
                    title = f"kpi {m.group(1)}" if m else item["widget"]["name"]
                ds = item["widget"]["queries"][0]["query"]["datasetName"]
                slug = re.sub(r"[^a-z0-9]+", "_", title.lower()).strip("_")
                out.append(CustomViz(dash, page["name"], f"{ds}__{slug}", title, ds, vl))
    return out


def write_specs(root: Path) -> list[Path]:
    """Ghi spec + README liệt kê widget (dashboard, trang, dataset, Fields)."""
    paths, lines = [], ["# Custom Viz specs (generated - do not edit by hand)", "",
                        "| Dashboard | Page | Spec file | Title | Dataset | Fields |",
                        "|---|---|---|---|---|---|"]
    for w in catalog():
        p = root / w.dashboard / f"{w.name}.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(w.spec, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        paths.append(p)
        lines.append(f"| {w.dashboard} | {w.page} | `{w.dashboard}/{w.name}.json` | "
                     f"{w.title} | `{w.dataset}` | {', '.join(w.fields)} |")
    (root / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
