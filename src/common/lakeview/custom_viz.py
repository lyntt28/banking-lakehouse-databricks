"""Danh mục widget Custom Viz (Vega-Lite) thay cho widget native, theo thiết kế HTML gốc.

Mỗi mục: dashboard, trang, widget native được thay (tiêu đề), dataset (đã có trong
`.lvdash.json` do generator sinh), Fields cần khai báo (Name = tên cột dataset) và spec.

Vì cấu trúc JSON của widget Custom Viz trong `.lvdash.json` chưa được Databricks công bố,
generator KHÔNG chèn các widget này vào file dashboard. `write_specs` xuất spec ra
`dashboards/custom_viz/<dashboard>/<widget>.json` để dán vào UI (xem
docs/custom_visualizations.md). Sau khi có 1 widget tạo từ UI + export, có thể tự động hoá."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

from common.lakeview import vega as v
from common.lakeview.card import CARD_COLORS
from common.lakeview.deposit import DEP_COLORS
from common.lakeview.overdue import OVD_COLORS


@dataclass(frozen=True)
class CustomViz:
    dashboard: str
    page: str
    name: str
    replaces: str        # tiêu đề widget native được thay ('' = widget mới)
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


CARD, DEP, OVD = "card_business", "retail_deposit", "loan_overdue"


def catalog() -> list[CustomViz]:
    c, d, o = CARD_COLORS, DEP_COLORS, OVD_COLORS
    out = []
    # ---- Card Business
    for i, (kpi, title) in enumerate([
            ("new_acquisition", "New Acquisition"), ("customer_base", "Customer Base"),
            ("using_customer", "Using Customer"), ("sales_volume", "Sales Volume (K USD)"),
            ("balance", "Balance (K USD)")]):
        out.append(CustomViz(CARD, "main", f"kpi_{kpi}", title, "card_kpi_cards",
                             v.kpi_card(kpi, c["kpi"][i], n_cmp=2)))
    out.append(CustomViz(CARD, "main", "new_acquisition", "New Acquisition", "card_new_acq",
                         v.stacked_with_total("month", "channel", "cards",
                                              {"Smart Credit": c["smart_credit"],
                                               "Branches": c["branches"]})))
    for donut, title in (("sector", "By Sectors"), ("location", "By Locations"),
                         ("channel", "By Channels")):
        out.append(CustomViz(CARD, "main", f"donut_{donut}", title, f"card_mix_{donut}",
                             v.donut("slice", "amount", c[donut])))
    out.append(CustomViz(CARD, "main", "risk_compare", "(Delinquency comparison table)",
                         "card_risk_compare",
                         v.signed_table("compare", [("dpd_1p", "DPD 1+"), ("dpd_10p", "DPD 10+"),
                                                    ("dpd_30p", "DPD 30+"), ("npl", "NPL")],
                                        unit=" pp", lower_is_better=True)))
    # ---- Retail Deposit
    for kpi, title, key in ([
            ("ending_balance", "Ending Balance", "bal"),
            ("monthly_avg_balance", "Monthly Avg Balance", "mavg"),
            ("retail_customers", "Retail Customers", "cust")]):
        out.append(CustomViz(DEP, "overview", f"kpi_{kpi}", title, "dep_kpi_cards",
                             v.kpi_card(kpi, d["ticker"][key])))
    for col, title, key in (("all_pct", "All Products Target (%)", "all"),
                            ("casa_pct", "CASA Target (%)", "casa"),
                            ("tda_pct", "TDA/ISA Target (%)", "tda")):
        out.append(CustomViz(DEP, "overview", f"gauge_{key}", title, "dep_gauges",
                             v.gauge(d["gauge"][key], col)))
    out.append(CustomViz(DEP, "overview", "monthly_years",
                         "Monthly Ending Balance - Year Comparison", "dep_monthly",
                         v.grouped_stacked_years(
                             "month", "_year", "_class", "ending_balance",
                             {"1-CASA": d["mom"]["1-CASA this year"],
                              "2-TD": d["mom"]["2-TD this year"],
                              "3-ISA": d["mom"]["3-ISA this year"]}, series="series")))
    out.append(CustomViz(DEP, "overview", "term_combo", "Deposit by Term and IR (in Month)",
                         "dep_term",
                         v.dual_axis_combo("tenor", "balance", "avg_interest", "Balance (K USD)",
                                           "Avg Interest (%)", d["term_bar"], d["term_line"],
                                           line_fmt=".2f")))
    # ---- Overdue
    for kpi, title, key, bad in (("overdue_ratio", "Overdue Ratio", "ratio", True),
                                 ("overdue_usd", "Overdue Balance (USD)", "overdue", True),
                                 ("ending_usd", "Ending Balance (USD)", "ending", False)):
        out.append(CustomViz(OVD, "main", f"kpi_{kpi}", title, "ovd_kpi_cards",
                             v.kpi_card(kpi, o["ticker"][key], lower_is_better=bad)))
    for ds, title, x in (("ovd_daily_trend", "Overdue Trend (Last 5 Days)", "day"),
                         ("ovd_monthly_trend", "Overdue Trend (Monthly)", "month")):
        out.append(CustomViz(OVD, "main", ds, title, ds,
                             v.dual_axis_combo(x, "overdue_k_usd", "overdue_ratio",
                                               "Overdue Amount (1000 USD)", "Overdue Ratio (%)",
                                               o["amount"], o["ratio"])))
    return out


def write_specs(root: Path) -> list[Path]:
    """Ghi spec + README liệt kê widget (dashboard, trang, dataset, Fields)."""
    paths, lines = [], ["# Custom Viz specs (generated - do not edit by hand)", "",
                        "| Dashboard | Page | Spec file | Replaces | Dataset | Fields |",
                        "|---|---|---|---|---|---|"]
    for w in catalog():
        p = root / w.dashboard / f"{w.name}.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(w.spec, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        paths.append(p)
        lines.append(f"| {w.dashboard} | {w.page} | `{w.dashboard}/{w.name}.json` | "
                     f"{w.replaces} | `{w.dataset}` | {', '.join(w.fields)} |")
    (root / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return paths
