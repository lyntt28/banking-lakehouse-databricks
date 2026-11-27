"""3 dashboard Databricks AI/BI (Lakeview) dựng lại từ dashboard Flask + Chart.js gốc.

Bố cục (thứ tự, vị trí, tỉ lệ rộng), loại biểu đồ và mã màu chép từ code gốc
(dashboard Flask + Chart.js cũ, đã xóa khỏi repo). Lưới Lakeview rộng 6 cột:
    - hàng 2 biểu đồ 1fr 1fr      -> 3 + 3
    - hàng 1fr 1.5fr (Overdue)    -> 2 + 4 (gần nhất trên lưới 6 cột)
    - hàng 1fr 2fr (Deposit)      -> 2 + 4
Những gì Lakeview không vẽ được y hệt (ghi rõ trong docs/gold_dashboards.md):
    gradient màu cột, đường "Total" chồng lên cột chồng, đồng hồ (gauge) -> counter %,
    2 chồng cột / tháng (năm nay vs năm trước) -> cột nhóm.

Mỗi dataset nhận tham số :report_date (DATE, mặc định hôm nay -> lấy ngày có dữ liệu gần
nhất <= ngày chọn), Card thêm :customer_type, Overdue thêm :branch, Deposit thêm :region.
`build_all(catalog)` trả về {tên file: dict JSON}; `write_all()` ghi ra thư mục dashboards/.

Mỗi dashboard 1 file (sửa dashboard nào chỉ đụng file đó -> các nhánh làm song song không conflict):
    core.py     khung JSON (Dash), format số, SQL dùng chung (ngày báo cáo, tỷ giá)
    card.py     Card Business            -> dashboards/card_business.lvdash.json
    overdue.py  Loan Overdue (LOAN)      -> dashboards/loan_overdue.lvdash.json
    deposit.py  Retail Deposit (DEPOSIT)     -> dashboards/retail_deposit.lvdash.json
    ops.py      Operations Monitor (DE)  -> dashboards/ops_monitor.lvdash.json
Sinh lại JSON:  PYTHONPATH=src python -m common.lakeview dashboards
"""

from __future__ import annotations

import json
from pathlib import Path

from common.lakeview.card import CARD_COLORS, card_dashboard
from common.lakeview.deposit import DEP_COLORS, deposit_dashboard
from common.lakeview.ops import ops_dashboard
from common.lakeview.overdue import OVD_COLORS, overdue_dashboard

__all__ = ["CARD_COLORS", "DEP_COLORS", "OVD_COLORS", "DASHBOARDS", "build_all", "write_all",
           "card_dashboard", "deposit_dashboard", "ops_dashboard", "overdue_dashboard"]

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
