"""Dữ liệu tham chiếu cho dashboard: chỉ tiêu kế hoạch (targets) và tỷ giá (fx_rates).

- targets  : chỉ tiêu theo chi nhánh x kỳ (tháng YYYY-MM và năm YYYY), gửi 1 lần ngày đầu.
             Giá trị ước từ quy mô dữ liệu sinh ra, nhân hệ số ngẫu nhiên 0.9-1.2
             -> % hoàn thành kế hoạch dao động quanh 100%.
- fx_rates : tỷ giá quy đổi về VND mỗi ngày. Cố định = USD_RATE, đúng tỷ giá hệ thống nguồn
             dùng khi lập file tổng hợp (bang1247) -> Gold đối soát khớp tuyệt đối.

Dùng Random riêng (seed + offset) để không làm đổi chuỗi ngẫu nhiên của các bảng khác.
"""

from __future__ import annotations

import random
from datetime import date

from data_generator.common import BRANCHES, USD_RATE

TARGET_HEADER = ["metric_code", "period_type", "period", "branchnbr", "target_value"]
FX_HEADER = ["rate_date", "currencycd", "rate_to_vnd"]

# metric_code -> đơn vị
TARGET_METRICS = {
    # Thẻ: 5 KPI Overview x loại KH (hậu tố _CN cá nhân / _TC tổ chức; Total = cộng 2 loại).
    # FLOW (luỹ kế): năm = tổng 12 tháng. STOCK: năm = chỉ tiêu tháng 12 (KHÔNG cộng 12 tháng).
    # USING (thẻ dùng distinct trong tháng): chỉ có chỉ tiêu tháng, không có chỉ tiêu năm.
    **{f"CARD_{k}_{t}": u for k, u in (("NEW_ACQ", "cards"), ("CUSTOMER_BASE", "cards"),
                                       ("USING", "cards"), ("SALES_VOLUME", "VND"),
                                       ("BALANCE", "VND")) for t in ("CN", "TC")},
    # Tiền gửi: chỉ tiêu TĂNG TRƯỞNG số dư trong năm (so với cuối năm trước), theo chi nhánh x
    # năm (chỉ dòng YEAR). Vùng / toàn hàng = tổng chi nhánh. Hoàn thành = tăng trưởng YTD /
    # chỉ tiêu (như dashboard gốc), KHÔNG phải số dư / chỉ tiêu.
    "DEPOSIT_GROWTH_ALL": "VND",      # tăng trưởng số dư mọi sản phẩm
    "DEPOSIT_GROWTH_CASA": "VND",     # tăng trưởng số dư CASA
    "DEPOSIT_GROWTH_TD_ISA": "VND",   # tăng trưởng số dư có kỳ hạn + ISA
}
CARD_STOCK_TARGETS = ("CARD_CUSTOMER_BASE_", "CARD_BALANCE_")
CARD_MONTH_ONLY = ("CARD_USING_",)
GROWTH_TARGETS = {k for k in TARGET_METRICS if k.startswith("DEPOSIT_GROWTH_")}


def target_rows(seed: int, year: int, per_branch_month: dict[str, float]) -> list[list]:
    """per_branch_month: giá trị kỳ vọng 1 chi nhánh / 1 tháng cho từng metric (chỉ tiêu tăng
    trưởng tiền gửi: giá trị kỳ vọng cả NĂM, chỉ sinh dòng YEAR)."""
    rnd = random.Random(seed + 101)
    rows = []
    for b in BRANCHES:
        for code, base in per_branch_month.items():
            if code in GROWTH_TARGETS:
                v = round(base * rnd.uniform(0.9, 1.2), 0)
                rows.append([code, "YEAR", str(year), b[0], f"{v:.0f}"])
                continue
            year_total = 0.0
            for m in range(1, 13):
                v = round(base * rnd.uniform(0.9, 1.2) * (1 + 0.01 * m), 0)
                year_total += v
                rows.append([code, "MONTH", f"{year}-{m:02d}", b[0], f"{v:.0f}"])
            if code.startswith(CARD_MONTH_ONLY):
                continue
            if code.startswith(CARD_STOCK_TARGETS):   # tồn: chỉ tiêu năm = cuối năm
                year_total = v
            rows.append([code, "YEAR", str(year), b[0], f"{year_total:.0f}"])
    return rows


def fx_rows(as_of: date) -> list[list]:
    return [[as_of.isoformat(), "VND", "1"], [as_of.isoformat(), "USD", f"{USD_RATE}"]]
