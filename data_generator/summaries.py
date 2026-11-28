"""File báo cáo tổng hợp do "hệ thống nguồn" gửi kèm -> dùng để ĐỐI SOÁT với Gold.

- card_kpi       : 9 file KPI thẻ/ngày, dạng dọc Trx_Date | Metric_Code | Metric_Value,
                   số có dấu phẩy ("219,627"), tỷ lệ có dấu % ("56.78%")
- deposit_summary: số dư tiền gửi theo nhóm TK x chi nhánh (layout bang1247),
                   STND_DT dạng YYYYMMDD, BAL đơn vị triệu VND
- card_overdue   : dư nợ / nợ quá hạn thẻ tín dụng theo nhóm SP x CN quản lý x CN bán x
                   nhóm nợ (layout credit_card_overdue_YYYYMMDD.txt), số dư VND

Số liệu tính từ bản ghi HỢP LỆ (không tính dòng bị cài lỗi FAIL), nên sau khi Silver
loại lỗi, Gold tính lại phải khớp đúng các file này.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path

from data_generator.common import BRANCH_BY_NBR, CARD_PRODUCTS, DEPOSIT_ALL, USD_RATE, write_csv

KPI_FILES = ["01_member_eop", "02_member_usage", "03_new_consumer", "04_member_closed",
             "05_balance_eop", "06_spend_sector", "07_spend_location", "08_spend_channel",
             "09_risk_dpd"]
DEPOSIT_HEADER = ["STND_DT", "ACCOUNT_GROUP", "GROUP_CLASS", "CURRENCY", "MANAGE_BRANCH",
                  "REGION", "BAL", "PORT", "DIFF_T_T1", "MON_AVG_BAL_T"]


def _n(x: float) -> str:
    return f"{round(x):,}"


def _pct(a: float, b: float) -> str:
    return f"{(a / b * 100 if b else 0):.2f}%"


def card_kpis(cards: list[dict], clean_txns: list[dict], statements: list[list],
              as_of: date) -> dict[str, list[tuple[str, str]]]:
    """Tính 9 nhóm KPI thẻ trong ngày. Trả về {tên file: [(metric_code, value)]}."""
    active = [c for c in cards if c["status"] == "ACTIVE"]
    eop = {t: sum(1 for c in active if c["cust_type"] == t) for t in ("CN", "TC")}
    by_card = {c["card_no"]: c for c in cards}
    users = {t: {x["card_no"] for x in clean_txns if by_card[x["card_no"]]["cust_type"] == t}
             for t in ("CN", "TC")}
    new = {t: sum(1 for c in cards if c["open_date"] == as_of and c["cust_type"] == t)
           for t in ("CN", "TC")}
    closed = {t: sum(1 for c in cards if c["close_date"] == as_of and c["cust_type"] == t)
              for t in ("CN", "TC")}
    amt = defaultdict(float)
    for x in clean_txns:
        amt[x["txn_type"]] += x["amount"]
        amt[x["location"]] += x["amount"]
        amt[x["channel"]] += x["amount"]
    total = sum(amt[t] for t in ("PURCHASE", "CASH_ADVANCE", "INSTALLMENT", "FLEXI"))
    # statements: [stnd_dt, card_no, bal, rev, min_due, dpd, limit] - dùng dư nợ thật (state)
    bal = {c["card_no"]: c["balance"] for c in active}
    dpd = {c["card_no"]: c["dpd"] for c in active}
    total_bal = sum(bal.values())
    n_active = len(active)
    rev = [c for c in active if c["revolver"] and c["balance"] > 0]
    risk = []
    for code, lo in (("DPD_1P", 1), ("DPD_10P", 10), ("DPD_30P", 30), ("NPL_90P", 90)):
        m = [k for k, d in dpd.items() if d >= lo]
        risk += [(f"{code}_Member", _n(len(m))), (f"{code}_Balance", _n(sum(bal[k] for k in m))),
                 (f"{code}_Rate", _pct(sum(bal[k] for k in m), total_bal))]
    return {
        "01_member_eop": [("EOP_Consumer", _n(eop["CN"])), ("EOP_Corporate", _n(eop["TC"])),
                          ("EOP_Total", _n(n_active))],
        "02_member_usage": [("Using_Consumer", _n(len(users["CN"]))),
                            ("Usage_Consumer_Rate", _pct(len(users["CN"]), eop["CN"])),
                            ("Using_Corporate", _n(len(users["TC"]))),
                            ("Usage_Corporate_Rate", _pct(len(users["TC"]), eop["TC"]))],
        "03_new_consumer": [("New_Consumer", _n(new["CN"])), ("New_Corporate", _n(new["TC"])),
                            ("New_Member", _n(new["CN"] + new["TC"]))],
        "04_member_closed": [("Consumer_Closed", _n(closed["CN"])),
                             ("Corporate_Closed", _n(closed["TC"])),
                             ("Member_Closed", _n(closed["CN"] + closed["TC"])),
                             ("Attrition_Rate",
                              _pct(closed["CN"] + closed["TC"], n_active))],
        "05_balance_eop": [("Total_Outstanding_Balance", _n(total_bal)),
                           ("Avg_Balance_Per_Member", _n(total_bal / n_active if n_active else 0))],
        "06_spend_sector": [("Purchasing_Amt", _n(amt["PURCHASE"])),
                            ("Cash_Advance_Amt", _n(amt["CASH_ADVANCE"])),
                            ("Installment_Amt", _n(amt["INSTALLMENT"])),
                            ("Flexi_Amt", _n(amt["FLEXI"])), ("Total_Spending_Amt", _n(total)),
                            ("Avg_Spending_Per_Member",       # / thẻ hiện hữu (EOP)
                             _n(total / n_active if n_active else 0))],
        "07_spend_location": [("Domestic_Spending_Amt", _n(amt["DOMESTIC"])),
                              ("XBorder_Spending_Amt", _n(amt["XBORDER"])),
                              ("Domestic_Rate", _pct(amt["DOMESTIC"], total)),
                              ("XBorder_Rate", _pct(amt["XBORDER"], total))],
        "08_spend_channel": [("POS_Spending_Amt", _n(amt["POS"])),
                             ("Online_Spending_Amt", _n(amt["ONLINE"])),
                             ("Offline_Rate", _pct(amt["POS"], total)),
                             ("Online_Rate", _pct(amt["ONLINE"], total))],
        "09_risk_dpd": [("Revolving_Balance", _n(sum(c["balance"] for c in rev))),
                        ("Revolving_Member", _n(len(rev))),
                        ("Revolving_Rate", _pct(len(rev), n_active))] + risk,
    }


def write_card_kpis(out_dir: Path, kpis: dict[str, list[tuple[str, str]]], as_of: date) -> int:
    n = 0
    for name in KPI_FILES:
        rows = [[as_of.isoformat(), code, val] for code, val in kpis[name]]
        n += write_csv(out_dir / f"{name}_{as_of:%Y%m%d}.txt",
                       ["Trx_Date", "Metric_Code", "Metric_Value"], rows, delimiter="\t")
    return n


def deposit_summary(accounts: list[dict], failed: set[str], as_of: date,
                    prev: dict, month_hist: dict) -> list[list]:
    """Tổng hợp số dư tiền gửi theo (nhóm TK, tiền tệ, chi nhánh). `prev`/`month_hist`
    giữ số liệu các ngày trước để tính chênh lệch T-1 và bình quân tháng."""
    groups = {p[0]: (p[1], p[2]) for p in DEPOSIT_ALL}
    bal: dict[tuple, float] = defaultdict(float)
    for a in accounts:
        if a["product_type"] == "LOAN" or a["account_id"] in failed:
            continue
        vnd = a["balance"] * (USD_RATE if a["currencycd"] == "USD" else 1)
        g = groups[a["product_type"]]
        bal[(g[0], g[1], a["currencycd"], a["branchnbr"])] += vnd / 1e6  # triệu VND
    total = sum(bal.values())
    rows = []
    for key in sorted(bal):
        b = round(bal[key], 2)
        hist = month_hist.setdefault((as_of.year, as_of.month, key), [])
        hist.append(b)
        rows.append([as_of.strftime("%Y%m%d"), key[0], key[1], key[2], key[3],
                     BRANCH_BY_NBR[key[3]][3], f"{b:.2f}", f"{b / total * 100:.2f}",
                     f"{b - prev.get(key, b):.4f}", f"{sum(hist) / len(hist):.2f}"])
        prev[key] = b
    return rows


CARD_OVERDUE_HEADER = ["REPORT_DT", "PRDT_CODE", "PRODUCT_TYPE", "PRODUCT_GROUP", "PRODUCT_NAME",
                       "MNG_BR", "SAL_BR", "DEL_GRP", "PORT_ACC", "OVD_ACC", "PORT_BAL",
                       "OVD_BAL"]
DEL_GROUPS = [(0, "0"), (10, "1-10"), (90, "11-90"), (180, "91-180"), (360, "181-360")]
ONLINE_SALE_BRANCH = 1   # thẻ mở qua Smart Credit (online) ghi nhận cho CN hội sở


def del_grp(dpd: int) -> str:
    """Số ngày quá hạn -> nhóm nợ của file credit_card_overdue."""
    return next((g for limit, g in DEL_GROUPS if dpd <= limit), "361+")


OVERDUE_PRODUCTS = {grade: prod for prod, grade, *_ in CARD_PRODUCTS}   # nhóm SP -> sản phẩm
OVERDUE_DEL_GROUPS = {g for _, g in DEL_GROUPS} | {"361+"}


def card_overdue_errors(rows: list[list]) -> list[str]:
    """Kiểm tra quy tắc nghiệp vụ của file card_overdue HỢP LỆ (generator không cài lỗi vào file
    này; dữ liệu lỗi để test DQ nằm riêng trong tests/test_silver.py). Rỗng = đạt."""
    errs = []
    for r in rows:
        _, _, _, grp, name, mng, sal, dg, pacc, oacc, pbal, obal = r
        pbal, obal = float(pbal), float(obal)
        if not (0 <= obal <= pbal):
            errs.append(f"OVD_BAL ngoài [0, PORT_BAL]: {r}")
        if not (0 <= oacc <= pacc):
            errs.append(f"OVD_ACC ngoài [0, PORT_ACC]: {r}")
        if OVERDUE_PRODUCTS.get(grp) != name:
            errs.append(f"PRODUCT_NAME không thuộc nhóm {grp}: {r}")
        if mng not in BRANCH_BY_NBR or (sal not in BRANCH_BY_NBR and sal != ONLINE_SALE_BRANCH):
            errs.append(f"Chi nhánh không hợp lệ: {r}")
        if dg not in OVERDUE_DEL_GROUPS:
            errs.append(f"DEL_GRP lạ: {r}")
    return errs


def card_overdue(cards: list[dict], as_of: date) -> list[list]:
    """Tổng hợp thẻ ACTIVE (trạng thái cuối ngày, sau card_statements) theo
    (hạng thẻ, sản phẩm, CN quản lý, CN bán, nhóm nợ). Quá hạn = DPD > 0."""
    agg: dict[tuple, list] = defaultdict(lambda: [0, 0, 0.0, 0.0])
    for c in cards:
        if c["status"] != "ACTIVE":
            continue
        sal = ONLINE_SALE_BRANCH if c["channel"] == "SMART_CREDIT" else c["branchnbr"]
        a = agg[(c["grade"], c["product"], c["branchnbr"], sal, del_grp(c["dpd"]))]
        a[0] += 1
        a[2] += c["balance"]
        if c["dpd"] > 0:
            a[1] += 1
            a[3] += c["balance"]
    return [[as_of.strftime("%Y%m%d"), "", "Credit Card", k[0], k[1], k[2], k[3], k[4],
             v[0], v[1], f"{v[2]:.2f}", f"{v[3]:.2f}"] for k, v in sorted(agg.items())]
