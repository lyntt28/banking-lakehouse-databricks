"""Thẻ tín dụng + bảo hiểm (bán chéo).

- cards            : snapshot thẻ mỗi ngày (mở mới / đóng thẻ thay đổi theo ngày)
- card_transactions: giao dịch thẻ (MCC, merchant, kênh POS/ONLINE, trong nước/nước ngoài)
- card_statements  : dư nợ thẻ cuối ngày, dư nợ quay vòng, DPD
- insurance_policies: hợp đồng bảo hiểm KH đã mua -> nhãn (label) cho bài toán ML cross-sell

Hành vi được "cài" có quy luật để model ML học được: KH thu nhập cao, thẻ hạng cao,
hay chi tiêu du lịch / y tế / giáo dục thì xác suất mua bảo hiểm cao hơn.
"""

from __future__ import annotations

import math
import random
from datetime import date, datetime, timedelta

from data_generator.common import (
    CARD_PRODUCTS,
    ERROR_RATE,
    GRADE_RANK,
    INSURANCE_PRODUCTS,
    MCC,
    hit,
)

HEADERS = {
    "cards": ["card_no", "customer_id", "cust_type", "branchnbr", "card_product", "card_grade",
              "credit_limit", "open_date", "status", "close_date", "snapshot_date",
              "acquisition_channel"],
    "card_statements": ["stnd_dt", "card_no", "outstanding_balance", "revolving_balance",
                        "min_payment_due", "dpd", "credit_limit"],
    "insurance_policies": ["policy_id", "customer_id", "product_code", "start_date",
                           "annual_premium", "status", "sold_channel"],
}
TXN_TYPES = ["PURCHASE", "CASH_ADVANCE", "INSTALLMENT", "FLEXI"]
LIFESTYLE_MCC = {"4511", "7011", "8062", "8220"}  # du lịch / y tế / giáo dục


def _new_card(rnd: random.Random, seq: int, c: dict, as_of: date, opened: date) -> dict:
    if c["cust_type"] == "TC":
        prod = CARD_PRODUCTS[4]
    else:
        # thu nhập cao -> thẻ hạng cao
        tier = min(3, max(0, int(math.log(max(c["monthly_income"], 1e6) / 1e7, 2.2) + 0.5)))
        prod = CARD_PRODUCTS[min(3, max(0, tier + rnd.choice([-1, 0, 0, 1])))]
    return {
        "card_no": f"4{seq:015d}", "customer_id": c["customer_id"], "cust_type": c["cust_type"],
        "branchnbr": c["branchnbr"], "product": prod[0], "grade": prod[1],
        "limit": round(rnd.uniform(prod[2], prod[3]), -6), "open_date": opened,
        "status": "ACTIVE", "close_date": None,
        # kênh phát hành: ~40% Smart Credit (online), còn lại tại chi nhánh
        "channel": "SMART_CREDIT" if seq % 5 < 2 else "BRANCH",
        "activity": rnd.betavariate(2, 3),  # mức độ hay dùng thẻ
        "lifestyle": rnd.random() < 0.25,   # hay chi du lịch / y tế / giáo dục
        "revolver": rnd.random() < 0.3,     # hay trả tối thiểu -> dư nợ quay vòng
        "balance": 0.0, "dpd": 0,
    }


def base_cards(rnd: random.Random, customers: list[dict], as_of: date) -> list[dict]:
    cards = []
    for c in customers:
        if rnd.random() < (0.45 if c["cust_type"] == "CN" else 0.25):
            card = _new_card(rnd, len(cards), c, as_of,
                             as_of - timedelta(days=rnd.randint(30, 2500)))
            card["balance"] = round(card["limit"] * rnd.uniform(0, 0.5), -3)
            card["dpd"] = rnd.choices([0, rnd.randint(1, 9), rnd.randint(10, 29),
                                       rnd.randint(30, 89), rnd.randint(90, 400)],
                                      [88, 5, 3, 2, 2])[0]
            cards.append(card)
    return cards


def evolve_cards(rnd: random.Random, cards: list[dict], customers: list[dict],
                 as_of: date) -> None:
    """Ngày mới: mở thẻ mới (~0.15%) và đóng thẻ (~0.05%)."""
    for card in cards:
        if card["status"] == "ACTIVE" and rnd.random() < 0.0005:
            card["status"], card["close_date"] = "CLOSED", as_of
            card["balance"], card["dpd"] = 0.0, 0
    for _ in range(max(1, int(len(cards) * 0.0015))):
        cards.append(_new_card(rnd, len(cards), rnd.choice(customers), as_of, as_of))


def snap_cards(rnd: random.Random, cards: list[dict], as_of: date) -> list[list]:
    rows = [[c["card_no"], c["customer_id"], c["cust_type"], c["branchnbr"], c["product"],
             c["grade"], f"{c['limit']:.0f}", c["open_date"].isoformat(), c["status"],
             c["close_date"].isoformat() if c["close_date"] else "", as_of.isoformat(),
             c["channel"]]
            for c in cards]
    rows += rnd.sample(rows, max(1, len(rows) // 300))  # trùng card_no (Silver phải bỏ trùng)
    return rows


def gen_card_transactions(rnd: random.Random, n: int, cards: list[dict],
                          as_of: date) -> tuple[list[dict], list[dict]]:
    """Trả về (tất cả giao dịch có lỗi cài cố ý, giao dịch sạch dùng tính KPI)."""
    # Trong ngày chỉ một phần thẻ có phát sinh giao dịch (tỷ lệ sử dụng ~25%)
    active = [c for c in cards if c["status"] == "ACTIVE" and rnd.random() < c["activity"] * 0.6]
    active = active or [c for c in cards if c["status"] == "ACTIVE"]
    weights = [c["activity"] for c in active]
    day0 = datetime.combine(as_of, datetime.min.time())
    out, clean = [], []
    for i in range(n):
        c = rnd.choices(active, weights)[0]
        ttype = rnd.choices(TXN_TYPES, [80, 4, 10, 6])[0]
        if ttype == "CASH_ADVANCE":
            mcc = MCC[-1]
        elif c["lifestyle"] and rnd.random() < 0.4:
            mcc = rnd.choice([m for m in MCC if m[0] in LIFESTYLE_MCC])
        else:
            mcc = rnd.choice(MCC[:-1])
        amt = round(rnd.lognormvariate(13.5 + GRADE_RANK[c["grade"]] * 0.3, 1.1), -3) or 1000.0
        if ttype == "INSTALLMENT":
            amt = round(amt * 5, -3)
        xborder = mcc[0] in ("4511", "7011") and rnd.random() < 0.3 or rnd.random() < 0.03
        t = {"txn_id": f"CTX{as_of:%Y%m%d}{i:08d}", "card_no": c["card_no"],
             "txn_ts": (day0 + timedelta(seconds=rnd.randint(0, 86399))).strftime(
                 "%Y-%m-%d %H:%M:%S"),
             "amount": amt, "currencycd": "VND", "txn_type": ttype, "mcc": mcc[0],
             "merchant_name": rnd.choice(mcc[2]),
             "channel": "ONLINE" if (mcc[0] in ("5999", "4511") or rnd.random() < 0.1)
             else "POS",
             "location": "XBORDER" if xborder else "DOMESTIC"}
        bad = False
        if hit(rnd):
            t["amount"], bad = rnd.choice([0.0, -amt]), True  # số tiền <= 0 (FAIL)
        elif hit(rnd, ERROR_RATE / 2):
            t["card_no"], bad = f"9{i:015d}", True  # thẻ không tồn tại (FAIL)
        elif i and hit(rnd, ERROR_RATE / 4):
            # trùng txn_id (FAIL) - bản ghi gốc vẫn hợp lệ
            t["txn_id"], bad = out[rnd.randint(0, i - 1)]["txn_id"], True
        out.append(t)
        if not bad:
            clean.append(t)
            c["balance"] = min(c["limit"] * 1.1, c["balance"] + amt)
    return out, clean


def snap_card_statements(rnd: random.Random, cards: list[dict], as_of: date) -> list[list]:
    rows = []
    for c in cards:
        if c["status"] != "ACTIVE":
            continue
        # cuối ngày: KH trả bớt nợ, DPD thay đổi
        if not c["revolver"] and rnd.random() < 0.05:
            c["balance"] = 0.0
        if c["dpd"] > 0:
            c["dpd"] = 0 if (c["dpd"] < 30 and rnd.random() < 0.2) else c["dpd"] + 1
        elif c["revolver"] and c["balance"] > 0 and rnd.random() < 0.002:
            c["dpd"] = 1
        bal = c["balance"] = round(c["balance"], 0)
        rev = bal if c["revolver"] else 0.0
        r = [as_of.isoformat(), c["card_no"], f"{bal:.0f}", f"{rev:.0f}",
             f"{round(bal * 0.05, -3):.0f}", c["dpd"], f"{c['limit']:.0f}"]
        if hit(rnd, ERROR_RATE / 2):
            r[6] = "0"  # hạn mức = 0 trong khi có dư nợ (WARN)
        rows.append(r)
    return rows


def _buy_prob(c: dict, cust_cards: list[dict]) -> float:
    if c["cust_type"] != "CN":
        return 0.0
    rank = max((GRADE_RANK[k["grade"]] for k in cust_cards), default=0)
    life = any(k["lifestyle"] for k in cust_cards)
    z = (-3.2 + 0.9 * math.log(max(c["monthly_income"], 1e6) / 2e7)
         + 0.45 * rank + 0.9 * life + 0.4 * (c["segment"] in ("AFFLUENT", "PRIORITY")))
    return 1 / (1 + math.exp(-z))


def base_insurance(rnd: random.Random, customers: list[dict], cards: list[dict],
                   as_of: date) -> list[dict]:
    by_cust: dict[str, list[dict]] = {}
    for k in cards:
        by_cust.setdefault(k["customer_id"], []).append(k)
    pols = []
    for c in customers:
        if rnd.random() < _buy_prob(c, by_cust.get(c["customer_id"], [])) * 0.6:
            pols.append(_policy(rnd, len(pols), c,
                                as_of - timedelta(days=rnd.randint(1, 1500))))
    return pols


def _policy(rnd: random.Random, seq: int, c: dict, start: date) -> dict:
    p = rnd.choice(INSURANCE_PRODUCTS)
    return {"policy_id": f"POL{seq:08d}", "customer_id": c["customer_id"], "product": p[0],
            "start_date": start, "premium": round(rnd.uniform(p[1], p[2]), -5),
            "status": rnd.choices(["INFORCE", "LAPSED"], [9, 1])[0],
            "channel": rnd.choice(["BRANCH", "BRANCH", "TELESALE", "ONLINE"])}


def evolve_insurance(rnd: random.Random, pols: list[dict], customers: list[dict],
                     cards: list[dict], as_of: date) -> None:
    """Mỗi ngày có thêm hợp đồng mới - theo cùng quy luật xác suất (để làm label ML)."""
    by_cust: dict[str, list[dict]] = {}
    for k in cards:
        by_cust.setdefault(k["customer_id"], []).append(k)
    for c in rnd.sample(customers, max(1, len(customers) // 50)):
        if rnd.random() < _buy_prob(c, by_cust.get(c["customer_id"], [])) * 0.05:
            pols.append(_policy(rnd, len(pols), c, as_of))


def snap_insurance(rnd: random.Random, pols: list[dict]) -> list[list]:
    rows = [[p["policy_id"], p["customer_id"], p["product"], p["start_date"].isoformat(),
             f"{p['premium']:.0f}", p["status"], p["channel"]] for p in pols]
    for r in rows:
        if hit(rnd, ERROR_RATE / 2):
            r[1] = ""  # customer_id NULL (FAIL)
    return rows
