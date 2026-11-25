"""Sinh dữ liệu core banking GIẢ LẬP (không có data thật) cho project.

Bảng sinh ra (mỗi ngày 1 thư mục as_of_date=YYYY-MM-DD):
    branches.csv, products.csv        : danh mục (ít thay đổi)
    customers.csv, accounts.csv       : snapshot đầy đủ mỗi ngày
    loans.csv                         : snapshot dư nợ cuối ngày
    transactions.json                 : giao dịch phát sinh trong ngày (JSON lines)

Lỗi cài cố ý (để kiểm tra DQ gate) - tỷ lệ nhỏ, cấu hình ở ERROR_RATE:
    customers   : trùng customer_id, customer_id NULL, CCCD sai độ dài, tuổi < 18,
                  ngày sinh sai định dạng (dd/MM/yyyy)
    accounts    : trùng account_id, customer_id không tồn tại, trạng thái lạ
    loans       : dư nợ âm, ngày giải ngân > ngày đáo hạn, nhóm nợ ngoài 1-5,
                  nhóm nợ không khớp DPD, lãi suất > 40%
    transactions: số tiền <= 0, account_id không tồn tại, ngày ở tương lai, trùng txn_id

Chạy:  python -m data_generator.generate --out ./data/landing --start-date 2026-11-01 --days 3
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

ERROR_RATE = 0.02  # ~2% bản ghi bị cài lỗi mỗi loại (dưới ngưỡng dừng Job 5%)

LAST_NAMES = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Huynh", "Vo", "Dang", "Bui", "Do"]
MIDDLE = ["Van", "Thi", "Minh", "Ngoc", "Duc", "Thanh", "Quoc", "Thu"]
FIRST = ["An", "Binh", "Chi", "Dung", "Hà", "Hai", "Lan", "Long", "Mai", "Nam", "Phuong",
         "Quang", "Son", "Trang", "Tuan", "Vy"]
PROVINCES = [("HN", "Ha Noi", "NORTH"), ("HP", "Hai Phong", "NORTH"), ("DN", "Da Nang", "CENTRAL"),
             ("HUE", "Hue", "CENTRAL"), ("HCM", "Ho Chi Minh", "SOUTH"),
             ("CT", "Can Tho", "SOUTH"), ("BD", "Binh Duong", "SOUTH")]
PRODUCTS = [
    # product_id, name, type, term_months, base_rate (%/năm)
    ("LN_HOME", "Home loan", "LOAN", 240, 9.5),
    ("LN_AUTO", "Auto loan", "LOAN", 60, 10.5),
    ("LN_CONS", "Consumer loan", "LOAN", 36, 18.0),
    ("LN_SME", "SME working capital", "LOAN", 12, 8.5),
    ("LN_CARD", "Credit card", "LOAN", 0, 28.0),
    ("DP_CASA", "Current account", "CASA", 0, 0.2),
    ("DP_TD06", "Term deposit 6M", "DEPOSIT", 6, 4.5),
    ("DP_TD12", "Term deposit 12M", "DEPOSIT", 12, 5.2),
]
ACCOUNT_STATUS = ["ACTIVE"] * 8 + ["DORMANT", "CLOSED"]


@dataclass
class Sizes:
    branches: int = 20
    customers: int = 2000
    accounts: int = 3500
    loans: int = 1500
    txns_per_day: int = 5000


def _hit(rnd: random.Random) -> bool:
    return rnd.random() < ERROR_RATE


def _write_csv(path: Path, header: list[str], rows: list[list]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return len(rows)


def gen_branches(n: int) -> list[list]:
    rows = []
    for i in range(1, n + 1):
        code, prov, region = PROVINCES[i % len(PROVINCES)]
        rows.append([f"BR{i:03d}", f"{prov} branch {i:02d}", region, prov,
                     (date(2005, 1, 1) + timedelta(days=150 * i)).isoformat()])
    return rows


def gen_customers(rnd: random.Random, n: int, branch_ids: list[str], as_of: date) -> list[list]:
    rows = []
    for i in range(1, n + 1):
        cid = f"C{i:07d}"
        age = rnd.randint(18, 75)
        dob = as_of - timedelta(days=age * 365 + rnd.randint(0, 364))
        nid = "0" + "".join(str(rnd.randint(0, 9)) for _ in range(11))  # CCCD 12 số
        name = f"{rnd.choice(LAST_NAMES)} {rnd.choice(MIDDLE)} {rnd.choice(FIRST)}"
        phone = "09" + "".join(str(rnd.randint(0, 9)) for _ in range(8))
        segment = rnd.choice(["MASS", "MASS", "MASS", "AFFLUENT", "SME"])
        prov = rnd.choice(PROVINCES)[1]
        dob_s = dob.isoformat()
        if _hit(rnd):
            nid = nid[:10]                       # LỖI WARN: CCCD 10 số
        if _hit(rnd):
            dob_s = (as_of - timedelta(days=15 * 365)).isoformat()  # LỖI WARN: 15 tuổi
        if _hit(rnd):
            dob_s = dob.strftime("%d/%m/%Y")     # LỖI: sai định dạng ngày
        rows.append([cid, name, nid, dob_s, rnd.choice(["M", "F"]), phone,
                     f"{cid.lower()}@example.com", prov, segment, rnd.choice(branch_ids),
                     (as_of - timedelta(days=rnd.randint(30, 3000))).isoformat()])
    # LỖI FAIL: trùng customer_id và customer_id NULL
    for r in rnd.sample(rows, int(n * ERROR_RATE / 2)):
        rows.append(list(r))
    for r in rnd.sample(rows, int(n * ERROR_RATE / 4)):
        bad = list(r)
        bad[0] = ""
        rows.append(bad)
    rnd.shuffle(rows)
    return rows


def evolve_customers(rnd: random.Random, rows: list[list]) -> list[list]:
    """Ngày sau: ~3% khách đổi tỉnh/segment/SĐT -> dùng để test SCD2."""
    out = []
    for r in rows:
        r = list(r)
        if r[0] and rnd.random() < 0.03:
            r[7] = rnd.choice(PROVINCES)[1]
            r[8] = rnd.choice(["MASS", "AFFLUENT", "SME"])
        out.append(r)
    return out


def gen_accounts(rnd: random.Random, n: int, customer_ids: list[str],
                 branch_ids: list[str], as_of: date) -> list[list]:
    rows = []
    for i in range(1, n + 1):
        prod = rnd.choice(PRODUCTS)
        cust = rnd.choice(customer_ids)
        status = rnd.choice(ACCOUNT_STATUS)
        if _hit(rnd):
            cust = f"C9{rnd.randint(0, 999999):06d}"   # LỖI FAIL: khách không tồn tại
        if _hit(rnd):
            status = rnd.choice(["active", "FROZEN", ""])  # LỖI WARN: trạng thái lạ
        bal = 0.0 if prod[2] == "LOAN" else round(rnd.lognormvariate(17, 1.2), 0)
        rows.append([f"A{i:09d}", cust, prod[0], rnd.choice(branch_ids),
                     (as_of - timedelta(days=rnd.randint(1, 2500))).isoformat(),
                     status, "VND", bal])
    for r in rnd.sample(rows, int(n * ERROR_RATE / 2)):  # LỖI FAIL: trùng account_id
        rows.append(list(r))
    rnd.shuffle(rows)
    return rows


def _dg(dpd: int) -> int:
    """Nhóm nợ theo DPD - trùng quy tắc src/common/banking.py (TT 11/2021).
    Không import để generator chạy độc lập, không cần pyspark."""
    for upper, g in [(10, 1), (90, 2), (180, 3), (360, 4)]:
        if dpd < upper:
            return g
    return 5


def _random_dpd(rnd: random.Random) -> int:
    # Phân phối gần thực tế: ~90% nhóm 1, NPL khoảng 2-4%
    x = rnd.random()
    if x < 0.90:
        return rnd.randint(0, 9)
    if x < 0.97:
        return rnd.randint(10, 89)
    if x < 0.985:
        return rnd.randint(90, 179)
    if x < 0.995:
        return rnd.randint(180, 359)
    return rnd.randint(360, 700)


def gen_loans(rnd: random.Random, n: int, loan_accounts: list[list], as_of: date,
              state: dict[str, dict] | None = None) -> tuple[list[list], dict[str, dict]]:
    """Snapshot dư nợ cuối ngày. state giữ thông tin khoản vay để ngày sau nhất quán
    (DPD tăng/giảm, dư nợ giảm dần)."""
    state = state or {}
    rows = []
    for i in range(1, n + 1):
        lid = f"L{i:08d}"
        if lid not in state:
            acc = rnd.choice(loan_accounts)
            prod = next(p for p in PRODUCTS if p[0] == acc[2])
            disb = as_of - timedelta(days=rnd.randint(30, 1500))
            term = prod[3] or 12
            principal = round(rnd.lognormvariate(19.5, 1.0), -5)
            state[lid] = {"acc": acc, "disb": disb,
                          "mat": disb + timedelta(days=30 * term), "principal": principal,
                          "rate": round(prod[4] + rnd.uniform(-1, 3), 2),
                          "dpd": _random_dpd(rnd),
                          "bal": round(principal * rnd.uniform(0.2, 1.0), 0)}
        else:
            s = state[lid]
            # Ngày mới: khách trả nợ (DPD về 0) hoặc tiếp tục quá hạn (DPD + 1)
            s["dpd"] = 0 if (s["dpd"] < 10 and rnd.random() < 0.7) else s["dpd"] + 1
            s["bal"] = max(0.0, round(s["bal"] * 0.998, 0))
        s = state[lid]
        acc = s["acc"]
        bal, disb_s, dg, rate = s["bal"], s["disb"].isoformat(), _dg(s["dpd"]), s["rate"]
        if _hit(rnd):
            bal = -abs(bal)                                   # LỖI FAIL: dư nợ âm
        if _hit(rnd):
            disb_s = (s["mat"] + timedelta(days=10)).isoformat()  # LỖI FAIL: giải ngân > đáo hạn
        if _hit(rnd):
            dg = rnd.choice([0, 6])                           # LỖI FAIL: nhóm nợ ngoài 1-5
        elif _hit(rnd):
            dg = 1 if dg > 1 else 3                           # LỖI WARN: nhóm nợ lệch DPD
        if _hit(rnd):
            rate = 45.0                                       # LỖI WARN: lãi suất > 40%
        rows.append([lid, acc[0], acc[1], acc[2], acc[3], disb_s, s["mat"].isoformat(),
                     s["principal"], bal, rate, s["dpd"], dg, "VND", as_of.isoformat()])
    return rows, state


def gen_transactions(rnd: random.Random, n: int, account_ids: list[str], as_of: date) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        ts = datetime.combine(as_of, datetime.min.time()) + timedelta(
            seconds=rnd.randint(0, 86399))
        amount = round(rnd.lognormvariate(14, 1.5), 0)
        acc = rnd.choice(account_ids)
        if _hit(rnd):
            amount = rnd.choice([0, -amount])                 # LỖI FAIL: số tiền <= 0
        if _hit(rnd):
            acc = f"A9{rnd.randint(0, 99999999):08d}"         # LỖI FAIL: tài khoản không tồn tại
        if _hit(rnd):
            ts = ts + timedelta(days=400)                     # LỖI FAIL: ngày ở tương lai
        rows.append({
            "txn_id": f"T{as_of:%Y%m%d}{i:07d}",
            "account_id": acc,
            "txn_ts": ts.isoformat(sep=" "),
            "amount": amount,
            "currency": "VND",
            "txn_type": rnd.choice(["DEBIT", "CREDIT"]),
            "channel": rnd.choice(["ATM", "MOBILE", "BRANCH", "INTERNET", "POS"]),
            "description": rnd.choice(["Transfer", "Bill payment", "Cash withdrawal",
                                       "Salary", "Loan repayment", "Card payment"]),
        })
    for r in rnd.sample(rows, int(n * ERROR_RATE / 2)):     # LỖI FAIL: trùng txn_id
        rows.append(dict(r))
    rnd.shuffle(rows)
    return rows


HEADERS = {
    "branches": ["branch_id", "branch_name", "region", "province", "open_date"],
    "products": ["product_id", "product_name", "product_type", "term_months", "base_rate"],
    "customers": ["customer_id", "full_name", "national_id", "date_of_birth", "gender",
                  "phone", "email", "province", "segment", "home_branch_id", "created_date"],
    "accounts": ["account_id", "customer_id", "product_id", "branch_id", "open_date",
                 "status", "currency", "balance"],
    "loans": ["loan_id", "account_id", "customer_id", "product_id", "branch_id",
              "disbursement_date", "maturity_date", "principal_amount", "outstanding_balance",
              "interest_rate", "dpd", "debt_group", "currency", "as_of_date"],
}


def generate(out: Path, start: date, days: int, seed: int = 42,
             sizes: Sizes | None = None) -> dict[str, int]:
    """Sinh dữ liệu `days` ngày liên tiếp. Trả về số dòng mỗi bảng (cộng dồn)."""
    sizes = sizes or Sizes()
    rnd = random.Random(seed)  # seed cố định: chạy lại ra đúng bộ data cũ
    counts: dict[str, int] = {}

    def add(table: str, n: int) -> None:
        counts[table] = counts.get(table, 0) + n

    branches = gen_branches(sizes.branches)
    branch_ids = [b[0] for b in branches]
    customers = gen_customers(rnd, sizes.customers, branch_ids, start)
    cust_ids = sorted({c[0] for c in customers if c[0]})
    accounts = gen_accounts(rnd, sizes.accounts, cust_ids, branch_ids, start)
    loan_accounts = [a for a in accounts if a[2].startswith("LN_")]
    acc_ids = sorted({a[0] for a in accounts})
    loan_state: dict[str, dict] = {}

    for d in range(days):
        as_of = start + timedelta(days=d)
        part = f"as_of_date={as_of.isoformat()}"
        if d == 0:
            add("branches", _write_csv(out / "branches" / part / "branches.csv",
                                       HEADERS["branches"], branches))
            add("products", _write_csv(out / "products" / part / "products.csv",
                                       HEADERS["products"], [list(p) for p in PRODUCTS]))
        else:
            customers = evolve_customers(rnd, customers)
        add("customers", _write_csv(out / "customers" / part / "customers.csv",
                                    HEADERS["customers"], customers))
        add("accounts", _write_csv(out / "accounts" / part / "accounts.csv",
                                   HEADERS["accounts"], accounts))
        loans, loan_state = gen_loans(rnd, sizes.loans, loan_accounts, as_of, loan_state)
        add("loans", _write_csv(out / "loans" / part / "loans.csv", HEADERS["loans"], loans))
        txns = gen_transactions(rnd, sizes.txns_per_day, acc_ids, as_of)
        p = out / "transactions" / part / "transactions.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text("\n".join(json.dumps(t, ensure_ascii=False) for t in txns) + "\n",
                     encoding="utf-8")
        add("transactions", len(txns))
    return counts


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate fake core-banking data")
    ap.add_argument("--out", default="data/landing")
    ap.add_argument("--start-date", default="2026-11-01")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    counts = generate(Path(a.out), date.fromisoformat(a.start_date), a.days, a.seed)
    for t, n in counts.items():
        print(f"{t:<13} {n:>7} rows")


if __name__ == "__main__":
    main()
