"""Core banking: chi nhánh, sản phẩm, khách hàng, tài khoản, khoản vay (layout duno), giao dịch.

Mỗi hàm `base_*` tạo thực thể gốc (ổn định qua các ngày).
Mỗi hàm `snap_*` xuất snapshot 1 ngày, có cài lỗi cố ý.
"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta

from data_generator.common import (
    BRANCHES,
    DEPOSIT_PRODUCTS,
    DN,
    ERROR_RATE,
    LOAN_PRODUCTS,
    LV,
    TEN,
    USD_RATE,
    debt_group,
    hit,
    mdy,
    person,
)

HEADERS = {
    "branches": ["branchnbr", "br_area", "br_name", "region", "province", "open_date"],
    "products": ["prodcode", "product_name", "product_family", "mjacctcd", "miacctcd",
                 "product_group", "scope", "term_months"],
    "customers": ["customer_id", "cust_type", "full_name", "id_no", "dob", "gender", "phone",
                  "email", "address", "province", "branchnbr", "segment", "monthly_income",
                  "open_date", "updated_at"],
    "accounts": ["account_id", "customer_id", "branchnbr", "product_type", "currencycd",
                 "status", "open_date", "close_date", "balance", "int_rate"],
    "loans": ["etl_dt", "br_area", "branchnbr", "br_name", "acctnbr", "customer_id", "scope",
              "mjacctcd", "miacctcd", "prodcode", "tensp", "nhomsp", "currencycd", "int_rate",
              "datestart", "datemat", "loan_dur", "dudau", "chovay", "thuno", "thulai", "ducuoi",
              "ducuoiqd", "songay_nqh", "nhom_no", "noqh", "offnbr", "offname", "ftp_rate",
              "acctstatcd", "acctsiba", "nextdue"],
}
SEGMENTS = ["MASS", "MASS", "MASS", "AFFLUENT", "PRIORITY", "SME"]
DESC = ["CK noi bo", "Nop tien mat tai quay", "Rut tien ATM", "Thanh toan hoa don dien",
        "Tra no vay ky {k}", "TTQT Thu phi LCN - PH LC so {r}", "Chuyen tien NAPAS 247 den {n}",
        "Thu lai tien gui ky han", "Phi duy tri tai khoan"]


# ---------- danh mục ----------
def branch_rows() -> list[list]:
    return [[b[0], b[1], b[2], b[3], b[4], b[5].isoformat()] for b in BRANCHES]


def product_rows() -> list[list]:
    rows = [[p[0], p[3], "LOAN", p[1], p[2], p[4], p[5], p[6]] for p in LOAN_PRODUCTS]
    rows += [[d[0], d[1], "DEPOSIT", "", "", d[2], "ALL", 0] for d in DEPOSIT_PRODUCTS]
    return rows


# ---------- khách hàng ----------
def base_customers(rnd: random.Random, n: int, as_of: date) -> list[dict]:
    out = []
    for i in range(n):
        org = rnd.random() < 0.10  # 10% KH tổ chức
        br = rnd.choice(BRANCHES)
        seg = "SME" if org else rnd.choice(SEGMENTS[:-1])
        income = round(rnd.lognormvariate(16.6 if seg == "MASS" else 17.6, 0.6), -5)
        out.append({
            "customer_id": str(1_000_000 + i), "cust_type": "TC" if org else "CN",
            "full_name": (f"{rnd.choice(DN)} {rnd.choice(LV)} {rnd.choice(TEN)} "
                          f"{rnd.choice(TEN)}" if org else person(rnd))[:50],
            "id_no": "".join(rnd.choices("0123456789", k=12 if rnd.random() < 0.85 else 9)),
            "dob": as_of - timedelta(days=rnd.randint(20 * 365, 70 * 365)),
            "gender": rnd.choice("MF"),
            "phone": "09" + "".join(rnd.choices("0123456789", k=8)),
            "email": f"user{i}@example.com",
            "address": f"{rnd.randint(1, 999)} DUONG SO {rnd.randint(1, 50)}",
            "province": br[4], "branchnbr": br[0], "segment": seg,
            "monthly_income": 0 if org else income,
            "open_date": as_of - timedelta(days=rnd.randint(30, 4000)),
        })
    return out


def evolve_customers(rnd: random.Random, customers: list[dict], rate: float = 0.03) -> int:
    """Ngày mới: ~3% KH đổi tỉnh / segment / SĐT -> để test SCD2. Trả về số KH thay đổi."""
    changed = 0
    for c in customers:
        if rnd.random() < rate:
            what = rnd.choice(["province", "segment", "phone"])
            if what == "province":
                c["province"] = rnd.choice([b[4] for b in BRANCHES if b[4] != c["province"]])
            elif what == "segment" and c["cust_type"] == "CN":
                c["segment"] = rnd.choice([s for s in SEGMENTS[:-1] if s != c["segment"]])
            else:
                c["phone"] = "09" + "".join(rnd.choices("0123456789", k=8))
            changed += 1
    return changed


def snap_customers(rnd: random.Random, base: list[dict], as_of: date) -> list[list]:
    rows = []
    for c in base:
        r = [c["customer_id"], c["cust_type"], c["full_name"], c["id_no"], mdy(c["dob"]),
             c["gender"], c["phone"], c["email"], c["address"], c["province"], c["branchnbr"],
             c["segment"], f"{c['monthly_income']:.0f}", mdy(c["open_date"]),
             f"{as_of.isoformat()} 00:00:00"]
        if hit(rnd):
            r[3] = r[3][:7]  # CCCD sai độ dài (WARN)
        if hit(rnd):
            r[4] = mdy(as_of - timedelta(days=rnd.randint(1, 15 * 365)))  # tuổi < 18 (WARN)
        if hit(rnd, ERROR_RATE / 4):
            r[4] = c["dob"].strftime("%Y-%m-%d")  # ngày sinh sai định dạng
        if hit(rnd, ERROR_RATE / 4):
            r[0] = ""  # customer_id NULL (FAIL)
        rows.append(r)
    rows += rnd.sample(rows, max(1, len(rows) // 200))  # trùng customer_id (FAIL)
    rnd.shuffle(rows)
    return rows


# ---------- tài khoản ----------
def base_deposit_accounts(rnd: random.Random, n: int, customers: list[dict],
                          as_of: date) -> list[dict]:
    out = []
    for i in range(n):
        c = rnd.choice(customers)
        st = rnd.choices(["ACTIVE", "CLOSED", "DORMANT"], [85, 8, 7])[0]
        od = as_of - timedelta(days=rnd.randint(1, 3000))
        prod = rnd.choices(DEPOSIT_PRODUCTS, [35, 20, 15, 30])[0]
        cur = rnd.choices(["VND", "USD"], [95, 5])[0]
        bal = round(rnd.lognormvariate(17, 1.8), -3)  # VND
        if cur == "USD":
            bal = round(bal / USD_RATE, 2)  # TK ngoại tệ lưu số dư theo USD
        out.append({
            "account_id": str(200_000_000 + i), "customer_id": c["customer_id"],
            "branchnbr": c["branchnbr"], "product_type": prod[0],
            "currencycd": cur, "status": st,
            "open_date": od,
            "close_date": od + timedelta(days=rnd.randint(30, 900)) if st == "CLOSED" else None,
            "balance": 0.0 if st == "CLOSED" else bal,
            "int_rate": rnd.uniform(0.001, 0.075),
        })
    return out


def snap_accounts(rnd: random.Random, accounts: list[dict]) -> tuple[list[list], set[str]]:
    """Trả về (rows, account_id bị lỗi FAIL) - FAIL sẽ vào quarantine, không vào Gold."""
    rows, failed = [], set()
    for i, a in enumerate(accounts):
        if a["product_type"] != "LOAN" and a["balance"]:
            a["balance"] = round(a["balance"] * rnd.uniform(0.98, 1.025), 2)
        r = [a["account_id"], a["customer_id"], a["branchnbr"], a["product_type"],
             a["currencycd"], a["status"], mdy(a["open_date"]),
             mdy(a["close_date"]) if a["close_date"] else "", f"{a['balance']:.2f}",
             f"{a['int_rate']:.5f}"]
        if hit(rnd):
            r[5] = rnd.choice(["active", "ACTV", "FROZEN"])  # trạng thái lạ (WARN)
        if hit(rnd, ERROR_RATE / 2):
            r[1] = str(9_000_000 + i)  # customer_id không tồn tại (FAIL)
            failed.add(a["account_id"])
        rows.append(r)
    rows += rnd.sample(rows, max(1, len(rows) // 200))  # trùng account_id (FAIL)
    return rows, failed


# ---------- khoản vay (layout file duno) ----------
def _random_dpd(rnd: random.Random) -> int:
    # ~91% nhóm 1, NPL (nhóm 3-5) ~3%
    bucket = rnd.choices([0, 1, 2, 3, 4, 5], [85, 6, 6, 1.5, 1, 0.5])[0]
    return [0, rnd.randint(1, 9), rnd.randint(10, 89), rnd.randint(90, 179),
            rnd.randint(180, 359), rnd.randint(360, 1500)][bucket]


def base_loans(rnd: random.Random, n: int, customers: list[dict], as_of: date,
               first_acct: int) -> tuple[list[dict], list[dict]]:
    """Mỗi khoản vay có 1 tài khoản LOAN trong accounts (acctnbr = account_id)."""
    loans, loan_accounts = [], []
    for i in range(n):
        c = rnd.choice(customers)
        pr = rnd.choice([p for p in LOAN_PRODUCTS if p[5] == c["cust_type"]])
        start = as_of - timedelta(days=rnd.randint(30, 2500))
        dur = pr[6]
        cur = "USD" if c["cust_type"] == "TC" and rnd.random() < 0.1 else "VND"
        bal = round(rnd.lognormvariate(19.5 if pr[5] == "CN" else 21, 1.2), -3)
        acct = str(first_acct + i)
        loans.append({
            "branch": next(b for b in BRANCHES if b[0] == c["branchnbr"]), "acctnbr": acct,
            "customer_id": c["customer_id"], "product": pr, "currencycd": cur,
            "int_rate": rnd.uniform(0.045, 0.16), "datestart": start,
            "datemat": start + timedelta(days=dur * 30), "loan_dur": dur,
            "balance": bal if cur == "VND" else round(bal / USD_RATE, 2),
            "dpd": _random_dpd(rnd), "offnbr": str(6_000_000 + rnd.randint(0, 9999)),
            "offname": person(rnd), "ftp_rate": rnd.uniform(0.03, 0.08),
            "acctsiba": f"{c['province'][:3]}.{'DN' if pr[5] == 'TC' else 'CN'}."
                        f"{rnd.randint(1, 999)}.{start:%d%m%y}",
        })
        loan_accounts.append({
            "account_id": acct, "customer_id": c["customer_id"], "branchnbr": c["branchnbr"],
            "product_type": "LOAN", "currencycd": cur, "status": "ACTIVE", "open_date": start,
            "close_date": None, "balance": 0.0, "int_rate": loans[-1]["int_rate"],
        })
    return loans, loan_accounts


def snap_loans(rnd: random.Random, base: list[dict], as_of: date) -> list[list]:
    rows = []
    for ln in base:
        # Diễn biến trong ngày: thu nợ gốc, giải ngân thêm, DPD tăng/giảm
        dudau = ln["balance"]
        chovay = round(dudau * rnd.choice([0] * 19 + [0.05]), 2)
        thuno = -round(dudau * rnd.choice([0] * 17 + [0.01, 0.02, 0.03]), 2)
        ducuoi = round(dudau + chovay + thuno, 2)
        ln["balance"] = ducuoi
        if ln["dpd"] > 0:
            ln["dpd"] = 0 if (ln["dpd"] < 30 and rnd.random() < 0.3) else ln["dpd"] + 1
        elif rnd.random() < 0.003:
            ln["dpd"] = 1
        fx = USD_RATE if ln["currencycd"] == "USD" else 1
        br, pr, dpd = ln["branch"], ln["product"], ln["dpd"]
        grp = debt_group(dpd)
        r = [mdy(as_of), br[1], br[0], br[2], ln["acctnbr"], ln["customer_id"], pr[5],
             pr[1], pr[2], pr[0], pr[3], pr[4], ln["currencycd"], f"{ln['int_rate']:.5f}",
             mdy(ln["datestart"]) + " 00:00:00", mdy(ln["datemat"]) + " 00:00:00",
             ln["loan_dur"], f"{dudau:.2f}", f"{chovay:.2f}", f"{thuno:.2f}",
             f"{-round(ducuoi * 0.008, 0):.2f}", f"{ducuoi:.2f}", f"{ducuoi * fx:.2f}",
             dpd, grp, "Y" if dpd > 0 else "N", ln["offnbr"], ln["offname"],
             f"{ln['ftp_rate']:.5f}", "NPFM" if grp >= 3 else "ACT", ln["acctsiba"],
             mdy(as_of + timedelta(days=rnd.randint(1, 30)))]
        if hit(rnd):
            r[21] = f"{-abs(ducuoi):.2f}"  # dư nợ âm (FAIL)
        if hit(rnd):
            r[14], r[15] = r[15], r[14]  # ngày giải ngân > ngày đáo hạn (FAIL)
        if hit(rnd, ERROR_RATE / 2):
            r[24] = rnd.choice([0, 6, 9])  # nhóm nợ ngoài 1-5 (FAIL)
        elif hit(rnd):
            r[24] = grp - 1 if grp > 1 else 2  # nhóm nợ lệch DPD (WARN)
        if hit(rnd, ERROR_RATE / 2):
            r[13] = f"{rnd.uniform(0.45, 1.2):.5f}"  # lãi suất > 40%/năm (WARN)
        if hit(rnd, ERROR_RATE / 4):
            r[14] = ln["datestart"].strftime("%d-%b-%Y")  # ngày sai định dạng
        if hit(rnd, ERROR_RATE / 4):
            r[21] = ""  # dư nợ NULL
        rows.append(r)
    rows += rnd.sample(rows, max(1, len(rows) // 200))  # trùng acctnbr
    return rows


# ---------- giao dịch tài khoản ----------
def gen_transactions(rnd: random.Random, n: int, acc_ids: list[str], as_of: date) -> list[dict]:
    day0 = datetime.combine(as_of, datetime.min.time())
    out = []
    for i in range(n):
        ts = day0 + timedelta(seconds=rnd.randint(0, 86399))
        amt = round(rnd.lognormvariate(14, 1.7), -3) or 1000.0
        d = rnd.choice(DESC).format(k=rnd.randint(1, 60), r=f"PTH{rnd.randint(10**8, 10**9)}",
                                    n=person(rnd))
        t = {"txn_id": f"TXN{as_of:%Y%m%d}{i:09d}", "account_id": rnd.choice(acc_ids),
             "txn_ts": ts.strftime("%Y-%m-%d %H:%M:%S"), "value_date": as_of.isoformat(),
             "amount": amt, "dr_cr": rnd.choice(["D", "C"]), "currencycd": "VND",
             "channel": rnd.choice(["TELLER", "ATM", "IB", "MB", "POS", "BATCH"]),
             "txn_type": rnd.choice(["TRF", "CASH", "FEE", "INT", "LOANPMT"]),
             "description": d[:182], "teller_user": f"USR{rnd.randint(1, 800):04d}",
             "branchnbr": rnd.choice(BRANCHES)[0]}
        if hit(rnd):
            t["amount"] = rnd.choice([0.0, -amt])  # số tiền <= 0 (FAIL)
        if hit(rnd, ERROR_RATE / 2):
            t["account_id"] = str(299_000_000 + i)  # account_id không tồn tại (FAIL)
        if hit(rnd, ERROR_RATE / 4):
            t["txn_ts"] = (ts + timedelta(days=rnd.randint(2, 30))).strftime("%Y-%m-%d %H:%M:%S")
        if hit(rnd, ERROR_RATE / 4):
            t["txn_ts"] = ts.strftime("%d/%m/%Y %H:%M")  # ngày sai định dạng
        if i and hit(rnd, ERROR_RATE / 4):
            t["txn_id"] = f"TXN{as_of:%Y%m%d}{rnd.randint(0, i - 1):09d}"  # trùng txn_id (FAIL)
        out.append(t)
    return out
