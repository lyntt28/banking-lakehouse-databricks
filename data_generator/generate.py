"""Sinh dữ liệu core banking GIẢ LẬP (không có thông tin KH thật).

Cấu trúc và độ dài cột mô phỏng file raw của core banking Việt Nam:
    loans.csv        : giống file "du no" (duno) - tên cột tiếng Việt viết tắt
                       (ducuoi, nhom_no, songay_nqh, acctsiba...), ngày dạng MM/DD/YYYY
    transactions.json: giống liệt kê giao dịch (mô tả dài tối đa 182 ký tự), JSON lines
    customers.csv, accounts.csv: snapshot đầy đủ mỗi ngày

Mỗi ngày 1 thư mục: <out>/<table>/as_of_date=YYYY-MM-DD/<table>.<csv|json>

Lỗi cài cố ý (để kiểm tra DQ gate), tỷ lệ ~ERROR_RATE mỗi loại:
    customers   : trùng/NULL customer_id, CCCD sai độ dài, tuổi < 18, ngày sinh sai định dạng
    accounts    : trùng account_id, customer_id không tồn tại, trạng thái lạ
    loans       : dư nợ âm/NULL, ngày giải ngân > đáo hạn, nhóm nợ ngoài 1-5,
                  nhóm nợ lệch DPD, lãi suất > 40%, ngày sai định dạng, trùng acctnbr
    transactions: số tiền <= 0, account_id không tồn tại, ngày ở tương lai,
                  ngày sai định dạng, trùng txn_id

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

HO = "NGUYEN TRAN LE PHAM HOANG HUYNH PHAN VU VO DANG BUI DO HO NGO DUONG LY".split()
DEM = "VAN THI MINH NGOC THANH HOANG QUOC DUC KIM BAO GIA XUAN".split()
TEN = ("AN BINH CHI DUNG GIANG HA HAI HANH HIEU HOA HUNG KHANH LAN LINH LONG MAI NAM NGA "
       "PHONG PHUONG QUAN SON TAM THAO THU TRANG TUAN VY YEN").split()
DN = ["CTY TNHH", "CTY CP", "DNTN", "CTY TNHH MTV", "HTX"]
LV = ["TM", "SX", "XD", "DV", "VT", "TM & DV", "XNK"]
# (branchnbr, br_area, br_name) - chi nhánh hư cấu
BRANCHES = [
    (1, "A1.HCMC", "BANK - CN SAI GON"), (2, "A1.HCMC", "BANK - CN TP.HO CHI MINH"),
    (101, "A2.HNOI", "BANK - CN HA NOI"), (102, "A2.HNOI", "BANK - CN THANG LONG"),
    (301, "A3.MTRG", "BANK - CN DA NANG"), (302, "A3.MTRG", "BANK - CN NINH THUAN"),
    (501, "A5.MTAY", "BANK - CN CAN THO"), (502, "A5.MTAY", "BANK - CN KIEN GIANG"),
    (601, "A6.DNBO", "BANK - CN DONG NAI"), (602, "A6.DNBO", "BANK - CN BINH DUONG"),
]
# (prodcode, mjacctcd, miacctcd, tensp, nhomsp, scope TC=tổ chức / CN=cá nhân)
PRODUCTS = [
    ("SKD", "CML", "MN01", "Cho vay bo sung von luu dong", "Nhom SP tai tro TMTN", "TC"),
    ("DADT", "CML", "MD02", "Cho vay du an dau tu", "Nhom SP tai tro du an va dau tu TSCD", "TC"),
    ("KTG", "CML", "MN05", "Tai tro thuong mai trong nuoc", "Nhom SP tai tro TMTN", "TC"),
    ("MNHA", "CNS", "MD04", "Cho vay mua nha de o", "Nhom SP cho vay tieu dung", "CN"),
    ("MOTO", "CNS", "MD06", "Cho vay mua o to", "Nhom SP cho vay tieu dung", "CN"),
    ("TCTD", "CNS", "MN03", "Cho vay tieu dung tin chap", "Nhom SP cho vay tieu dung", "CN"),
    ("THAU", "CNS", "OD01", "Cho vay thau chi", "Nhom SP cho vay tieu dung", "CN"),
]
DESC = ["CK noi bo", "Nop tien mat tai quay", "Rut tien ATM", "Thanh toan hoa don dien",
        "Tra no vay ky {k}", "TTQT Thu phi LCN - PH LC so {r}", "Chuyen tien NAPAS 247 den {n}",
        "Thu lai tien gui ky han", "Phi duy tri tai khoan"]
USD_RATE = 25_400

HEADERS = {
    "customers": ["customer_id", "cust_type", "full_name", "id_no", "dob", "gender", "phone",
                  "email", "address", "province", "branchnbr", "segment", "open_date",
                  "updated_at"],
    "accounts": ["account_id", "customer_id", "branchnbr", "product_type", "currencycd",
                 "status", "open_date", "close_date", "balance", "int_rate"],
    "loans": ["etl_dt", "br_area", "branchnbr", "br_name", "acctnbr", "customer_id", "scope",
              "mjacctcd", "miacctcd", "prodcode", "tensp", "nhomsp", "currencycd", "int_rate",
              "datestart", "datemat", "loan_dur", "dudau", "chovay", "thuno", "thulai", "ducuoi",
              "ducuoiqd", "songay_nqh", "nhom_no", "noqh", "offnbr", "offname", "ftp_rate",
              "acctstatcd", "acctsiba", "nextdue"],
}


@dataclass
class Sizes:
    customers: int = 20_000
    accounts: int = 30_000
    loans: int = 12_000
    txns_per_day: int = 200_000


def _hit(rnd: random.Random, rate: float = ERROR_RATE) -> bool:
    return rnd.random() < rate


def _mdy(d: date) -> str:
    # Định dạng ngày chuẩn của nguồn: MM/DD/YYYY (giống file duno)
    return d.strftime("%m/%d/%Y")


def _person(rnd: random.Random) -> str:
    return f"{rnd.choice(HO)} {rnd.choice(DEM)} {rnd.choice(TEN)}"


def _dg(dpd: int) -> int:
    """Nhóm nợ theo số ngày quá hạn (TT 11/2021) - phải khớp common.banking."""
    if dpd < 10:
        return 1
    if dpd < 90:
        return 2
    if dpd < 180:
        return 3
    if dpd < 360:
        return 4
    return 5


def _random_dpd(rnd: random.Random) -> int:
    bucket = rnd.choices([0, 1, 2, 3, 4, 5], [80, 8, 5, 3, 2, 2])[0]
    return [0, rnd.randint(1, 9), rnd.randint(10, 89), rnd.randint(90, 179),
            rnd.randint(180, 359), rnd.randint(360, 1900)][bucket]


def _write_csv(path: Path, header: list[str], rows: list[list]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return len(rows)


def _write_jsonl(path: Path, rows: list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return len(rows)


# ---------- thực thể gốc (ổn định qua các ngày) ----------
def base_customers(rnd: random.Random, n: int, as_of: date) -> list[dict]:
    out = []
    for i in range(n):
        org = rnd.random() < 0.25  # 25% KH tổ chức, 75% cá nhân
        name = (f"{rnd.choice(DN)} {rnd.choice(LV)} {rnd.choice(TEN)} {rnd.choice(TEN)}"
                if org else _person(rnd))
        br = rnd.choice(BRANCHES)
        out.append({
            "customer_id": str(1_000_000 + i), "cust_type": "TC" if org else "CN",
            "full_name": name[:50],
            "id_no": "".join(rnd.choices("0123456789", k=12 if rnd.random() < 0.8 else 9)),
            "dob": as_of - timedelta(days=rnd.randint(19 * 365, 75 * 365)),
            "gender": rnd.choice("MF"),
            "phone": "09" + "".join(rnd.choices("0123456789", k=8)),
            "email": f"user{i}@example.com",
            "address": f"{rnd.randint(1, 999)} DUONG SO {rnd.randint(1, 50)}",
            "province": br[2].split("CN ")[-1], "branchnbr": br[0],
            "segment": rnd.choice(["SME", "MM&LC", "RETAIL", "AFF"]),
            "open_date": as_of - timedelta(days=rnd.randint(30, 4000)),
        })
    return out


def base_accounts(rnd: random.Random, n: int, cust_ids: list[str], as_of: date) -> list[dict]:
    out = []
    for i in range(n):
        st = rnd.choices(["ACTIVE", "CLOSED", "DORMANT"], [85, 8, 7])[0]
        od = as_of - timedelta(days=rnd.randint(1, 3000))
        out.append({
            "account_id": str(200_000_000 + i), "customer_id": rnd.choice(cust_ids),
            "branchnbr": rnd.choice(BRANCHES)[0],
            "product_type": rnd.choice(["CASA", "TD", "TD", "SAVING"]),
            "currencycd": rnd.choices(["VND", "USD"], [95, 5])[0], "status": st,
            "open_date": od,
            "close_date": od + timedelta(days=rnd.randint(30, 900)) if st == "CLOSED" else None,
            "balance": 0.0 if st == "CLOSED" else round(rnd.lognormvariate(17, 1.8), -3),
            "int_rate": rnd.uniform(0.001, 0.075),
        })
    return out


def base_loans(rnd: random.Random, n: int, cust_ids: list[str], as_of: date) -> list[dict]:
    out = []
    for i in range(n):
        pr = rnd.choice(PRODUCTS)
        start = as_of - timedelta(days=rnd.randint(30, 2500))
        dur = rnd.choice([6, 12, 24, 36, 60, 120, 240])
        cur = rnd.choices(["VND", "USD"], [96, 4])[0]
        bal = round(rnd.lognormvariate(19.5, 1.5), -3)
        out.append({
            "branch": rnd.choice(BRANCHES), "acctnbr": str(300_000_000 + i),
            "customer_id": rnd.choice(cust_ids), "product": pr, "currencycd": cur,
            "int_rate": rnd.uniform(0.045, 0.16), "datestart": start,
            "datemat": start + timedelta(days=dur * 30), "loan_dur": dur,
            "balance": bal if cur == "VND" else round(bal / USD_RATE, 2),
            "dpd": _random_dpd(rnd), "offnbr": str(6_000_000 + rnd.randint(0, 9999)),
            "offname": _person(rnd), "ftp_rate": rnd.uniform(0.03, 0.08),
            "acctsiba": f"{rnd.choice(['HCM', 'HAN', 'DNG', 'CTO', 'KIG'])}."
                        f"{'DN' if pr[5] == 'TC' else 'CN'}.{rnd.randint(1, 999)}.{start:%d%m%y}",
        })
    return out


# ---------- snapshot từng ngày (có cài lỗi) ----------
def snap_customers(rnd: random.Random, base: list[dict], as_of: date) -> list[list]:
    rows = []
    for c in base:
        r = [c["customer_id"], c["cust_type"], c["full_name"], c["id_no"], _mdy(c["dob"]),
             c["gender"], c["phone"], c["email"], c["address"], c["province"], c["branchnbr"],
             c["segment"], _mdy(c["open_date"]), f"{as_of.isoformat()} 00:00:00"]
        if _hit(rnd):
            r[3] = r[3][:7]  # CCCD sai độ dài (WARN)
        if _hit(rnd):
            r[4] = _mdy(as_of - timedelta(days=rnd.randint(1, 15 * 365)))  # tuổi < 18 (WARN)
        if _hit(rnd, ERROR_RATE / 4):
            r[4] = c["dob"].strftime("%Y-%m-%d")  # ngày sinh sai định dạng
        if _hit(rnd, ERROR_RATE / 4):
            r[0] = ""  # customer_id NULL (FAIL)
        rows.append(r)
    rows += rnd.sample(rows, max(1, len(rows) // 200))  # trùng customer_id (FAIL)
    rnd.shuffle(rows)
    return rows


def snap_accounts(rnd: random.Random, base: list[dict]) -> list[list]:
    rows = []
    for i, a in enumerate(base):
        bal = a["balance"] * rnd.uniform(0.97, 1.03) if a["balance"] else 0.0
        r = [a["account_id"], a["customer_id"], a["branchnbr"], a["product_type"],
             a["currencycd"], a["status"], _mdy(a["open_date"]),
             _mdy(a["close_date"]) if a["close_date"] else "", f"{bal:.2f}",
             f"{a['int_rate']:.5f}"]
        if _hit(rnd):
            r[5] = rnd.choice(["active", "ACTV", "FROZEN"])  # trạng thái lạ (WARN)
        if _hit(rnd, ERROR_RATE / 2):
            r[1] = str(9_000_000 + i)  # customer_id không tồn tại (FAIL)
        rows.append(r)
    rows += rnd.sample(rows, max(1, len(rows) // 200))  # trùng account_id (FAIL)
    return rows


def snap_loans(rnd: random.Random, base: list[dict], as_of: date) -> list[list]:
    rows = []
    for ln in base:
        # Diễn biến trong ngày: thu nợ gốc, giải ngân thêm, DPD tăng/giảm
        dudau = ln["balance"]
        chovay = round(dudau * rnd.choice([0, 0, 0, 0, 0.05]), 2)
        thuno = -round(dudau * rnd.choice([0, 0, 0, 0.01, 0.03]), 2)
        ducuoi = round(dudau + chovay + thuno, 2)
        ln["balance"] = ducuoi
        if ln["dpd"] > 0:
            ln["dpd"] = 0 if rnd.random() < 0.05 else ln["dpd"] + 1
        elif rnd.random() < 0.002:
            ln["dpd"] = 1
        fx = USD_RATE if ln["currencycd"] == "USD" else 1
        br, pr, dpd = ln["branch"], ln["product"], ln["dpd"]
        grp = _dg(dpd)
        r = [_mdy(as_of), br[1], br[0], br[2], ln["acctnbr"], ln["customer_id"], pr[5],
             pr[1], pr[2], pr[0], pr[3], pr[4], ln["currencycd"], f"{ln['int_rate']:.5f}",
             _mdy(ln["datestart"]) + " 00:00:00", _mdy(ln["datemat"]) + " 00:00:00",
             ln["loan_dur"], f"{dudau:.2f}", f"{chovay:.2f}", f"{thuno:.2f}",
             f"{-round(ducuoi * 0.008, 0):.2f}", f"{ducuoi:.2f}", f"{ducuoi * fx:.2f}",
             dpd, grp, "Y" if dpd > 0 else "N", ln["offnbr"], ln["offname"],
             f"{ln['ftp_rate']:.5f}", "NPFM" if grp >= 3 else "ACT", ln["acctsiba"],
             _mdy(as_of + timedelta(days=rnd.randint(1, 30)))]
        if _hit(rnd):
            r[21] = f"{-abs(ducuoi):.2f}"  # dư nợ âm (FAIL)
        if _hit(rnd):
            r[14], r[15] = r[15], r[14]  # ngày giải ngân > ngày đáo hạn (FAIL)
        if _hit(rnd, ERROR_RATE / 2):
            r[24] = rnd.choice([0, 6, 9])  # nhóm nợ ngoài 1-5 (FAIL)
        elif _hit(rnd):
            r[24] = grp - 1 if grp > 1 else 2  # nhóm nợ lệch DPD (WARN)
        if _hit(rnd, ERROR_RATE / 2):
            r[13] = f"{rnd.uniform(0.45, 1.2):.5f}"  # lãi suất > 40%/năm (WARN)
        if _hit(rnd, ERROR_RATE / 4):
            r[14] = ln["datestart"].strftime("%d-%b-%Y")  # ngày sai định dạng
        if _hit(rnd, ERROR_RATE / 4):
            r[21] = ""  # dư nợ NULL
        rows.append(r)
    rows += rnd.sample(rows, max(1, len(rows) // 200))  # trùng acctnbr
    return rows


def gen_transactions(rnd: random.Random, n: int, acc_ids: list[str], as_of: date) -> list[dict]:
    day0 = datetime.combine(as_of, datetime.min.time())
    out = []
    for i in range(n):
        ts = day0 + timedelta(seconds=rnd.randint(0, 86399))
        amt = round(rnd.lognormvariate(14, 1.7), -3) or 1000.0
        d = rnd.choice(DESC).format(k=rnd.randint(1, 60), r=f"PTH{rnd.randint(10**8, 10**9)}",
                                    n=_person(rnd))
        t = {"txn_id": f"TXN{as_of:%Y%m%d}{i:09d}", "account_id": rnd.choice(acc_ids),
             "txn_ts": ts.strftime("%Y-%m-%d %H:%M:%S"), "value_date": as_of.isoformat(),
             "amount": amt, "dr_cr": rnd.choice(["D", "C"]), "currencycd": "VND",
             "channel": rnd.choice(["TELLER", "ATM", "IB", "MB", "POS", "BATCH"]),
             "txn_type": rnd.choice(["TRF", "CASH", "FEE", "INT", "LOANPMT"]),
             "description": d[:182], "teller_user": f"USR{rnd.randint(1, 800):04d}",
             "branchnbr": rnd.choice(BRANCHES)[0]}
        if _hit(rnd):
            t["amount"] = rnd.choice([0.0, -amt])  # số tiền <= 0 (FAIL)
        if _hit(rnd, ERROR_RATE / 2):
            t["account_id"] = str(299_000_000 + i)  # account_id không tồn tại (FAIL)
        if _hit(rnd, ERROR_RATE / 4):
            t["txn_ts"] = (ts + timedelta(days=rnd.randint(2, 30))).strftime("%Y-%m-%d %H:%M:%S")
        if _hit(rnd, ERROR_RATE / 4):
            t["txn_ts"] = ts.strftime("%d/%m/%Y %H:%M")  # ngày sai định dạng
        if i and _hit(rnd, ERROR_RATE / 4):
            t["txn_id"] = f"TXN{as_of:%Y%m%d}{rnd.randint(0, i - 1):09d}"  # trùng txn_id (FAIL)
        out.append(t)
    return out


def generate(out: Path, start: date, days: int, seed: int = 42,
             sizes: Sizes | None = None) -> dict[str, int]:
    """Sinh `days` ngày dữ liệu bắt đầu từ `start`. Trả về tổng số dòng theo bảng."""
    sizes = sizes or Sizes()
    rnd = random.Random(seed)
    out = Path(out)
    counts: dict[str, int] = {}

    def add(table: str, n: int) -> None:
        counts[table] = counts.get(table, 0) + n

    customers = base_customers(rnd, sizes.customers, start)
    cust_ids = [c["customer_id"] for c in customers]
    accounts = base_accounts(rnd, sizes.accounts, cust_ids, start)
    acc_ids = [a["account_id"] for a in accounts]
    loans = base_loans(rnd, sizes.loans, cust_ids, start)

    for d in range(days):
        as_of = start + timedelta(days=d)
        part = f"as_of_date={as_of.isoformat()}"
        add("customers", _write_csv(out / "customers" / part / "customers.csv",
                                    HEADERS["customers"], snap_customers(rnd, customers, as_of)))
        add("accounts", _write_csv(out / "accounts" / part / "accounts.csv",
                                   HEADERS["accounts"], snap_accounts(rnd, accounts)))
        add("loans", _write_csv(out / "loans" / part / "loans.csv",
                                HEADERS["loans"], snap_loans(rnd, loans, as_of)))
        add("transactions", _write_jsonl(out / "transactions" / part / "transactions.json",
                                         gen_transactions(rnd, sizes.txns_per_day, acc_ids,
                                                          as_of)))
    return counts


def main() -> None:
    p = argparse.ArgumentParser(description="Sinh data core banking giả lập")
    p.add_argument("--out", default="data/landing")
    p.add_argument("--start-date", default=str(date.today() - timedelta(days=1)))
    p.add_argument("--days", type=int, default=1)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--scale", type=float, default=1.0, help="nhân số dòng (0.1 = 10%%)")
    a = p.parse_args()
    base = Sizes()
    sizes = Sizes(*(max(1, int(v * a.scale)) for v in
                    (base.customers, base.accounts, base.loans, base.txns_per_day)))
    counts = generate(Path(a.out), date.fromisoformat(a.start_date), a.days, a.seed, sizes)
    for t, n in counts.items():
        print(f"{t:<13} {n:>9,} rows")


if __name__ == "__main__":
    main()
