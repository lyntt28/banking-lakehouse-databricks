"""Hằng số và hàm tiện ích dùng chung cho generator (không cần pyspark)."""

from __future__ import annotations

import csv
import json
import random
from datetime import date
from pathlib import Path

ERROR_RATE = 0.02  # ~2% bản ghi bị cài lỗi mỗi loại (dưới ngưỡng dừng Job 5%)
USD_RATE = 25_400

HO = "NGUYEN TRAN LE PHAM HOANG HUYNH PHAN VU VO DANG BUI DO HO NGO DUONG LY".split()
DEM = "VAN THI MINH NGOC THANH HOANG QUOC DUC KIM BAO GIA XUAN".split()
TEN = ("AN BINH CHI DUNG GIANG HA HAI HANH HIEU HOA HUNG KHANH LAN LINH LONG MAI NAM NGA "
       "PHONG PHUONG QUAN SON TAM THAO THU TRANG TUAN VY YEN").split()
DN = ["CTY TNHH", "CTY CP", "DNTN", "CTY TNHH MTV", "HTX"]
LV = ["TM", "SX", "XD", "DV", "VT", "TM & DV", "XNK"]

# branchnbr, br_area (vùng), br_name, region (cho file tổng hợp), province, open_date
BRANCHES = [
    (1, "A1.HCMC", "BANK - CN SAI GON", "South 1", "HO CHI MINH", date(2005, 3, 1)),
    (2, "A1.HCMC", "BANK - CN TP.HO CHI MINH", "South 1", "HO CHI MINH", date(2006, 7, 1)),
    (101, "A2.HNOI", "BANK - CN HA NOI", "North", "HA NOI", date(2007, 1, 15)),
    (102, "A2.HNOI", "BANK - CN THANG LONG", "North", "HA NOI", date(2010, 5, 20)),
    (301, "A3.MTRG", "BANK - CN DA NANG", "Central", "DA NANG", date(2008, 9, 1)),
    (302, "A3.MTRG", "BANK - CN NINH THUAN", "Central", "NINH THUAN", date(2012, 4, 2)),
    (501, "A5.MTAY", "BANK - CN CAN THO", "South 2", "CAN THO", date(2009, 6, 10)),
    (502, "A5.MTAY", "BANK - CN KIEN GIANG", "South 2", "KIEN GIANG", date(2013, 8, 8)),
    (601, "A6.DNBO", "BANK - CN DONG NAI", "South 1", "DONG NAI", date(2011, 2, 14)),
    (602, "A6.DNBO", "BANK - CN BINH DUONG", "South 1", "BINH DUONG", date(2014, 11, 3)),
]
BRANCH_BY_NBR = {b[0]: b for b in BRANCHES}

# prodcode, mjacctcd, miacctcd, tensp, nhomsp, scope (TC=tổ chức / CN=cá nhân), term (tháng)
LOAN_PRODUCTS = [
    ("SKD", "CML", "MN01", "Cho vay bo sung von luu dong", "Nhom SP tai tro TMTN", "TC", 12),
    ("DADT", "CML", "MD02", "Cho vay du an dau tu", "Nhom SP tai tro du an va dau tu TSCD",
     "TC", 60),
    ("KTG", "CML", "MN05", "Tai tro thuong mai trong nuoc", "Nhom SP tai tro TMTN", "TC", 6),
    ("MNHA", "CNS", "MD04", "Cho vay mua nha de o", "Nhom SP cho vay tieu dung", "CN", 240),
    ("MOTO", "CNS", "MD06", "Cho vay mua o to", "Nhom SP cho vay tieu dung", "CN", 60),
    ("TCTD", "CNS", "MN03", "Cho vay tieu dung tin chap", "Nhom SP cho vay tieu dung", "CN", 36),
    ("THAU", "CNS", "OD01", "Cho vay thau chi", "Nhom SP cho vay tieu dung", "CN", 12),
]
# Tiền gửi: product_type, ACCOUNT_GROUP (giống file tổng hợp bang1247), GROUP_CLASS
DEPOSIT_PRODUCTS = [
    ("CASA", "Current Account", "1-CASA"),
    ("PAYROLL", "Payroll / Payroll Plus Account", "1-CASA"),
    ("SAVING", "Time Saving Account", "1-CASA"),
    ("TD", "Term Deposit", "2-TD"),
]
# Sản phẩm suy ra từ sản phẩm gốc theo account_id (không thêm lượt ngẫu nhiên -> dữ liệu cũ
# giữ nguyên): 1/4 SAVING là ISA (tích luỹ cá nhân), 1/25 CASA là Edu-Save (du học)
DEPOSIT_DERIVED = [
    ("ISA", "Individual Saving Account", "3-ISA"),
    ("EDUSAVE", "Edu-Save Account", "1-CASA"),
]
DEPOSIT_ALL = DEPOSIT_PRODUCTS + DEPOSIT_DERIVED
TENORS = [1, 3, 6, 12, 24, 36]                       # kỳ hạn TD / ISA (tháng)
TENOR_RATE = {1: 0.030, 3: 0.034, 6: 0.045, 12: 0.052, 24: 0.055, 36: 0.057}
# card_product, card_grade, hạn mức min/max (VND)
CARD_PRODUCTS = [
    ("VISA_CLASSIC", "CLASSIC", 10_000_000, 50_000_000),
    ("VISA_GOLD", "GOLD", 50_000_000, 150_000_000),
    ("VISA_PLATINUM", "PLATINUM", 150_000_000, 500_000_000),
    ("MC_WORLD", "PREMIUM", 300_000_000, 1_000_000_000),
    ("VISA_CORP", "CORPORATE", 100_000_000, 2_000_000_000),
]
GRADE_RANK = {"CLASSIC": 1, "GOLD": 2, "PLATINUM": 3, "PREMIUM": 4, "CORPORATE": 2}
# mcc, sector, merchant mẫu
MCC = [
    ("5411", "Supermarket", ["CO.OPMART", "WINMART", "BACH HOA XANH"]),
    ("5812", "Restaurant", ["HIGHLANDS COFFEE", "PHO 24", "GOGI HOUSE"]),
    ("5541", "Fuel", ["PETROLIMEX", "PVOIL"]),
    ("4511", "Airline", ["VIETNAM AIRLINES", "VIETJET AIR", "BAMBOO AIRWAYS"]),
    ("7011", "Hotel", ["MUONG THANH", "VINPEARL", "AGODA"]),
    ("5732", "Electronics", ["THE GIOI DI DONG", "DIEN MAY XANH", "FPT SHOP"]),
    ("5999", "E-commerce", ["SHOPEE", "LAZADA", "TIKI"]),
    ("8062", "Hospital", ["BV FV", "BV VINMEC", "BV HOAN MY"]),
    ("8220", "Education", ["RMIT", "BRITISH COUNCIL", "ILA"]),
    ("6011", "Cash", ["ATM"]),
]
INSURANCE_PRODUCTS = [("LIFE_SAVING", 12_000_000, 60_000_000), ("HEALTH", 5_000_000, 25_000_000),
                      ("INVEST_LINKED", 20_000_000, 120_000_000)]


def hit(rnd: random.Random, rate: float = ERROR_RATE) -> bool:
    return rnd.random() < rate


def mdy(d: date) -> str:
    """Ngày kiểu core banking: MM/DD/YYYY."""
    return d.strftime("%m/%d/%Y")


def person(rnd: random.Random) -> str:
    return f"{rnd.choice(HO)} {rnd.choice(DEM)} {rnd.choice(TEN)}"


def debt_group(dpd: int) -> int:
    """Nhóm nợ theo DPD (TT 11/2021) - phải khớp common.banking (có pytest kiểm tra)."""
    for upper, g in [(10, 1), (90, 2), (180, 3), (360, 4)]:
        if dpd < upper:
            return g
    return 5


def write_csv(path: Path, header: list[str], rows: list[list], delimiter: str = ",") -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=delimiter)
        w.writerow(header)
        w.writerows(rows)
    return len(rows)


def write_jsonl(path: Path, rows: list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return len(rows)
