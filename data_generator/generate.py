"""Sinh dữ liệu core banking GIẢ LẬP (không có thông tin KH thật) cho project Lakehouse.

Mỗi ngày 1 thư mục: <out>/<table>/as_of_date=YYYY-MM-DD/<file>

Bảng chi tiết (Bronze -> Silver -> Gold):
    branches, products         : danh mục (chỉ ngày đầu)
    customers                  : snapshot, ~3% KH đổi tỉnh/segment/SĐT mỗi ngày (test SCD2)
    accounts                   : tiền gửi + tài khoản vay (acctnbr của loans = account_id)
    loans                      : layout file dư nợ (duno) - cột tiếng Việt, ngày MM/DD/YYYY
    transactions               : giao dịch tài khoản (JSON lines)
    cards, card_transactions,  : thẻ tín dụng (MCC, merchant, kênh, trong nước/nước ngoài),
    card_statements              dư nợ thẻ, DPD
    insurance_policies         : hợp đồng bảo hiểm -> label cho ML cross-sell
    targets, fx_rates          : chỉ tiêu kế hoạch theo chi nhánh x kỳ, tỷ giá về VND

File tổng hợp để đối soát (tính từ bản ghi hợp lệ):
    card_kpi                   : 9 file KPI thẻ dạng dọc (tab, số có dấu phẩy, tỷ lệ %)
    deposit_summary            : số dư tiền gửi theo nhóm TK x chi nhánh (layout bang1247)

Lỗi cài cố ý (~2% mỗi loại, dưới ngưỡng dừng Job 5%): xem docstring từng module.

Chạy:  python -m data_generator.generate --out ./data/landing --start-date 2026-11-01 --days 3
"""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass, fields
from datetime import date, timedelta
from pathlib import Path

from data_generator import cards as cd
from data_generator import core
from data_generator import reference as ref
from data_generator import summaries as sm
from data_generator.common import BRANCHES, USD_RATE, write_csv, write_jsonl


@dataclass
class Sizes:
    customers: int = 20_000
    accounts: int = 30_000        # tài khoản tiền gửi (chưa tính TK vay)
    loans: int = 12_000
    txns_per_day: int = 100_000
    card_txns_per_day: int = 50_000


def month_ends_before(start: date, months: int) -> list[date]:
    """`months` ngày cuối tháng liền trước `start` (cũ -> mới)."""
    out, d = [], start.replace(day=1) - timedelta(days=1)
    for _ in range(months):
        out.append(d)
        d = d.replace(day=1) - timedelta(days=1)
    return out[::-1]


def generate(out: Path, start: date, days: int, seed: int = 42,
             sizes: Sizes | None = None, history_months: int = 0) -> dict[str, int]:
    """Sinh `days` ngày dữ liệu bắt đầu từ `start`. Trả về tổng số dòng theo bảng.

    history_months > 0: trước đó sinh thêm snapshot CUỐI THÁNG của N tháng liền trước
    (cho so sánh MoM / YoY, xu hướng 7-13 tháng). Mỗi mốc là 1 bước tiến hoá trạng thái
    (KH đổi thông tin, thẻ mở/đóng, DPD tăng...), giống 1 ngày thường.
    """
    sizes = sizes or Sizes()
    dates = month_ends_before(start, history_months) + \
        [start + timedelta(days=d) for d in range(days)]
    start = dates[0]
    rnd = random.Random(seed)  # seed cố định: chạy lại ra đúng bộ data cũ
    out = Path(out)
    counts: dict[str, int] = {}

    def add(table: str, n: int) -> None:
        counts[table] = counts.get(table, 0) + n

    # ---- thực thể gốc ----
    customers = core.base_customers(rnd, sizes.customers, start)
    accounts = core.base_deposit_accounts(rnd, sizes.accounts, customers, start)
    loans, loan_accounts = core.base_loans(rnd, sizes.loans, customers, start,
                                           first_acct=300_000_000)
    accounts += loan_accounts
    acc_ids = [a["account_id"] for a in accounts]
    cards = cd.base_cards(rnd, customers, start)
    policies = cd.base_insurance(rnd, customers, cards, start)
    dep_prev: dict = {}
    dep_month: dict = {}

    for d, as_of in enumerate(dates):
        part = f"as_of_date={as_of.isoformat()}"

        def path(table: str, fname: str, _part: str = part) -> Path:
            return out / table / _part / fname

        if d == 0:
            add("branches", write_csv(path("branches", "branches.csv"),
                                      core.HEADERS["branches"], core.branch_rows()))
            add("products", write_csv(path("products", "products.csv"),
                                      core.HEADERS["products"], core.product_rows()))
            nb = len(BRANCHES)
            dep_vnd = sum(a["balance"] * (USD_RATE if a["currencycd"] == "USD" else 1)
                          for a in accounts if a["product_type"] != "LOAN")
            add("targets", write_csv(path("targets", "targets.csv"), ref.TARGET_HEADER,
                                     [r for y in sorted({x.year for x in dates})
                                      for r in ref.target_rows(seed, y, {
                                          "CARD_NEW_ACQ": len(cards) * 0.0015 * 30 / nb,
                                          "CARD_SALES_VOLUME": sizes.card_txns_per_day
                                          * 2.5e6 * 30 / nb,
                                          "DEPOSIT_BALANCE": dep_vnd / nb,
                                      })]))
        else:
            core.evolve_customers(rnd, customers)
            cd.evolve_cards(rnd, cards, customers, as_of)
            cd.evolve_insurance(rnd, policies, customers, cards, as_of)

        add("fx_rates", write_csv(path("fx_rates", "fx_rates.csv"), ref.FX_HEADER,
                                  ref.fx_rows(as_of)))
        cust_rows = core.snap_customers(rnd, customers, as_of)
        add("customers", write_csv(path("customers", "customers.csv"),
                                   core.HEADERS["customers"], cust_rows))
        acc_rows, acc_failed = core.snap_accounts(rnd, accounts)
        # KH bị NULL id hôm nay -> TK của KH đó cũng rơi vào quarantine (FK) ở Silver
        cust_ok = {r[0] for r in cust_rows}
        acc_failed |= {a["account_id"] for a in accounts if a["customer_id"] not in cust_ok}
        add("accounts", write_csv(path("accounts", "accounts.csv"),
                                  core.HEADERS["accounts"], acc_rows))
        add("loans", write_csv(path("loans", "loans.csv"), core.HEADERS["loans"],
                               core.snap_loans(rnd, loans, as_of)))
        add("transactions", write_jsonl(path("transactions", "transactions.json"),
                                        core.gen_transactions(rnd, sizes.txns_per_day,
                                                              acc_ids, as_of)))
        # ---- thẻ + bảo hiểm ----
        add("cards", write_csv(path("cards", "cards.csv"), cd.HEADERS["cards"],
                               cd.snap_cards(rnd, cards, as_of)))
        ctx, ctx_clean = cd.gen_card_transactions(rnd, sizes.card_txns_per_day, cards, as_of)
        add("card_transactions", write_jsonl(path("card_transactions",
                                                  "card_transactions.json"), ctx))
        stm = cd.snap_card_statements(rnd, cards, as_of)
        add("card_statements", write_csv(path("card_statements", "card_statements.csv"),
                                         cd.HEADERS["card_statements"], stm))
        add("insurance_policies", write_csv(path("insurance_policies", "insurance_policies.csv"),
                                            cd.HEADERS["insurance_policies"],
                                            cd.snap_insurance(rnd, policies)))
        # ---- file tổng hợp để đối soát ----
        kpis = sm.card_kpis(cards, ctx_clean, stm, as_of)
        add("card_kpi", sm.write_card_kpis(out / "card_kpi" / part, kpis, as_of))
        add("deposit_summary", write_csv(
            path("deposit_summary", f"bang1247_{as_of:%Y%m%d}.txt"), sm.DEPOSIT_HEADER,
            sm.deposit_summary(accounts, acc_failed, as_of, dep_prev, dep_month),
            delimiter="\t"))
    return counts


def main() -> None:
    p = argparse.ArgumentParser(description="Sinh data core banking giả lập")
    p.add_argument("--out", default="data/landing")
    p.add_argument("--start-date", default="2026-11-01")
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--scale", type=float, default=1.0, help="nhân số dòng (0.1 = 10%%)")
    p.add_argument("--history-months", type=int, default=0,
                   help="thêm snapshot cuối tháng của N tháng trước start-date (13 = YoY)")
    a = p.parse_args()
    sizes = Sizes(*(max(1, int(getattr(Sizes(), f.name) * a.scale)) for f in fields(Sizes)))
    counts = generate(Path(a.out), date.fromisoformat(a.start_date), a.days, a.seed, sizes,
                      a.history_months)
    for t, n in counts.items():
        print(f"{t:<19} {n:>9,} rows")


if __name__ == "__main__":
    main()
