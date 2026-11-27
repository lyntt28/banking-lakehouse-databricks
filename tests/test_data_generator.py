import csv
import json
from datetime import date

import pytest

from data_generator.common import USD_RATE, debt_group
from data_generator.core import HEADERS
from data_generator.generate import Sizes, generate

D1, D2 = "2026-11-01", "2026-11-02"
SMALL = Sizes(customers=1500, accounts=2000, loans=800, txns_per_day=2000,
              card_txns_per_day=3000)


def _read(path, delimiter=","):
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=delimiter))


def _jsonl(path):
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines()]


@pytest.fixture(scope="module")
def out(tmp_path_factory):
    o = tmp_path_factory.mktemp("landing")
    generate(o, date(2026, 11, 1), 2, seed=1, sizes=SMALL)
    return o


def _t(out, table, day, fname=None, delimiter=","):
    p = out / table / f"as_of_date={day}" / (fname or f"{table}.csv")
    return _read(p, delimiter)


def test_debt_group_matches_banking_rule():
    from common.banking import debt_group_from_dpd

    for dpd in [0, 9, 10, 89, 90, 179, 180, 359, 360, 800]:
        assert debt_group(dpd) == debt_group_from_dpd(dpd)


def test_deterministic(tmp_path):
    s = Sizes(200, 300, 100, 300, 300)
    assert generate(tmp_path / "a", date(2026, 11, 1), 2, seed=7, sizes=s) == \
        generate(tmp_path / "b", date(2026, 11, 1), 2, seed=7, sizes=s)
    a = (tmp_path / "a/loans/as_of_date=2026-11-02/loans.csv").read_bytes()
    b = (tmp_path / "b/loans/as_of_date=2026-11-02/loans.csv").read_bytes()
    assert a == b


def test_dimensions_only_first_day(out):
    assert len(_t(out, "branches", D1)) == 10
    assert {p["product_family"] for p in _t(out, "products", D1)} == {"LOAN", "DEPOSIT"}
    assert not (out / "branches" / f"as_of_date={D2}").exists()


def test_injected_errors_below_stop_threshold(out):
    cust = _t(out, "customers", D1)
    ids = [r["customer_id"] for r in cust]
    assert "" in ids and len(set(ids)) < len(ids)  # có NULL và trùng
    loans = _t(out, "loans", D1)
    neg = [r for r in loans if r["ducuoi"] and float(r["ducuoi"]) < 0]
    assert neg and len(neg) / len(loans) < 0.05
    assert any(r["nhom_no"] in ("0", "6", "9") for r in loans)
    ctx = _jsonl(out / "card_transactions" / f"as_of_date={D1}" / "card_transactions.json")
    bad = sum(1 for t in ctx if t["amount"] <= 0) / len(ctx)
    assert 0 < bad < 0.05


def test_loans_follow_duno_layout_and_link_accounts(out):
    loans = _t(out, "loans", D2)
    assert list(loans[0].keys()) == HEADERS["loans"]
    mm, dd, yyyy = loans[0]["etl_dt"].split("/")
    assert f"{yyyy}-{mm}-{dd}" == D2
    acc = {r["account_id"] for r in _t(out, "accounts", D2) if r["product_type"] == "LOAN"}
    assert {r["acctnbr"] for r in loans} <= acc


def test_customers_change_for_scd2(out):
    def attrs(day):
        return {r["customer_id"]: (r["province"], r["segment"], r["phone"])
                for r in _t(out, "customers", day) if r["customer_id"]}

    a, b = attrs(D1), attrs(D2)
    changed = sum(1 for k in a if k in b and a[k] != b[k]) / len(a)
    assert 0.01 < changed < 0.06  # ~3% KH thay đổi thuộc tính


def test_card_kpi_reconciles_with_valid_transactions(out):
    cards = {r["card_no"] for r in _t(out, "cards", D2)}
    ctx = _jsonl(out / "card_transactions" / f"as_of_date={D2}" / "card_transactions.json")
    counts: dict[str, int] = {}
    for t in ctx:
        counts[t["txn_id"]] = counts.get(t["txn_id"], 0) + 1
    assert any(n > 1 for n in counts.values())  # có trùng txn_id
    # Quy tắc Silver: amount > 0, card tồn tại, txn_id trùng -> quarantine CẢ các bản ghi
    total = sum(t["amount"] for t in ctx
                if t["amount"] > 0 and t["card_no"] in cards and counts[t["txn_id"]] == 1)
    kpi = {r["Metric_Code"]: r["Metric_Value"] for r in
           _t(out, "card_kpi", D2, "06_spend_sector_20261102.txt", "	")}
    assert float(kpi["Total_Spending_Amt"].replace(",", "")) == pytest.approx(total, abs=1)


def test_deposit_summary_reconciles_with_accounts(out):
    # FK ở Silver = KH đã có từ các ngày trước + KH trong lô hôm nay
    cust = {r["customer_id"] for d in (D1, D2) for r in _t(out, "customers", d)
            if r["customer_id"]}
    seen, total = set(), 0.0
    for r in _t(out, "accounts", D2):
        if r["product_type"] == "LOAN" or r["account_id"] in seen or r["customer_id"] not in cust:
            continue
        seen.add(r["account_id"])
        total += float(r["balance"]) * (USD_RATE if r["currencycd"] == "USD" else 1) / 1e6
    summ = _t(out, "deposit_summary", D2, "bang1247_20261102.txt", "\t")
    assert summ[0]["STND_DT"] == "20261102"
    assert sum(float(r["BAL"]) for r in summ) == pytest.approx(total, rel=1e-4)


def test_insurance_label_has_signal_for_ml(out):
    cust = {r["customer_id"]: r for r in _t(out, "customers", D2) if r["customer_id"]}
    buyers = {r["customer_id"] for r in _t(out, "insurance_policies", D2) if r["customer_id"]}
    cn = [c for c in cust.values() if c["cust_type"] == "CN"]
    rich = [c for c in cn if float(c["monthly_income"]) > 40_000_000]
    poor = [c for c in cn if float(c["monthly_income"]) < 15_000_000]

    def rate(xs):
        return sum(c["customer_id"] in buyers for c in xs) / len(xs)

    assert rich and poor and rate(rich) > rate(poor) * 1.5  # thu nhập cao mua nhiều hơn


def test_new_accounts_open_after_first_day(out):
    """Ngày 2 có TK mở mới (open_date = ngày 2) -> biểu đồ TK mới / Edu-Save có dữ liệu."""
    d1 = {r["account_id"] for r in _t(out, "accounts", D1)}
    new = [r for r in _t(out, "accounts", D2) if r["account_id"] not in d1 and r["account_id"]]
    assert new, "không có TK mở mới ở ngày 2"
    assert all(r["account_id"].startswith("25") for r in new)
    assert all(r["product_type"] != "LOAN" for r in new)
