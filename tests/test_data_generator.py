import csv
import json
from datetime import date

from data_generator.generate import Sizes, _dg, generate


def _read(path):
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_dg_matches_banking_rule():
    from common.banking import debt_group_from_dpd

    for dpd in [0, 9, 10, 89, 90, 179, 180, 359, 360, 800]:
        assert _dg(dpd) == debt_group_from_dpd(dpd)


def test_generate_is_deterministic_and_has_injected_errors(tmp_path):
    sizes = Sizes(customers=400, accounts=600, loans=300, txns_per_day=800)
    c1 = generate(tmp_path / "a", date(2026, 11, 1), 2, seed=1, sizes=sizes)
    c2 = generate(tmp_path / "b", date(2026, 11, 1), 2, seed=1, sizes=sizes)
    assert c1 == c2  # cùng seed -> cùng số dòng

    part = "as_of_date=2026-11-01"
    cust = _read(tmp_path / "a" / "customers" / part / "customers.csv")
    ids = [r["customer_id"] for r in cust]
    assert "" in ids                       # có customer_id NULL
    assert len(set(ids)) < len(ids)        # có trùng

    loans = _read(tmp_path / "a" / "loans" / part / "loans.csv")
    assert any(r["ducuoi"] and float(r["ducuoi"]) < 0 for r in loans)
    assert any(r["nhom_no"] in ("0", "6", "9") for r in loans)

    txns = [json.loads(x) for x in
            (tmp_path / "a" / "transactions" / part / "transactions.json").read_text().splitlines()]
    assert any(t["amount"] <= 0 for t in txns)
    # Lỗi cố ý phải dưới ngưỡng dừng Job 5%
    bad = sum(1 for t in txns if t["amount"] <= 0) / len(txns)
    assert bad < 0.05


def test_loans_follow_duno_layout(tmp_path):
    from data_generator.generate import HEADERS

    sizes = Sizes(customers=50, accounts=80, loans=200, txns_per_day=10)
    generate(tmp_path, date(2026, 11, 1), 2, seed=3, sizes=sizes)
    for d in ("2026-11-01", "2026-11-02"):
        loans = _read(tmp_path / "loans" / f"as_of_date={d}" / "loans.csv")
        assert list(loans[0].keys()) == HEADERS["loans"]
        # etl_dt dạng MM/DD/YYYY, trùng ngày partition
        mm, dd, yyyy = loans[0]["etl_dt"].split("/")
        assert f"{yyyy}-{mm}-{dd}" == d
    # acctnbr ổn định qua các ngày (cùng tập khoản vay)
    a = {r["acctnbr"] for r in _read(tmp_path / "loans/as_of_date=2026-11-01/loans.csv")}
    b = {r["acctnbr"] for r in _read(tmp_path / "loans/as_of_date=2026-11-02/loans.csv")}
    assert a == b
