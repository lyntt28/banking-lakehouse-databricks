import pytest

from common.batch import (
    assert_fresh,
    assert_reset_allowed,
    missing_tables,
    pending_dates,
    required_tables,
)
from common.silver import ORDER


def test_pending_dates_catches_up_after_outage():
    # job dừng 24/11, 25/11: lần chạy 26/11 phải xử lý đủ 3 ngày, đúng thứ tự
    bronze = ["2026-11-26", "2026-11-23", "2026-11-24", "2026-11-25", "2026-11-25"]
    assert pending_dates(bronze, "2026-11-23") == ["2026-11-24", "2026-11-25", "2026-11-26"]


def test_pending_dates_first_run_and_explicit_rerun():
    assert pending_dates(["2026-11-02", "2026-11-01"], None) == ["2026-11-01", "2026-11-02"]
    assert pending_dates(["2026-11-02"], "2026-11-02") == []
    assert pending_dates(["2026-11-02"], "2026-11-02", "2026-11-01") == ["2026-11-01"]
    with pytest.raises(ValueError):
        pending_dates([], None, "2026-11-01' OR 1=1")


def test_missing_required_file_is_detected():
    counts = {t: 10 for t in ORDER}
    assert missing_tables(counts, ORDER) == []
    counts["core_transactions"] = 0
    counts["ref_branches"] = 0              # bảng tham chiếu: không gửi hằng ngày -> bỏ qua
    assert missing_tables(counts, ORDER) == ["core_transactions"]
    assert "ref_targets" not in required_tables(ORDER)


def test_reset_blocked_on_prod():
    assert_reset_allowed("bank_dev")
    assert_reset_allowed("bank_test")
    with pytest.raises(PermissionError):
        assert_reset_allowed("bank_prod")


def test_freshness_gate():
    assert_fresh("2026-11-25", "2026-11-26", 2)
    with pytest.raises(RuntimeError):
        assert_fresh("2026-11-20", "2026-11-26", 2)
    with pytest.raises(RuntimeError):
        assert_fresh(None, "2026-11-26", 2)
