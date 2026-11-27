import pytest

from common.batch import (
    assert_fresh,
    assert_reset_allowed,
    business_dates,
    corrected_days,
    expected_through,
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
    assert pending_dates([], None) == []


def test_pending_dates_does_not_skip_a_day_without_files():
    # 25/11 chưa có file: vẫn phải nằm trong danh sách -> Silver dừng ở 25/11 (thiếu file),
    # watermark không nhảy sang 26/11 và bỏ quên 25/11 khi file gửi trễ.
    bronze = ["2026-11-24", "2026-11-26"]
    assert pending_dates(bronze, None) == ["2026-11-24", "2026-11-25", "2026-11-26"]
    assert pending_dates(bronze, "2026-11-24") == ["2026-11-25", "2026-11-26"]
    # file 25/11 về trễ, watermark vẫn ở 24/11 -> xử lý tiếp từ 25/11
    assert pending_dates(bronze + ["2026-11-25"], "2026-11-24") == ["2026-11-25", "2026-11-26"]


def test_pending_dates_respects_holidays():
    assert pending_dates(["2026-11-24", "2026-11-26"], None,
                         holidays=frozenset({"2026-11-25"})) == ["2026-11-24", "2026-11-26"]
    assert business_dates("2026-11-30", "2026-12-02") == ["2026-11-30", "2026-12-01",
                                                         "2026-12-02"]


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


def test_corrected_days_detects_files_loaded_after_silver_success():
    done = {"2026-11-24": "2026-11-25 01:00", "2026-11-25": "2026-11-26 01:00"}
    loaded = {"2026-11-24": "2026-11-25 00:30",     # nạp trước Silver -> bình thường
              "2026-11-25": "2026-11-26 09:00",     # file sửa nạp sau Silver -> correction
              "2026-11-26": "2026-11-27 00:30"}     # chưa có Silver -> ngày mới, không phải sửa
    assert corrected_days(loaded, done) == ["2026-11-25"]
    assert corrected_days(loaded, {}) == []


def test_calendar_is_month_end_only_before_daily_feed_start():
    # lịch sử 13 tháng = snapshot cuối tháng; từ daily_from mới là lịch hằng ngày
    assert business_dates("2026-09-30", "2026-11-03", daily_from="2026-11-01") == [
        "2026-09-30", "2026-10-31", "2026-11-01", "2026-11-02", "2026-11-03"]
    assert business_dates("2024-01-31", "2024-03-01", daily_from="2024-03-01") == [
        "2024-01-31", "2024-02-29", "2024-03-01"]                   # năm nhuận
    # backfill từ đầu: không đòi file 2026-10-01..30 (nguồn không bao giờ gửi)
    bronze = ["2026-09-30", "2026-10-31", "2026-11-01", "2026-11-02"]
    assert pending_dates(bronze, None, daily_from="2026-11-01") == bronze
    # thiếu 1 snapshot cuối tháng vẫn bị phát hiện (không nhảy qua)
    assert pending_dates(["2026-09-30", "2026-11-01"], None, daily_from="2026-11-01") == [
        "2026-09-30", "2026-10-31", "2026-11-01"]


def test_expected_through_and_sla_extends_calendar_past_last_file():
    assert expected_through("2026-11-27", 1) == "2026-11-26"
    assert expected_through("2026-03-01", 1) == "2026-02-28"
    with pytest.raises(ValueError):
        expected_through("2026-11-27", -1)
    # nguồn ngừng gửi sau 24: không có SLA thì không thấy ngày nào thiếu (job "xanh")
    assert pending_dates(["2026-11-24"], "2026-11-24") == []
    # có SLA: 25, 26 là ngày dự kiến -> Silver dừng ở 25 với SourceFilesMissing
    assert pending_dates(["2026-11-24"], "2026-11-24",
                         expected_until="2026-11-26") == ["2026-11-25", "2026-11-26"]
    # file về sớm hơn SLA: vẫn xử lý tới ngày bronze mới nhất
    assert pending_dates(["2026-11-27"], "2026-11-26",
                         expected_until="2026-11-26") == ["2026-11-27"]
    assert pending_dates([], None, expected_until="2026-11-26") == []   # chưa có mốc
