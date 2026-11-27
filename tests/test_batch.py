import pytest

from common.batch import (
    assert_fresh,
    assert_reset_allowed,
    business_dates,
    corrected_days,
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


# ---- lịch lịch sử (cuối tháng) vs lịch hằng ngày; ngày dự kiến + SLA
def test_history_month_ends_do_not_require_mid_month_days():
    from common.batch import daily_start

    hist = ["2025-10-31", "2025-11-30", "2025-12-31", "2026-01-31"]
    daily = ["2026-02-01", "2026-02-02", "2026-02-03"]
    assert daily_start(hist + daily) == "2026-01-31"
    # backfill lịch sử từ đầu: chỉ các ngày cuối tháng, rồi từng ngày
    assert pending_dates(hist + daily, None) == hist + daily
    # đang giữa lịch sử: ngày kế tiếp là cuối tháng sau, không phải 01/12
    assert pending_dates(hist + daily, "2025-11-30") == hist[2:] + daily
    # trong giai đoạn hằng ngày vẫn bắt được ngày thiếu
    assert pending_dates(hist + ["2026-02-01", "2026-02-03"], "2026-01-31") == daily
    # chỉ định rõ ngày bắt đầu hằng ngày
    assert pending_dates(["2026-01-31", "2026-02-02"], None, daily_from="2026-02-01") == \
        ["2026-01-31", "2026-02-01", "2026-02-02"]


def test_expected_until_detects_today_without_any_file():
    # file mới nhất là 25/11, sáng 27/11 phải có file 26/11 -> 26/11 nằm trong danh sách chờ
    assert pending_dates(["2026-11-24", "2026-11-25"], "2026-11-25",
                         expected_until="2026-11-26") == ["2026-11-26"]
    assert pending_dates(["2026-11-24", "2026-11-25"], "2026-11-25") == []
    assert pending_dates(["2026-11-26"], "2026-11-26", expected_until="2026-11-26") == []


def test_expected_processing_date_and_sla():
    from datetime import datetime

    from common.batch import expected_processing_date, missing_status, sla_deadline

    assert expected_processing_date(datetime(2026, 11, 27, 6, 30), 1) == "2026-11-26"
    assert sla_deadline("2026-11-26", 1, 9) == datetime(2026, 11, 27, 9, 0)
    assert missing_status("2026-11-26", datetime(2026, 11, 27, 6, 30), 1, 9) == "WAITING"
    assert missing_status("2026-11-26", datetime(2026, 11, 27, 9, 30), 1, 9) == "LATE"
    assert missing_status("2026-11-20", datetime(2026, 11, 27, 6, 30), 1, 9) == "LATE"
