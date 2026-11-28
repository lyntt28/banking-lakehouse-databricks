"""Overdue dashboard: logic nghiệp vụ trên dữ liệu nhiều ngày (13 cuối tháng + ngày gần đây).

Chạy SQL dataset thật của dashboard (common.lakeview.overdue.overdue_sql) trên temp view.
Số liệu VND, tỷ giá cố định 25.000 -> USD dễ tính tay."""

from datetime import date, timedelta
from decimal import Decimal

import pytest
from dashboard_sql import literals, localize

from common import gold_cards as gc
from common import serving as sv
from common.lakeview.overdue import overdue_sql

FX = 25_000
MONTH_ENDS = [date(2025, 10, 31), date(2025, 11, 30), date(2025, 12, 31)] + [
    (date(2026, m + 1, 1) - timedelta(days=1)) for m in range(1, 11)]   # 2026-01-31..10-31
DAILY = [date(2026, 11, d) for d in range(1, 11)]                       # 2026-11-01..11-10
DATES = MONTH_ENDS + DAILY
# ngày đã có trong landing nhưng CHƯA publish -> không được xuất hiện trên dashboard
UNPUBLISHED = date(2026, 11, 11)


def _rows(d: date, i: int) -> list[tuple]:
    """(report_dt, product_group, product_name, mng_br, sal_br, del_grp, port_acc, ovd_acc,
    port_bal, ovd_bal). Dòng B tách 2 dòng cùng grain -> Gold phải SUM (không bỏ)."""
    D = Decimal
    return [
        (d, "GOLD", "VISA_GOLD", "101", "101", "0", 100, 0, D(1_000_000_000 + i * 10_000_000),
         D(0)),
        (d, "GOLD", "VISA_GOLD", "101", "101", "11-90", 30, 10, D(300_000_000),
         D(300_000_000 + i * 1_000_000)),
        (d, "GOLD", "VISA_GOLD", "101", "101", "11-90", 20, 5, D(200_000_000), D(100_000_000)),
        (d, "CLASSIC", "VISA_CLASSIC", "102", "1", "1-10", 40, 4, D(400_000_000),
         D(100_000_000)),                                   # 4.000 USD < ngưỡng 10.000
        (d, "CLASSIC", "VISA_CLASSIC", "8125", "8125", "361+", 30, 30, D(300_000_000),
         D(300_000_000)),                                   # CN bị loại khỏi treemap
        (d, "PLATINUM", "VISA_PLATINUM", "103", "103", "0", 0, 0, D(0), D(0)),   # mẫu = 0
    ]


@pytest.fixture(scope="module")
def views(spark):
    rows = [r for i, d in enumerate(DATES + [UNPUBLISHED]) for r in _rows(d, i)]
    silver = spark.createDataFrame(
        rows, "report_dt date, product_group string, product_name string, mng_br string, "
              "sal_br string, del_grp string, port_acc bigint, ovd_acc bigint, "
              "port_bal decimal(22,2), ovd_bal decimal(22,2)") \
        .selectExpr("*", "'Credit Card' AS product_type")
    mart = gc.build_mart_card_overdue_daily(silver)
    mart.createOrReplaceTempView("gold_card_mart_overdue_daily")
    spark.createDataFrame([(date(2025, 1, 1), "USD", float(FX))],
                          "rate_date date, currencycd string, rate_to_vnd double") \
        .createOrReplaceTempView("silver_ref_fx_rates")
    published = ", ".join(f"(DATE'{d}')" for d in DATES)
    spark.sql(sv.report_calendar_body(f"SELECT * FROM VALUES {published} AS t(as_of_date)")) \
        .createOrReplaceTempView("gold_overdue_report_calendar")
    return {"silver": silver, "mart": mart}


def run(spark, name: str, report_date: str, branch: str = "all") -> list:
    return spark.sql(localize(overdue_sql("")[name], literals(report_date, branch=branch))) \
        .collect()


def _ovd(i: int) -> int:     # tổng quá hạn VND của ngày thứ i (mọi CN)
    return 300_000_000 + i * 1_000_000 + 100_000_000 + 100_000_000 + 300_000_000


def _port(i: int) -> int:
    return 1_000_000_000 + i * 10_000_000 + 300_000_000 + 200_000_000 + 400_000_000 + 300_000_000


IDX = {d: i for i, d in enumerate(DATES)}


# ---------------------------------------------------------------- Gold / reconciliation
def test_same_grain_rows_are_summed_not_dropped(views):
    m = views["mart"].filter("as_of_date = DATE'2026-11-05' AND del_grp = '11-90'").first()
    assert m.port_acc == 50 and m.port_bal == Decimal(500_000_000)


def test_silver_gold_reconciliation_exact_decimal(views):
    rec = gc.overdue_reconciliation(views["silver"], views["mart"])
    assert all(r["match"] for r in rec.values())
    assert isinstance(rec["ovd_bal"]["gold"], Decimal)
    broken = views["mart"].limit(3)
    assert not gc.overdue_reconciliation(views["silver"], broken)["port_bal"]["match"]


# ---------------------------------------------------------------- KPI + so sánh
def test_kpi_current_ratio_from_aggregated_sums(spark, views):
    r = run(spark, "ovd_kpi", "2026-11-05")[0]
    i = IDX[date(2026, 11, 5)]
    assert r.actual_date == date(2026, 11, 5)
    assert r.fx_vnd_usd == FX
    assert float(r.overdue_ratio) == pytest.approx(_ovd(i) / _port(i) * 100)
    assert float(r.overdue_usd) == pytest.approx(_ovd(i) / FX)
    assert float(r.ending_usd) == pytest.approx(_port(i) / FX)


def test_compare_dates_d1_m1_y1(spark, views):
    rows = {r.period: r for r in run(spark, "ovd_compare", "2026-11-05")}
    assert rows["D1"].actual_date == date(2026, 11, 4)
    assert rows["M1"].actual_date == date(2026, 10, 31)
    assert rows["Y1"].actual_date == date(2025, 12, 31)
    assert "2026-10-31" in rows["M1"].cmp
    cur, d1 = IDX[date(2026, 11, 5)], IDX[date(2026, 11, 4)]
    assert rows["D1"].overdue_usd == round((_ovd(cur) - _ovd(d1)) / FX)


def test_first_day_of_month_previous_is_month_end(spark, views):
    rows = {r.period: r for r in run(spark, "ovd_compare", "2026-11-01")}
    assert rows["D1"].actual_date == date(2026, 10, 31)
    assert rows["M1"].actual_date == date(2026, 10, 31)


def test_mid_month_never_reads_future_or_unpublished(spark, views):
    # 11-11 có trong mart nhưng chưa publish; 11-15 không có data
    assert run(spark, "ovd_kpi", "2026-11-15")[0].actual_date == date(2026, 11, 10)
    # chọn ngày giữa tháng lịch sử: chỉ có snapshot cuối tháng trước
    r = run(spark, "ovd_kpi", "2026-03-15")[0]
    assert r.actual_date == date(2026, 2, 28)
    mon = run(spark, "ovd_monthly_trend", "2026-03-15")
    assert max(x.actual_date for x in mon) <= date(2026, 3, 15)


def test_missing_comparison_is_na_not_zero(spark, views):
    rows = {r.period: r for r in run(spark, "ovd_compare", "2025-11-30")}
    assert rows["Y1"].actual_date is None
    assert rows["Y1"].overdue_usd is None and rows["Y1"].ratio_txt == "N/A"


def test_zero_denominator_ratio_is_null(spark, views):
    r = run(spark, "ovd_kpi", "2026-11-05", branch="103")[0]
    assert r.overdue_ratio is None and r.overdue_usd == 0


def test_ratio_respects_branch_filter(spark, views):
    r = run(spark, "ovd_kpi", "2026-11-05", branch="102")[0]
    assert float(r.overdue_ratio) == pytest.approx(25.0)


# ---------------------------------------------------------------- xu hướng
def test_last_5_days(spark, views):
    rows = run(spark, "ovd_daily_trend", "2026-11-05")
    assert [r.actual_date for r in rows] == [date(2026, 11, d) for d in range(1, 6)]
    rows = run(spark, "ovd_daily_trend", "2026-11-02")   # lùi qua các snapshot cuối tháng
    assert [r.actual_date for r in rows] == [date(2026, 8, 31), date(2026, 9, 30),
                                             date(2026, 10, 31), date(2026, 11, 1),
                                             date(2026, 11, 2)]


def test_monthly_trend_one_snapshot_per_month(spark, views):
    rows = run(spark, "ovd_monthly_trend", "2026-11-05")
    assert [r.month for r in rows] == ["2026-06", "2026-07", "2026-08", "2026-09", "2026-10",
                                       "2026-11"]
    assert rows[-1].actual_date == date(2026, 11, 5)   # tháng hiện tại = ngày báo cáo
    i = IDX[date(2026, 11, 5)]
    # 1 snapshot, không cộng các ngày trong tháng
    assert float(rows[-1].overdue_k_usd) == pytest.approx(_ovd(i) / FX / 1000)


def test_debt_groups_single_snapshot_and_total(spark, views):
    rows = run(spark, "ovd_debt_groups", "2026-11-05")
    months = sorted({r.month for r in rows})
    assert months[0] == "2026-01" and months[-1] == "2026-11" and len(months) == 11
    assert "0" not in {r.del_grp for r in rows}
    for m in months:
        snap = {r.actual_date for r in rows if r.month == m}
        assert len(snap) == 1                         # 1 ngày chốt chung cho mọi nhóm nợ
        i = IDX[snap.pop()]
        total = sum(float(r.overdue_k_usd) for r in rows if r.month == m)
        assert total == pytest.approx(_ovd(i) / FX / 1000)
    nov = {r.del_grp for r in rows if r.month == "2026-11"}
    assert nov == {"1-10", "11-90", "361+"}


# ---------------------------------------------------------------- product / branch
def test_product_amount_and_top5_ratio(spark, views):
    amt = {r.product_group: r for r in run(spark, "ovd_product", "2026-11-05")}
    assert set(amt) == {"GOLD", "CLASSIC", "PLATINUM"}
    assert amt["PLATINUM"].overdue_ratio is None
    top = run(spark, "ovd_product_top5", "2026-11-05")
    assert [r.product_group for r in top] == ["CLASSIC", "GOLD"]   # PLATINUM: mẫu = 0
    assert float(top[0].overdue_ratio) == pytest.approx(400_000_000 / 700_000_000 * 100)


def test_branch_top5_rules(spark, views):
    rows = run(spark, "ovd_branch_top5", "2026-11-05")
    names = [r.branch for r in rows]
    assert "8125" not in names            # loại CN 8125
    assert "102" not in names             # 4.000 USD <= ngưỡng 10.000 USD
    assert names == ["101"]
    assert all(r.overdue_usd > 10_000 for r in rows)
    assert run(spark, "ovd_branch_top5", "2026-11-05", branch="102") == []
