"""Retail Deposit: logic nghiệp vụ trên dữ liệu tổng hợp nhiều ngày (tính tay được).

Gold: D-1 / M-1 / Y-1 bằng snapshot + equality join, khoản đã tất toán, sản phẩm mới, MTD
average, mart tháng, lãi suất bình quân gia quyền, CASA theo KH hiệu lực, TK mở mới.
Dashboard: chạy SQL dataset thật (common.lakeview.deposit.deposit_sql) trên temp view."""

from datetime import date
from decimal import Decimal

import pytest
from dashboard_sql import literals, localize

from common import gold_deposit as gd
from common import serving as sv
from common.lakeview.deposit import deposit_sql

FX = 25_000
PRODUCTS = [("CASA", "Current Account", "1-CASA", "DEPOSIT"),
            ("TD", "Term Deposit", "2-TD", "DEPOSIT"),
            ("ISA", "ISA", "3-ISA", "DEPOSIT")]
ACC = ("account_id string, customer_id string, branchnbr string, product_type string, "
       "currencycd string, status string, open_date date, balance decimal(22,2), "
       "int_rate double, tenor_months int")


def acc(aid, prod, bal, branch="101", cur="VND", status="ACTIVE", cust="c1", opened=None,
        rate=0.01, tenor=0):
    return (aid, cust, branch, prod, cur, status, opened or date(2020, 1, 1),
            Decimal(bal), rate, tenor)


@pytest.fixture(scope="module")
def ref(spark):
    return {
        "products": spark.createDataFrame(
            PRODUCTS, "prodcode string, product_name string, product_group string, "
                      "product_family string"),
        "fx": spark.createDataFrame([("VND", 1.0), ("USD", float(FX))],
                                    "currencycd string, rate_to_vnd double"),
        "branches": spark.createDataFrame([("101", "North"), ("201", "South 1")],
                                          "branchnbr string, region string"),
    }


# số dư theo ngày: CASA tăng dần; TD tất toán ngày 2026-11-03; ISA mở mới ngày 2026-11-02
SNAPSHOTS = {
    "2025-12-31": [acc("a", "CASA", 50), acc("b", "TD", 400, tenor=6)],
    "2026-10-31": [acc("a", "CASA", 90), acc("b", "TD", 400, tenor=6)],
    "2026-11-01": [acc("a", "CASA", 100), acc("b", "TD", 400, tenor=6)],
    "2026-11-02": [acc("a", "CASA", 200), acc("b", "TD", 400, tenor=6),
                   acc("c", "ISA", 60, tenor=12)],
    "2026-11-03": [acc("a", "CASA", 300), acc("c", "ISA", 60, tenor=12)],
}


def chain(spark, ref, days):
    hist = None
    out = {}
    for d in days:
        a = spark.createDataFrame(SNAPSHOTS[d], ACC)
        m = gd.build_mart_deposit_trend(a, ref["products"], ref["fx"], ref["branches"], d,
                                        history=hist).localCheckpoint()   # cắt lineage
        out[d] = m
        hist = m if hist is None else hist.unionByName(m)
    return out, hist


@pytest.fixture(scope="module")
def trend(spark, ref):
    return chain(spark, ref, list(SNAPSHOTS))


def by_group(df):
    return {r.group_class: r for r in df.collect()}


# ---------------------------------------------------------------- ngày so sánh
def test_resolve_compare_dates():
    ds = ["2025-12-31", "2026-10-31", "2026-11-01", "2026-11-02", "2026-11-05"]
    r = gd.resolve_compare_dates(ds, "2026-11-03")
    assert r == {"d1": date(2026, 11, 2), "m1": date(2026, 10, 31), "y1": date(2025, 12, 31)}
    r = gd.resolve_compare_dates(["2026-09-30"], "2026-11-03")   # không có tháng 10
    assert r["m1"] is None and r["y1"] is None and r["d1"] == date(2026, 9, 30)


# ---------------------------------------------------------------- dep_mart_trend
def test_d1_m1_y1_and_closed_product_kept(trend):
    m = by_group(trend[0]["2026-11-03"])
    td = m["2-TD"]                       # đã tất toán: vẫn có dòng, số dư 0
    assert td.balance_vnd == 0 and td.balance_vnd_d1 == 400 and td.diff_d1 == -400
    assert td.balance_vnd_m1 == 400 and td.diff_y1 == -400
    casa = m["1-CASA"]
    assert (casa.diff_d1, casa.diff_m1, casa.diff_y1) == (100, 210, 250)
    assert (casa.comparison_date_d1, casa.comparison_date_m1, casa.comparison_date_y1) == (
        date(2026, 11, 2), date(2026, 10, 31), date(2025, 12, 31))


def test_new_product_compares_with_zero_not_null(trend):
    isa = by_group(trend[0]["2026-11-02"])["3-ISA"]
    assert isa.balance_vnd_d1 == 0 and isa.diff_d1 == 60     # snapshot D-1 có, SP chưa có


def test_missing_snapshot_is_null(trend):
    first = by_group(trend[0]["2025-12-31"])["1-CASA"]
    assert first.balance_vnd_d1 is None and first.diff_y1 is None
    assert first.comparison_date_y1 is None


def test_mtd_average_complete_and_additive(trend):
    m = trend[0]["2026-11-03"]
    assert all(r.monthly_avg_complete for r in m.collect())
    casa = by_group(m)["1-CASA"]
    assert casa.monthly_avg_balance_vnd == pytest.approx(Decimal(200))   # (100+200+300)/3
    total = sum(r.monthly_avg_balance_vnd for r in m.collect())
    assert float(total) == pytest.approx((500 + 660 + 360) / 3)          # tổng 3 ngày / 3


def test_mtd_average_incomplete_when_day_missing(spark, ref):
    out, _ = chain(spark, ref, ["2026-11-01", "2026-11-03"])   # thiếu 2026-11-02
    r = out["2026-11-03"].collect()
    assert all(x.monthly_avg_complete is False and x.monthly_avg_balance_vnd is None for x in r)
    hist_only = by_group(chain(spark, ref, ["2026-10-31"])[0]["2026-10-31"])["1-CASA"]
    assert hist_only.monthly_avg_balance_vnd is None   # chỉ có snapshot cuối tháng


# ---------------------------------------------------------------- dep_mart_monthly
def test_monthly_mart_incremental_and_no_future(trend):
    days, hist = trend
    first = gd.build_mart_deposit_monthly(days["2026-11-01"], "2026-11-01",
                                          trend_history=hist.filter("as_of_date < '2026-11-01'"))
    mid = gd.build_mart_deposit_monthly(days["2026-11-02"], "2026-11-02", previous=first)
    rows = {(r.month_id, r.group_class): r for r in mid.collect()}
    nov = rows[("2026-11", "1-CASA")]
    assert nov.snapshot_date == date(2026, 11, 2) and nov.ending_balance_vnd == 200
    assert not nov.is_month_closed                       # không đọc ngày sau 11-02
    octo = rows[("2026-10", "1-CASA")]
    assert octo.ending_balance_vnd == 90 and octo.is_month_closed
    assert octo.monthly_avg_balance_vnd is None          # chỉ có cuối tháng -> không bịa avg
    assert rows[("2025-12", "1-CASA")].ending_balance_vnd == 50   # tháng năm trước
    assert {r.as_of_date for r in mid.collect()} == {date(2026, 11, 2)}


# ---------------------------------------------------------------- tenor / CASA / new
def test_tenor_weighted_rate_and_buckets(spark, ref):
    a = spark.createDataFrame([
        acc("t1", "TD", 100, rate=0.05, tenor=1), acc("t2", "TD", 300, rate=0.07, tenor=3),
        acc("t3", "TD", 50, rate=0.06, tenor=6), acc("t4", "ISA", 50, rate=0.08, tenor=24),
        acc("k", "CASA", 999, rate=0.001)], ACC)
    t = gd.build_mart_deposit_by_tenor(a, ref["products"], ref["fx"], "2026-11-03",
                                       ref["branches"]).collect()
    b = {}
    for r in t:
        x = b.setdefault(r.tenor_bucket, [0.0, 0.0, r.tenor_sort_order])
        x[0] += float(r.interest_weighted_numerator)
        x[1] += float(r.interest_weighted_denominator)
    assert set(b) == {"01-06M", "06-12M", "24M+"}          # CASA không có kỳ hạn -> bỏ
    assert b["01-06M"][0] / b["01-06M"][1] == pytest.approx(0.065)   # gia quyền, không AVG
    assert b["01-06M"][2] < b["06-12M"][2] < b["24M+"][2]
    assert all(r.min_rate <= r.weighted_rate <= r.max_rate for r in t)


def test_casa_customers_as_of_report_date(spark, ref):
    cust = spark.createDataFrame([
        ("c1", "CN", "101", date(2020, 1, 1), date(2026, 10, 31)),   # cũ: chuyển CN
        ("c1", "CN", "201", date(2026, 11, 1), None),
        ("c2", "CN", "101", date(2020, 1, 1), None),
        ("c3", "CN", "101", date(2020, 1, 1), None),
        ("c4", "TC", "101", date(2020, 1, 1), None)],
        "customer_id string, cust_type string, branchnbr string, valid_from date, "
        "valid_to date")
    a = spark.createDataFrame([
        acc("x1", "CASA", 1, cust="c1"), acc("x2", "CASA", 1, cust="c2", status="DORMANT"),
        acc("x3", "TD", 1, cust="c3", tenor=6)], ACC)
    old = {r.branchnbr: r for r in gd.build_mart_casa_customer(
        a, ref["products"], cust, "2026-10-15", ref["branches"]).collect()}
    assert old["101"].retail_customers == 3                     # c1 còn ở CN 101
    new = {r.branchnbr: r for r in gd.build_mart_casa_customer(
        a, ref["products"], cust, "2026-11-03", ref["branches"]).collect()}
    assert new["201"].retail_customers == 1 and new["201"].region == "South 1"
    r = new["101"]
    assert (r.retail_customers, r.casa_active_customers, r.casa_dormant_customers,
            r.non_casa_customers) == (2, 0, 1, 1)
    assert r.casa_active_customers + r.casa_dormant_customers == r.casa_customers


def test_new_accounts_once_no_future_ytd_reset(spark):
    rows = [acc("e1", "EDUSAVE", 1, opened=date(2025, 12, 5)),
            acc("e2", "EDUSAVE", 1, opened=date(2026, 1, 10)),
            acc("e2", "EDUSAVE", 1, opened=date(2026, 1, 10)),      # snapshot trùng
            acc("e3", "EDUSAVE", 1, opened=date(2026, 2, 3)),
            acc("e4", "EDUSAVE", 1, opened=date(2026, 11, 20)),     # sau ngày báo cáo
            acc("e0", "EDUSAVE", 1, opened=date(2023, 6, 1))]       # trước năm trước
    m = {r.open_month: r for r in gd.build_mart_deposit_new_accounts(
        spark.createDataFrame(rows, ACC), "2026-11-03").collect()}
    assert set(m) == {"2025-12", "2026-01", "2026-02"}
    assert m["2026-01"].new_accounts == 1 and m["2026-01"].new_accounts_ytd == 1   # reset T1
    assert m["2026-02"].new_accounts_ytd == 2


# ---------------------------------------------------------------- dashboard SQL
@pytest.fixture(scope="module")
def views(spark, ref, trend):
    _, hist = trend
    hist.createOrReplaceTempView("gold_dep_mart_trend")
    spark.sql(sv.report_calendar_body("SELECT DISTINCT as_of_date FROM gold_dep_mart_trend")) \
        .createOrReplaceTempView("gold_deposit_report_calendar")
    spark.createDataFrame([(date(2020, 1, 1), "USD", float(FX))],
                          "rate_date date, currencycd string, rate_to_vnd double") \
        .createOrReplaceTempView("silver_ref_fx_rates")
    spark.createDataFrame(
        [("DEPOSIT_GROWTH_ALL", "YEAR", "2026", "101", 500.0),
         ("DEPOSIT_GROWTH_CASA", "YEAR", "2026", "101", 125.0),
         ("DEPOSIT_GROWTH_TD_ISA", "YEAR", "2026", "101", 100.0)],
        "metric_code string, period_type string, period string, branchnbr string, "
        "target_value double").createOrReplaceTempView("silver_ref_targets")
    ref["branches"].createOrReplaceTempView("gold_dim_branch")
    spark.createDataFrame(
        [(date.fromisoformat(d), "101", "North", 10 + i, 5, 3, 2, 5)
         for i, d in enumerate(SNAPSHOTS)],
        "as_of_date date, branchnbr string, region string, retail_customers long, "
        "casa_customers long, casa_active_customers long, casa_dormant_customers long, "
        "non_casa_customers long").createOrReplaceTempView("gold_dep_mart_casa_customer")


def run(spark, name, report_date, **p):
    params = {"region": "all", "branch": "all", "product": "all", "currency": "all",
              "nationality": "all", **p}
    return spark.sql(localize(deposit_sql("")[name], literals(report_date, **params))).collect()


def test_tickers_use_resolved_date_and_mtd_avg(spark, views):
    r = run(spark, "dep_tickers", "2026-11-04")[0]             # chưa publish 11-04 -> 11-03
    assert r.report_date == date(2026, 11, 3)
    assert float(r.ending_balance) == pytest.approx(360 / FX / 1000)
    assert float(r.monthly_avg_balance) == pytest.approx(1520 / 3 / FX / 1000)
    assert r.retail_customers == 14
    hist = run(spark, "dep_tickers", "2026-10-31")[0]
    assert hist.monthly_avg_balance is None                    # tháng thiếu ngày -> N/A


def test_compare_absolute_differences(spark, views):
    rows = {r.ord: r for r in run(spark, "dep_compare", "2026-11-03")}
    assert "2026-11-02" in rows[1].compare and "2025-12-31" in rows[3].compare
    assert rows[1].ending_balance == round((360 - 660) / FX / 1000)
    assert rows[1].retail_customers == 1 and rows[3].retail_customers == 4
    first = {r.ord: r for r in run(spark, "dep_compare", "2025-12-31")}
    assert first[3].ending_balance is None and "N/A" in first[3].compare


def test_gauges_growth_over_target(spark, views):
    r = run(spark, "dep_gauges", "2026-11-03")[0]
    # tăng trưởng từ 31/12/2025: tổng 360 - 450 = -90; CASA 300 - 50 = 250; TD+ISA 60 - 400
    assert float(r.all_pct) == pytest.approx(-90 / 500 * 100)
    assert float(r.casa_pct) == pytest.approx(250 / 125 * 100)
    assert float(r.tda_pct) == pytest.approx(-340 / 100 * 100)


def test_by_product_grouping_sets(spark, views):
    rows = run(spark, "dep_by_product", "2026-11-03")
    assert rows[-1].product == "GRAND TOTAL" and rows[-1].portion == 100
    detail = [r for r in rows if r.level == 2]
    sub = [r for r in rows if r.level == 1]
    assert sum(float(r.portion) for r in sub) == pytest.approx(100, abs=0.2)
    assert len(detail) == len(sub) == 3
    assert rows[0].level == 1 and rows[1].level == 2          # lớp rồi tới nhóm TK của lớp


def test_daily30_only_window_and_product_filter(spark, views):
    rows = run(spark, "dep_daily30", "2026-11-03", product="1-CASA")
    assert [r.as_of_date for r in rows] == [date(2026, 10, 31), date(2026, 11, 1),
                                            date(2026, 11, 2), date(2026, 11, 3)]
    assert float(rows[-1].balance) == pytest.approx(300 / FX / 1000)
