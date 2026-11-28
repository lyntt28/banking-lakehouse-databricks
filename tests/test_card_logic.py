"""Card Business: logic nghiệp vụ mart tháng + dataset dashboard trên dữ liệu tính tay.

- Using members = distinct thẻ MTD (không SUM / MAX theo ngày)
- Tồn (stock) tại ngày chốt, luỹ kế (flow) cộng đủ ngày; thiếu ngày -> N/A
- Chọn ngày giữa tháng: tháng hiện tại vẫn có, không đọc ngày sau
- Mart tháng incremental = dựng lại từ đầu
- Dashboard: Balance / Revolving không xếp chồng, spend / EOP, chỉ tiêu đúng loại KH,
  vs Last Year-end (tháng 12 năm trước), rủi ro so sánh bằng pp"""

import json
from datetime import date

import pytest
from dashboard_sql import literals, localize
from pyspark.sql import functions as F

from common import gold_cards as gc
from common import serving as sv
from common.lakeview import build_all
from common.lakeview.card import card_sql

FX = 25_000
COLS = gc.STOCK + gc.FLOW + ["using_members"]
NOV = [date(2026, 11, d) for d in range(1, 21)]     # có dữ liệu tới 20/11
HIST = [date(2025, 12, 31), date(2026, 10, 31)]      # lịch sử: chỉ cuối tháng


def drow(d, cust, eop, new, spend, bal=None, rev=0, dpd1=0, npl=0):
    v = dict.fromkeys(COLS, 0)
    v.update(eop_members=eop, new_members=new, spend_amt=spend, purchase_amt=spend,
             domestic_amt=spend, pos_amt=spend, balance=bal if bal is not None else eop * 1000,
             revolving_balance=rev, dpd_1p_balance=dpd1, npl_90p_balance=npl)
    return (d, "101", cust, "BRANCH", *[v[c] for c in COLS])


@pytest.fixture(scope="module")
def daily(spark):
    rows = [drow(d, "CN", 100, 1, 1000, rev=30_000, dpd1=5000) for d in HIST] + \
        [drow(d, "TC", 10, 0, 500) for d in HIST]
    for i, d in enumerate(NOV):
        rows.append(drow(d, "CN", 100 + i, 2, 1000, rev=30_000, dpd1=5000 + i * 100))
        rows.append(drow(d, "TC", 10, 1, 500))
    return spark.createDataFrame(
        rows, "as_of_date date, branchnbr string, cust_type string, acquisition_channel string, "
              + ", ".join(f"{c} long" for c in COLS))


@pytest.fixture(scope="module")
def txns(spark):
    # thẻ A dùng ngày 1, 2; B ngày 2; C ngày 3 -> daily distinct 1, 2, 1 (SUM 4, MAX 2)
    rows = [("A", date(2026, 11, 1)), ("A", date(2026, 11, 2)), ("B", date(2026, 11, 2)),
            ("C", date(2026, 11, 3)), ("D", date(2026, 11, 18))]
    t = spark.createDataFrame(rows, "card_no string, _as_of_date date")
    cards = spark.createDataFrame([(c, "101", "CN", "BRANCH") for c in "ABCD"],
                                  "card_no string, branchnbr string, cust_type string, "
                                  "acquisition_channel string")
    return t, cards


def monthly(daily, txns, day, previous=None):
    t, cards = txns
    d = date.fromisoformat(day)
    mstart = d.replace(day=1)
    mtd = t.filter((F.col("_as_of_date") >= F.lit(mstart)) & (F.col("_as_of_date") <= F.lit(d)))
    month_daily = daily.filter((F.col("as_of_date") >= F.lit(mstart)))
    extra = {} if previous is not None else {
        "history_daily": daily.filter(F.col("as_of_date") < F.lit(mstart)),
        "history_using": gc.using_members_by_month(
            t.filter(F.col("_as_of_date") < F.lit(mstart)), cards)}
    return gc.build_mart_card_kpi_monthly(
        month_daily, day, using_mtd=gc.using_members_by_month(mtd, cards),
        previous=previous, **extra).localCheckpoint()


def rows(df):
    return {(r.year_month, r.cust_type): r for r in df.collect()}


def test_using_members_distinct_mtd(daily, txns):
    m = rows(monthly(daily, txns, "2026-11-03"))
    assert m[("2026-11", "CN")].using_members == 3          # không phải 4 (SUM) hay 2 (MAX)


def test_stock_flow_and_mid_month(daily, txns):
    m = rows(monthly(daily, txns, "2026-11-15"))
    cn = m[("2026-11", "CN")]
    assert cn.snapshot_date == date(2026, 11, 15)           # dữ liệu có tới 20/11: không đọc
    assert cn.eop_members == 114 and cn.new_members == 30   # tồn ngày 15; luỹ kế 15 ngày
    assert cn.flow_complete and cn.days_expected == 15
    assert cn.using_members == 3                            # thẻ D (18/11) chưa tính
    octo = m[("2026-10", "CN")]
    assert octo.eop_members == 100 and not octo.flow_complete   # chỉ có ngày cuối tháng
    assert not cn.ytd_complete                              # tháng 1-10 thiếu ngày
    assert {r.as_of_date for r in m.values()} == {date(2026, 11, 15)}
    assert min(k[0] for k in m) == "2025-12"                # từ tháng 1 năm trước


def test_incremental_equals_rebuild(daily, txns):
    prev = monthly(daily, txns, "2026-11-15")
    inc = rows(monthly(daily, txns, "2026-11-16", previous=prev))
    full = rows(monthly(daily, txns, "2026-11-16"))
    assert inc.keys() == full.keys()
    for k in full:
        a, b = inc[k].asDict(), full[k].asDict()
        assert a == b, k


# ---------------------------------------------------------------- dashboard SQL
@pytest.fixture(scope="module")
def views(spark, daily, txns):
    daily.filter(F.col("as_of_date") <= F.lit(date(2026, 11, 15))) \
        .createOrReplaceTempView("gold_card_mart_kpi_daily")
    monthly(daily, txns, "2026-11-15").createOrReplaceTempView("gold_card_mart_kpi_monthly")
    published = ", ".join(f"(DATE'{d}')" for d in HIST + NOV[:15])
    spark.sql(sv.report_calendar_body(f"SELECT * FROM VALUES {published} AS t(as_of_date)")) \
        .createOrReplaceTempView("gold_card_report_calendar")
    spark.createDataFrame([(date(2020, 1, 1), "USD", float(FX))],
                          "rate_date date, currencycd string, rate_to_vnd double") \
        .createOrReplaceTempView("silver_ref_fx_rates")
    tg = [("CARD_NEW_ACQ_CN", "MONTH", "2026-11", "101", 60.0),
          ("CARD_NEW_ACQ_TC", "MONTH", "2026-11", "101", 1000.0),
          ("CARD_CUSTOMER_BASE_CN", "YEAR", "2026", "101", 228.0),
          ("CARD_BALANCE_CN", "MONTH", "2026-11", "101", 228_000.0)]
    spark.createDataFrame(tg, "metric_code string, period_type string, period string, "
                              "branchnbr string, target_value double") \
        .createOrReplaceTempView("silver_ref_targets")


def run(spark, name, report_date, customer_type="consumer"):
    sql = localize(card_sql("")[name], literals(report_date, customer_type=customer_type))
    return spark.sql(sql).collect()


def test_kpi_mtd_and_stock(spark, views):
    r = run(spark, "card_kpi", "2026-11-17")[0]               # chưa publish 16/17 -> 15/11
    assert r.report_date == date(2026, 11, 15)
    assert (r.new_acquisition, r.customer_base, r.using_customer) == (30, 114, 3)
    assert float(r.sales_volume_k_usd) == pytest.approx(15_000 / FX / 1000)
    assert float(r.dpd_1p) == pytest.approx((5000 + 14 * 100) / 114_000 * 100)
    allc = run(spark, "card_kpi", "2026-11-15", "all")[0]
    assert allc.customer_base == 124 and allc.new_acquisition == 45


def test_compare_last_month_year_end_and_targets(spark, views):
    c = {r.line: r for r in run(spark, "card_kpi_compare", "2026-11-15")}
    assert "2026-10" in c[1].compare and "2025-12" in c[2].compare
    assert c[1].new_acquisition is None                       # tháng 10 thiếu ngày -> N/A
    assert float(c[1].customer_base) == pytest.approx(14.0)   # 114 vs 100
    assert float(c[3].new_acquisition) == pytest.approx(50.0)  # 30 / 60 (chỉ tiêu CN, không TC)
    assert float(c[3].balance) == pytest.approx(50.0)
    assert float(c[4].customer_base) == pytest.approx(50.0)   # tồn: hiện tại / chỉ tiêu cuối năm
    assert c[4].new_acquisition is None                       # YTD thiếu tháng -> N/A
    assert c[4].using_customer is None                        # không có chỉ tiêu năm


def test_trend_six_points_and_spend_per_member(spark, views):
    vol = run(spark, "card_ticket", "2026-11-15")
    assert {r.month for r in vol} <= {f"2026-{m:02d}" for m in range(6, 12)}
    nov = next(r for r in vol if r.month == "2026-11")
    assert float(nov.spend_per_member) == pytest.approx(15_000 / FX / 114)   # / EOP


def test_risk_compare_in_pp(spark, views):
    r = {x.line: x for x in run(spark, "card_risk_compare", "2026-11-15")}
    cur, d1 = (5000 + 1400) / 114_000 * 100, (5000 + 1300) / 113_000 * 100
    assert float(r[1].dpd_1p) == pytest.approx(round(cur - d1, 2))
    assert "2026-10-31" in r[2].compare


def test_balance_chart_grouped_not_stacked():
    body = build_all("")["card_business.lvdash.json"]
    spec = next(i["widget"]["spec"] for p in body["pages"] for i in p["layout"]
                if (i["widget"].get("spec", {}).get("frame", {}).get("title") or {})
                .get("value") == "Balance (K USD)")
    vl = json.loads(spec["jsonSpec"]["spec"])
    enc = vl["layer"][0]["encoding"]
    assert "xOffset" in enc and enc["y"]["stack"] is None, json.dumps(vl)[:200]
