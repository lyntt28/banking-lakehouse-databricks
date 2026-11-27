"""Gold tiền gửi: dữ liệu có lịch sử cuối tháng (history_months) để kiểm tra M-1 / xu hướng."""

from datetime import date

import pytest
from pyspark.sql import functions as F

from common.bronze import add_bronze_metadata
from common.gold_deposit import (
    build_mart_casa_customer,
    build_mart_deposit_by_tenor,
    build_mart_deposit_new_accounts,
    build_mart_deposit_trend,
    compare_with_history,
)
from common.silver import TABLES, prepare_silver
from data_generator.generate import Sizes, generate, month_ends_before

DATES = ["2026-09-30", "2026-10-31", "2026-11-01"]


def test_month_ends_before():
    assert month_ends_before(date(2026, 1, 15), 3) == [date(2025, 10, 31), date(2025, 11, 30),
                                                        date(2025, 12, 31)]
    assert month_ends_before(date(2026, 3, 1), 0) == []


@pytest.fixture(scope="module")
def silver_days(spark, tmp_path_factory):
    """{ngày: {bảng: DataFrame silver}} cho 2 mốc cuối tháng + 1 ngày thường."""
    out = tmp_path_factory.mktemp("hist")
    generate(out, date(2026, 11, 1), 1, seed=3,
             sizes=Sizes(400, 600, 100, 100, 100), history_months=2)

    def read(t):
        df = spark.read.option("header", "true").csv(str(out / t))
        return add_bronze_metadata(df, F.input_file_name())

    br = {t: read(t) for t in ["branches", "products", "customers", "accounts", "fx_rates"]}
    res = {}
    for d in DATES:
        day = {t: br[t].filter(F.col("_as_of_date") == d) for t in br}
        cust = prepare_silver(day["customers"], TABLES["core_customers"])
        acc = prepare_silver(day["accounts"], TABLES["core_accounts"],
                             cust.known_keys("customer_id"))
        res[d] = {"customers": cust.valid, "accounts": acc.valid,
                  "fx": prepare_silver(day["fx_rates"], TABLES["ref_fx_rates"]).valid}
    first = {t: br[t].filter(F.col("_as_of_date") == DATES[0]) for t in ["branches", "products"]}
    res["products"] = prepare_silver(first["products"], TABLES["ref_products"]).valid
    res["branches"] = prepare_silver(first["branches"], TABLES["ref_branches"]).valid
    return res


def test_history_dates_generated(silver_days):
    assert all(silver_days[d]["accounts"].count() > 0 for d in DATES)


def test_new_products_and_tenors(silver_days):
    acc = silver_days[DATES[-1]]["accounts"]
    types = {r.product_type for r in acc.select("product_type").distinct().collect()}
    assert {"ISA", "EDUSAVE", "TD", "CASA"} <= types
    assert acc.filter("product_type IN ('TD', 'ISA') AND tenor_months = 0").count() == 0
    assert acc.filter("product_type IN ('CASA', 'EDUSAVE') AND tenor_months <> 0").count() == 0


def test_trend_with_month_comparisons(silver_days):
    s = silver_days
    hist = None
    for d in DATES:
        m = build_mart_deposit_trend(s[d]["accounts"], s["products"], s[d]["fx"],
                                     s["branches"], d, history=hist)
        hist = m if hist is None else hist.unionByName(m)
    last = hist.filter(F.col("as_of_date") == DATES[-1])
    assert last.filter("group_class IS NULL OR account_group IS NULL").count() == 0
    assert last.agg(F.sum("share_pct")).first()[0] == pytest.approx(100)
    tot = last.agg(F.sum("balance_vnd"), F.sum("balance_vnd_d1"), F.sum("balance_vnd_m1"),
                   F.sum("balance_vnd_y1")).first()
    oct31 = hist.filter(F.col("as_of_date") == DATES[1]).agg(F.sum("balance_vnd")).first()[0]
    sep30 = hist.filter(F.col("as_of_date") == DATES[0]).agg(F.sum("balance_vnd")).first()[0]
    assert float(tot[1]) == pytest.approx(float(oct31), rel=0.02)  # D-1 = 31/10
    assert float(tot[2]) == pytest.approx(float(oct31), rel=0.02)   # M-1 = cuối tháng 10
    assert tot[3] is None                                    # chưa có 31/12/2025
    m1_of_oct = hist.filter(F.col("as_of_date") == DATES[1]).agg(
        F.sum("balance_vnd_m1")).first()[0]
    assert float(m1_of_oct) == pytest.approx(float(sep30), rel=0.02)


def test_compare_with_history_skips_missing_days(spark):
    df = spark.createDataFrame(
        [(date(2026, 10, 30), "A", 10.0), (date(2026, 11, 3), "A", 30.0),
         (date(2026, 11, 3), "B", 5.0)], "as_of_date date, k string, v double")
    r = {(x.as_of_date, x.k): x for x in compare_with_history(df, ["k"], ["v"]).collect()}
    a = r[(date(2026, 11, 3), "A")]
    assert a.v_d1 == 10.0 and a.v_m1 == 10.0 and a.v_y1 is None
    assert r[(date(2026, 11, 3), "B")].v_d1 is None


def test_by_tenor(silver_days):
    s = silver_days
    t = build_mart_deposit_by_tenor(s[DATES[-1]]["accounts"], s["products"], s[DATES[-1]]["fx"],
                                    DATES[-1]).collect()
    assert {r.tenor_months for r in t} <= {1, 3, 6, 12, 24, 36}
    by = {}
    for r in t:
        if r.product_type == "TD" and r.currencycd == "VND":
            by[r.tenor_months] = float(r.avg_rate)
    assert by[36] > by[1]                    # lãi suất tăng theo kỳ hạn
    assert all(r.min_rate <= r.avg_rate <= r.max_rate for r in t)


def test_casa_customer(silver_days):
    s = silver_days
    m = build_mart_casa_customer(s[DATES[-1]]["accounts"], s["products"],
                                 s[DATES[-1]]["customers"], DATES[-1]).collect()
    assert m and all(r.casa_customers <= r.retail_customers for r in m)
    assert all(r.casa_active_customers + r.casa_dormant_customers == r.casa_customers
               for r in m)


def test_new_accounts_ytd(silver_days):
    m = build_mart_deposit_new_accounts(silver_days[DATES[-1]]["accounts"])
    k = m.filter("product_type = 'EDUSAVE'").orderBy("branchnbr", "open_month").collect()
    assert k
    run = {}
    for r in k:
        key = (r.branchnbr, r.open_month[:4])
        run[key] = run.get(key, 0) + r.new_accounts
        assert r.new_accounts_ytd == run[key]
