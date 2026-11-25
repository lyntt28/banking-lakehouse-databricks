"""Gold: star schema + mart, tính từ kết quả Silver của conftest."""

from datetime import date

import pytest
from pyspark.sql import functions as F

from common.gold import (
    build_dim_customer,
    build_dim_date,
    build_dim_product,
    build_fact_loan_daily,
    build_fact_transaction,
    build_mart_customer_360,
    build_mart_deposit_daily,
    build_mart_loan_overdue_daily,
    build_mart_npl_daily,
)

AS_OF = "2026-11-01"


@pytest.fixture(scope="module")
def gold(results):
    # silver.customers sau lần SCD2 đầu tiên: mọi bản ghi là bản hiện hành
    cust = results["customers"].valid.withColumn("valid_from", F.lit(AS_OF).cast("date")) \
        .withColumn("valid_to", F.lit(None).cast("date")).withColumn("is_current", F.lit(True))
    g = {"dim_customer": build_dim_customer(cust)}
    g["dim_product"] = build_dim_product(results["products"].valid)
    g["fact_loan"] = build_fact_loan_daily(results["loans"].valid, g["dim_customer"])
    g["fact_txn"] = build_fact_transaction(results["transactions"].valid,
                                           results["accounts"].valid, g["dim_customer"])
    g["overdue"] = build_mart_loan_overdue_daily(g["fact_loan"], g["dim_product"])
    return g


def test_dim_date(spark):
    d = build_dim_date(spark, "2025-12-30", "2026-01-02").orderBy("date").collect()
    assert [r.date_key for r in d] == [20251230, 20251231, 20260101, 20260102]
    assert [r.is_year_end for r in d] == [False, True, False, False]
    assert d[1].is_month_end and not d[0].is_month_end


def test_fact_loan_grain_and_balance(results, gold):
    f = gold["fact_loan"]
    assert f.count() == results["loans"].valid.count()
    assert f.groupBy("as_of_date", "acctnbr").count().filter("count > 1").count() == 0
    assert f.filter("date_key <> 20261101").count() == 0


def test_fact_loan_links_customer_version(gold):
    f = gold["fact_loan"]
    # khoản vay của khách bị quarantine thì không có customer_sk; còn lại phải có
    assert f.filter("customer_sk IS NOT NULL").count() > 0.9 * f.count()


def test_fact_transaction_signed_amount(results, gold):
    f = gold["fact_txn"]
    assert f.count() == results["transactions"].valid.count()
    assert f.filter("(dr_cr = 'D' AND signed_amount >= 0) OR "
                    "(dr_cr = 'C' AND signed_amount <= 0)").count() == 0


def test_mart_overdue_reconciles_with_fact(gold):
    tot = gold["fact_loan"].agg(F.sum("balance_vnd")).first()[0]
    mart = gold["overdue"].agg(F.sum("portfolio_balance"), F.sum("overdue_balance"),
                               F.sum("npl_balance")).first()
    assert mart[0] == tot
    assert 0 < mart[2] <= mart[1] <= mart[0]   # NPL ⊂ quá hạn ⊂ toàn danh mục


def test_mart_npl_daily_comparisons(spark):
    rows = [(date(2025, 12, 31), 1000, 100, 50), (date(2026, 10, 30), 1000, 100, 20),
            (date(2026, 11, 2), 1000, 100, 30), (date(2026, 11, 3), 1000, 100, 40)]
    m = spark.createDataFrame(
        rows, "as_of_date date, portfolio_balance long, overdue_balance long, npl_balance long")
    r = {x.as_of_date: x for x in build_mart_npl_daily(m).collect()}
    d = r[date(2026, 11, 3)]
    assert d.npl_ratio == 0.04
    assert d.npl_ratio_d1 == 0.03     # 02/11
    assert d.npl_ratio_m1 == 0.02     # cuối tháng 10 không có file -> ngày gần nhất 30/10
    assert d.npl_ratio_y1 == 0.05     # 31/12/2025
    assert r[date(2025, 12, 31)].npl_ratio_d1 is None


def test_mart_deposit_excludes_loans(results):
    m = build_mart_deposit_daily(results["accounts"].valid, AS_OF)
    assert m.filter("product_type = 'LOAN'").count() == 0
    src = results["accounts"].valid.filter("product_type <> 'LOAN'")
    assert m.agg(F.sum("account_count")).first()[0] == src.count()


def test_customer_360(results, gold):
    m = build_mart_customer_360(gold["dim_customer"], gold["fact_loan"],
                                results["accounts"].valid, gold["fact_txn"], AS_OF)
    assert m.count() == gold["dim_customer"].count()
    assert m.groupBy("customer_id").count().filter("count > 1").count() == 0
    assert m.filter("loan_count > 0").count() > 0
    assert m.filter("has_npl AND worst_debt_group < 3").count() == 0
