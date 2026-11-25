"""Mỗi view dashboard phải chạy được trên đúng schema của mart (dựng từ dữ liệu sinh 1 ngày)."""

import pytest
from pyspark.sql import functions as F

from common import gold as g
from common import gold_cards as gc
from common import gold_deposit as gd
from common.dashboard_views import VIEWS, view_statements

AS_OF = "2026-11-01"


@pytest.fixture(scope="module")
def marts(spark, results):
    r = {t: x.valid for t, x in results.items()}
    cust = r["customers"].withColumn("valid_from", F.lit(AS_OF).cast("date")) \
        .withColumn("valid_to", F.lit(None).cast("date")).withColumn("is_current", F.lit(True))
    dim_customer = g.build_dim_customer(cust)
    dim_product = g.build_dim_product(r["products"])
    fact_loan = g.build_fact_loan_daily(r["loans"], dim_customer)
    overdue = g.build_mart_loan_overdue_daily(fact_loan, dim_product)
    card_daily = gc.build_mart_card_kpi_daily(r["cards"], r["card_transactions"],
                                              r["card_statements"], AS_OF)
    m = {
        "dim_branch": g.build_dim_branch(r["branches"]),
        "mart_loan_overdue_daily": overdue,
        "mart_npl_daily": g.build_mart_npl_daily(overdue),
        "mart_card_kpi_daily": card_daily,
        "mart_card_spend_mix": gc.build_mart_card_spend_mix(r["card_transactions"], AS_OF),
        "mart_card_kpi_monthly": gc.build_mart_card_kpi_monthly(card_daily, r["targets"]),
        "mart_deposit_trend": gd.build_mart_deposit_trend(r["accounts"], dim_product,
                                                          r["fx_rates"], r["branches"], AS_OF),
        "mart_deposit_by_tenor": gd.build_mart_deposit_by_tenor(r["accounts"], dim_product,
                                                                r["fx_rates"], AS_OF),
        "mart_casa_customer": gd.build_mart_casa_customer(r["accounts"], dim_product,
                                                          dim_customer, AS_OF),
        "mart_deposit_new_accounts": gd.build_mart_deposit_new_accounts(r["accounts"]),
        "targets": r["targets"],
    }
    for name, df in m.items():
        df.createOrReplaceTempView(name)
    return m


def _local(sql: str) -> str:
    return sql.replace("{catalog}.gold.", "").replace("{catalog}.silver.", "")


@pytest.mark.parametrize("view", VIEWS, ids=[v[2] for v in VIEWS])
def test_view_runs_and_returns_rows(spark, marts, view):
    dash, order, name, desc, sql = view
    rows = spark.sql(_local(sql)).collect()
    assert rows, f"{name} ({desc}) trả về 0 dòng"


def test_three_dashboards_in_order():
    by = {}
    for dash, order, *_ in VIEWS:
        by.setdefault(dash, []).append(order)
    assert set(by) == {"card", "overdue", "deposit"}
    assert all(sorted(v) == list(range(1, len(v) + 1)) for v in by.values())


def test_view_statements_qualified():
    sql = view_statements("bank_dev")
    assert len(sql) == len(VIEWS)
    assert all(s.startswith("CREATE OR REPLACE VIEW bank_dev.gold.v_") for s in sql)
    assert not any("{catalog}" in s for s in sql)


def test_card_kpi_view_values(spark, marts):
    sql = next(v[4] for v in VIEWS if v[2] == "v_card_kpi_cards")
    r = spark.sql(_local(sql)).first()
    total = marts["mart_card_kpi_daily"].agg(F.sum("eop_members")).first()[0]
    assert r.customer_base == total
    assert r.new_vs_month_target_pct is not None   # có chỉ tiêu tháng 11/2026


def test_deposit_table_shares_sum_to_100(spark, marts):
    sql = next(v[4] for v in VIEWS if v[2] == "v_deposit_account_group_table")
    rows = spark.sql(_local(sql)).collect()
    assert sum(float(r.share_pct) for r in rows) == pytest.approx(100)
    assert {r.group_class for r in rows} == {"1-CASA", "2-TD", "3-ISA"}
