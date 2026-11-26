"""Gold thẻ: mart từ Silver phải khớp file KPI nguồn (card_kpi) và bang1247."""

from datetime import date

import pytest
from pyspark.sql import functions as F

from common.gold import build_mart_deposit_daily
from common.gold_cards import (
    build_mart_card_kpi_daily,
    build_mart_card_kpi_monthly,
    build_mart_card_spend_mix,
    card_kpi_metrics,
    deposit_reconciliation,
    reconcile,
)

AS_OF = "2026-11-01"


@pytest.fixture(scope="module")
def card_daily(results):
    return build_mart_card_kpi_daily(results["card_master"].valid,
                                     results["card_transactions_hist"].valid,
                                     results["card_statements_hist"].valid, AS_OF).cache()


@pytest.fixture(scope="module")
def recon(results, card_daily):
    return reconcile(card_kpi_metrics(card_daily), results["card_kpi_source"].valid).cache()


def test_card_daily_one_row_per_dims(card_daily):
    keys = ["as_of_date", "branchnbr", "cust_type", "acquisition_channel"]
    assert card_daily.groupBy(*keys).count().filter("count > 1").count() == 0
    assert card_daily.filter("acquisition_channel IS NULL").count() == 0


def test_every_source_metric_reconciles(recon):
    rows = recon.collect()
    assert len(rows) == 45                       # 9 file KPI, 45 mã chỉ tiêu
    bad = [(r.metric_code, r.gold_value, r.source_value, r.status) for r in rows
           if r.status not in ("MATCH", "WITHIN_TOLERANCE")]
    assert not bad, bad


def test_stock_metrics_match_exactly(recon):
    """Số thẻ, dư nợ, DPD không phụ thuộc giao dịch trùng -> phải MATCH tuyệt đối."""
    st = {r.metric_code: r.status for r in recon.collect()}
    for code in ["EOP_Total", "EOP_Consumer", "Total_Outstanding_Balance", "Revolving_Balance",
                 "DPD_1P_Balance", "DPD_30P_Member", "NPL_90P_Balance", "NPL_90P_Rate",
                 "New_Total", "Member_Closed"]:
        assert st[code] == "MATCH", code


def test_reconcile_flags_mismatch(spark):
    gold = spark.createDataFrame([(date(2026, 11, 1), "EOP_Total", 100.0),
                                  (date(2026, 11, 1), "Total_Spending_Amt", 1005.0),
                                  (date(2026, 11, 1), "Extra", 1.0)],
                                 "as_of_date date, metric_code string, gold_value double")
    src = spark.createDataFrame([(date(2026, 11, 1), "EOP_Total", 90.0, False),
                                 (date(2026, 11, 1), "Total_Spending_Amt", 1000.0, False),
                                 (date(2026, 11, 1), "Missing", 5.0, False)],
                                "trx_date date, metric_code string, metric_value double, "
                                "is_rate boolean")
    st = {r.metric_code: r.status for r in reconcile(gold, src).collect()}
    assert st == {"EOP_Total": "MISMATCH", "Total_Spending_Amt": "WITHIN_TOLERANCE",
                  "Extra": "MISSING_IN_SOURCE", "Missing": "MISSING_IN_GOLD"}


def test_spend_mix_sums_to_daily(results, card_daily):
    mix = build_mart_card_spend_mix(results["card_transactions_hist"].valid, AS_OF)
    a = mix.agg(F.sum("amount")).first()[0]
    b = card_daily.agg(F.sum("spend_amt")).first()[0]
    assert a == b


def test_deposit_reconciles_with_bang1247(results):
    from data_generator.common import DEPOSIT_ALL

    mart = build_mart_deposit_daily(results["core_accounts"].valid, AS_OF)
    r = deposit_reconciliation(mart, results["dep_summary_source"].valid,
                               results["ref_fx_rates"].valid,
                               {p[0]: p[1] for p in DEPOSIT_ALL})
    st = r.groupBy("status").count().collect()
    assert {x.status for x in st} == {"MATCH"}, r.filter("status <> 'MATCH'").limit(5).collect()


def test_monthly_stock_flow_mom_and_targets(spark):
    cols = ["eop_members", "balance", "revolving_balance", "revolving_members", "credit_limit",
            "dpd_1p_members", "dpd_1p_balance", "dpd_10p_members", "dpd_10p_balance",
            "dpd_30p_members", "dpd_30p_balance", "npl_90p_members", "npl_90p_balance",
            "new_members", "closed_members", "spend_amt", "txn_count", "purchase_amt",
            "cash_advance_amt", "installment_amt", "flexi_amt", "domestic_amt", "xborder_amt",
            "pos_amt", "online_amt", "using_members"]

    def row(d, eop, new, spend):
        v = dict.fromkeys(cols, 0)
        v.update(eop_members=eop, new_members=new, spend_amt=spend, balance=eop * 10)
        return (d, "1", "CN", "BRANCH", *[v[c] for c in cols])

    daily = spark.createDataFrame(
        [row(date(2026, 10, 31), 100, 5, 1000), row(date(2026, 11, 1), 110, 3, 500),
         row(date(2026, 11, 2), 120, 2, 700)],
        "as_of_date date, branchnbr string, cust_type string, acquisition_channel string, "
        + ", ".join(f"{c} long" for c in cols))
    targets = spark.createDataFrame(
        [("CARD_NEW_ACQ", "MONTH", "2026-11", "1", 10.0),
         ("CARD_SALES_VOLUME", "MONTH", "2026-11", "1", 2400.0),
         ("CARD_NEW_ACQ", "YEAR", "2026", "1", 100.0),
         ("CARD_SALES_VOLUME", "YEAR", "2026", "1", 10000.0)],
        "metric_code string, period_type string, period string, branchnbr string, "
        "target_value double")
    m = {r.year_month: r for r in build_mart_card_kpi_monthly(daily, targets).collect()}
    nov = m["2026-11"]
    assert nov.eop_members == 120 and nov.last_date == date(2026, 11, 2)   # tồn: ngày cuối
    assert nov.new_members == 5 and nov.spend_amt == 1200                  # luỹ kế: cộng
    assert nov.eop_members_mom_pct == pytest.approx(20.0)                  # 120 vs 100
    assert nov.eop_members_yoy_pct is None                                 # chưa có 2025-11
    assert nov.new_vs_month_target_pct == pytest.approx(50.0)
    assert nov.sales_vs_month_target_pct == pytest.approx(50.0)
    assert nov.new_vs_year_target_pct == pytest.approx(10.0)               # YTD 5+5 / 100
    assert m["2026-10"].target_new_month is None
