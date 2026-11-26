from common.dq import Rule, apply_dq, dq_summary, should_stop

RULES = [
    Rule("loan_id_not_null", "loan_id IS NOT NULL"),
    Rule("balance_non_negative", "outstanding_balance >= 0"),
    Rule("rate_reasonable", "interest_rate BETWEEN 0 AND 40", severity="WARN"),
]


def _df(spark):
    rows = [
        ("L1", 100.0, 10.0),   # hợp lệ
        ("L2", -5.0, 10.0),    # FAIL: dư nợ âm
        (None, 50.0, 12.0),    # FAIL: thiếu loan_id
        ("L4", 70.0, 55.0),    # WARN: lãi suất bất thường -> vẫn đi tiếp
    ]
    return spark.createDataFrame(
        rows, "loan_id string, outstanding_balance double, interest_rate double"
    )


def test_apply_dq_splits_valid_and_quarantine(spark):
    valid, quarantine = apply_dq(_df(spark), RULES)
    assert sorted(r.loan_id for r in valid.collect()) == ["L1", "L4"]
    assert quarantine.count() == 2
    assert valid.columns == ["loan_id", "outstanding_balance", "interest_rate"]


def test_row_counts_reconcile(spark):
    df = _df(spark)
    valid, quarantine = apply_dq(df, RULES)
    assert valid.count() + quarantine.count() == df.count()


def test_quarantine_keeps_reason(spark):
    _, quarantine = apply_dq(_df(spark), RULES)
    reasons = {r.loan_id: r._failed_rules for r in quarantine.collect()}
    assert reasons["L2"] == ["balance_non_negative"]
    assert reasons[None] == ["loan_id_not_null"]


def test_summary_and_stop_threshold(spark):
    summary = dq_summary(_df(spark), RULES, "silver.loan_snapshots")
    by_rule = {r.rule_name: r for r in summary.collect()}
    assert by_rule["balance_non_negative"].failed_rows == 1
    assert by_rule["balance_non_negative"].fail_pct == 25.0
    assert by_rule["rate_reasonable"].status == "WARN"
    assert should_stop(summary, max_fail_pct=5.0) is True
    assert should_stop(summary, max_fail_pct=30.0) is False
