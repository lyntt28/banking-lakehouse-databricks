"""Silver: test trên dữ liệu sinh bởi data_generator (đọc như Bronze: mọi cột string)."""


import pytest
from pyspark.sql import functions as F

from common.dq import dq_summary
from common.silver import (
    CUSTOMERS,
    ORDER,
    TABLES,
    add_dup_key_count,
    merge_condition,
    prepare_silver,
    quarantine_records,
)

AS_OF = "2026-11-01"


@pytest.mark.parametrize("table", ORDER)
def test_row_counts_reconcile(bronze, results, table):
    r = results[table]
    assert bronze[table].count() == r.valid.count() + r.quarantine.count() + r.exact_dupes


@pytest.mark.parametrize("table", ORDER)
def test_valid_keys_unique_and_not_null(results, table):
    key = TABLES[table].key
    v = results[table].valid
    assert v.filter(" OR ".join(f"`{k}` IS NULL" for k in key)).count() == 0
    assert v.groupBy(*key).count().filter("count > 1").count() == 0


@pytest.mark.parametrize("table", ORDER)
def test_valid_has_no_helper_columns(results, table):
    cols = results[table].valid.columns
    assert not [c for c in cols if c.startswith(("_raw_", "_dup_", "_fk_", "_failed"))]


def test_injected_errors_are_quarantined(results):
    def rules(t):
        return {r for row in results[t].quarantine.select(F.explode("_failed_rules")).collect()
                for r in row}

    expected = {"balance_not_negative", "start_before_maturity", "debt_group_in_range"}
    assert expected <= rules("loan_snapshots")
    assert {"customer_exists"} <= rules("core_accounts")
    assert {"amount_positive", "account_exists"} <= rules("core_transactions")
    assert "customer_id_not_null" in rules("core_customers")


def test_loans_valid_business_rules(results):
    v = results["loan_snapshots"].valid
    assert v.filter("ducuoi < 0 OR datestart > datemat OR nhom_no NOT BETWEEN 1 AND 5").count() == 0
    # debt_group tính lại từ DPD, is_npl = nhóm 3-5
    assert v.filter("is_npl <> (debt_group >= 3)").count() == 0
    assert v.filter("songay_nqh >= 90 AND debt_group < 3").count() == 0
    assert dict(v.dtypes)["as_of_date"] == "date"


def test_fail_rate_under_stop_threshold(results):
    for t in ORDER:
        s = dq_summary(results[t].checked, TABLES[t].rules, t)
        worst = s.filter("severity = 'FAIL'").agg(F.max("fail_pct")).first()[0] or 0
        assert worst < 5, f"{t}: {worst}%"


def test_bad_formats_become_null_not_errors(spark):
    df = spark.createDataFrame(
        [("1", "CN", "1990-01-31", AS_OF), ("2", "CN", "01/31/1990", AS_OF)],
        "customer_id string, cust_type string, dob string, _as_of_date string",
    ).withColumn("_as_of_date", F.to_date("_as_of_date"))
    for c in CUSTOMERS.cast:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    r = prepare_silver(df, CUSTOMERS)
    assert [x.customer_id for x in r.valid.collect()] == ["2"]
    q = r.quarantine.first()
    assert q.customer_id == "1" and q._failed_rules == ["dob_valid_format"]


def test_dup_key_count_ignores_null_keys(spark):
    df = spark.createDataFrame([("a",), ("a",), (None,), (None,), ("b",)], "k string")
    got = {(r.k, r._dup_key_count) for r in add_dup_key_count(df, ["k"]).collect()}
    assert got == {("a", 2), (None, 1), ("b", 1)}


def test_quarantine_records_shape(results):
    q = quarantine_records(results["loan_snapshots"].quarantine, "loans", "run-1")
    assert q.columns == ["run_id", "table_name", "failed_rules", "record_json", "_source_file",
                         "quarantined_at"]
    assert q.filter("record_json LIKE '%acctnbr%'").count() == q.count() > 0


def test_merge_condition():
    assert merge_condition(["as_of_date", "acctnbr"]) == \
        "t.`as_of_date` = s.`as_of_date` AND t.`acctnbr` = s.`acctnbr`"
