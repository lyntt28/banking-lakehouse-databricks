"""Silver: test trên dữ liệu sinh bởi data_generator (đọc như Bronze: mọi cột string)."""


import pytest
from pyspark.sql import functions as F

from common.dq import dq_summary
from common.silver import (
    CUSTOMERS,
    ORDER,
    TABLES,
    add_dup_key_count,
    latest_delivery,
    merge_condition,
    prepare_silver,
    quarantine_records,
)

AS_OF = "2026-11-01"


@pytest.mark.parametrize("table", ORDER)
def test_row_counts_reconcile(bronze, results, table):
    r = results[table]
    assert bronze[table].count() == r.valid.count() + r.quarantine.count() + r.exact_dupes \
        + r.superseded


@pytest.mark.parametrize("table", ORDER)
def test_valid_keys_unique_and_not_null(results, table):
    key = TABLES[table].key
    v = results[table].valid
    if table == "card_overdue_source":   # grain chưa xác nhận duy nhất: dòng cùng khóa hợp lệ
        key = ["report_dt", "mng_br", "del_grp"]
        assert v.filter(" OR ".join(f"`{k}` IS NULL" for k in key)).count() == 0
        return
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


def test_correction_file_replaces_earlier_delivery(spark):
    """File gửi lại cùng ngày thay thế file trước: giá trị sửa được dùng, dòng bị rút khỏi file
    sửa biến mất (không phải cả 2 bản cùng khóa bị quarantine vì trùng khóa)."""
    rows = [("A1", "100", "loans.csv", "2026-11-03 06:00:00"),
            ("A2", "200", "loans.csv", "2026-11-03 06:00:00"),
            ("A1", "150", "loans_fix.csv", "2026-11-03 09:00:00")]
    df = (spark.createDataFrame(rows, "acct string, bal string, _source_file string, ts string")
          .withColumn("_load_ts", F.col("ts").cast("timestamp")).drop("ts")
          .withColumn("_as_of_date", F.lit("2026-11-02").cast("date")))
    kept, n = latest_delivery(df, "loan_snapshots")
    assert n == 2
    assert [(r.acct, r.bal) for r in kept.collect()] == [("A1", "150")]


def test_correction_replaces_only_its_file_group(spark):
    """Bảng nhiều file / ngày (9 nhóm KPI thẻ): file sửa nhóm 05 không xoá nhóm khác."""
    rows = [("x", "/v/as_of_date=2026-11-02/01_member_eop_20261102.txt", "2026-11-03 06:00:00"),
            ("y", "/v/as_of_date=2026-11-02/05_balance_eop_20261102.txt", "2026-11-03 06:00:00"),
            ("z", "/v/as_of_date=2026-11-02/05_balance_eop_20261102_v2.txt",
             "2026-11-03 09:00:00")]
    df = (spark.createDataFrame(rows, "v string, _source_file string, ts string")
          .withColumn("_load_ts", F.col("ts").cast("timestamp")).drop("ts"))
    kept, n = latest_delivery(df, "card_kpi_source")
    assert n == 1
    assert sorted(r.v for r in kept.collect()) == ["x", "z"]


def _ovd_bronze(spark, rows):
    cols = ["REPORT_DT", "PRDT_CODE", "PRODUCT_TYPE", "PRODUCT_GROUP", "PRODUCT_NAME", "MNG_BR",
            "SAL_BR", "DEL_GRP", "PORT_ACC", "OVD_ACC", "PORT_BAL", "OVD_BAL"]
    return spark.createDataFrame(rows, ", ".join(f"{c} string" for c in cols))


def test_overdue_same_key_rows_kept_not_quarantined(spark):
    """File mẫu gốc: nhiều dòng cùng khóa nghiệp vụ nhưng khác số liệu -> giữ cả (Gold SUM)."""
    base = ["20261101", "", "Credit Card", "GOLD", "VISA_GOLD", "101", "101", "11-90"]
    res = prepare_silver(_ovd_bronze(spark, [base + ["3", "1", "300", "100"],
                                             base + ["2", "1", "200", "50"]]),
                         TABLES["card_overdue_source"])
    assert res.valid.count() == 2 and res.quarantine.count() == 0
    assert TABLES["card_overdue_source"].write_mode == "replace_day"


def test_overdue_dq_rules(spark):
    ok = ["20261101", "", "Credit Card", "GOLD", "VISA_GOLD", "101", "101"]
    rows = [
        ok + ["11-90", "-1", "0", "300", "100"],        # tài khoản âm -> FAIL
        ok + ["11-90", "1", "0", "-5", "0"],            # dư nợ âm -> FAIL
        ok + ["11-90", "1", "0", "abc", "0"],           # số sai định dạng -> FAIL
        ["2026-13-01"] + ok[1:] + ["11-90", "1", "0", "1", "0"],   # ngày sai -> FAIL
        ok[:5] + ["", "101", "11-90", "1", "0", "1", "0"],          # thiếu CN quản lý -> FAIL
        ok + ["31-90", "1", "0", "1", "0"],             # nhóm nợ lạ -> WARN (giữ)
        ok + ["1-10", "1", "2", "100", "200"],          # quá hạn > danh mục -> WARN (giữ)
    ]
    res = prepare_silver(_ovd_bronze(spark, rows), TABLES["card_overdue_source"])
    assert res.quarantine.count() == 5 and res.valid.count() == 2
    rules = {r for x in res.quarantine.select("_failed_rules").collect()
             for r in x._failed_rules}
    assert {"ovd_acc_not_negative", "ovd_balance_not_negative", "ovd_balance_numeric",
            "ovd_report_dt_valid", "ovd_key_not_null"} <= rules
