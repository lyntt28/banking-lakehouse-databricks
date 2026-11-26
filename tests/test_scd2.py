from common.scd2 import add_row_hash, build_scd2_staging

TRACKED = ["full_name", "address"]


def _current(spark):
    df = spark.createDataFrame(
        [("C1", "Nguyen A", "Q1"), ("C2", "Tran B", "Q3")],
        "customer_id string, full_name string, address string",
    )
    return add_row_hash(df, TRACKED)


def test_unchanged_rows_produce_nothing(spark):
    incoming = spark.createDataFrame(
        [("C1", "Nguyen A", "Q1")], "customer_id string, full_name string, address string"
    )
    staging = build_scd2_staging(incoming, _current(spark), "customer_id", TRACKED)
    assert staging.count() == 0


def test_changed_row_is_closed_and_reinserted(spark):
    incoming = spark.createDataFrame(
        [("C2", "Tran B", "Thu Duc")], "customer_id string, full_name string, address string"
    )
    staging = build_scd2_staging(incoming, _current(spark), "customer_id", TRACKED).collect()
    assert len(staging) == 2
    keys = sorted((r.merge_key or "<insert>") for r in staging)
    assert keys == ["<insert>", "C2"]
    assert all(r.address == "Thu Duc" for r in staging)


def test_new_customer_only_inserted(spark):
    incoming = spark.createDataFrame(
        [("C9", "Le C", "Q7")], "customer_id string, full_name string, address string"
    )
    staging = build_scd2_staging(incoming, _current(spark), "customer_id", TRACKED).collect()
    assert len(staging) == 1
    assert staging[0].merge_key is None


def test_same_day_change_is_correction_not_new_version(spark):
    from datetime import date

    from pyspark.sql import functions as F

    cur = _current(spark).withColumn("valid_from", F.lit(date(2026, 11, 2)))
    incoming = spark.createDataFrame(
        [("C2", "Tran B", "Thu Duc")], "customer_id string, full_name string, address string"
    )
    rows = build_scd2_staging(incoming, cur, "customer_id", TRACKED, "2026-11-02").collect()
    assert [(r.merge_key, r._action) for r in rows] == [("C2", "correct")]
    rows = build_scd2_staging(incoming, cur, "customer_id", TRACKED, "2026-11-03").collect()
    assert sorted(r._action for r in rows) == ["close", "insert"]
