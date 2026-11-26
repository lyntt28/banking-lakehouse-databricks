"""Kiểm chứng ghi Delta THẬT (local Delta Lake): MERGE, SCD2, ghi đè 1 ngày.

Mục tiêu chính: chạy lại cùng dữ liệu KHÔNG làm thay đổi số dòng (idempotent).
"""

from datetime import date

import pytest
from pyspark.sql import functions as F

from common.delta_io import merge_upsert, overwrite_date, scd2_upsert

TRACKED = ["province", "segment"]


@pytest.fixture()
def tbl(spark, request):
    name = f"default.t_{request.node.name}"
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    yield name
    spark.sql(f"DROP TABLE IF EXISTS {name}")


def _cust(spark, rows):
    return spark.createDataFrame(rows, "customer_id string, province string, segment string")


def test_merge_upsert_is_idempotent(spark, tbl):
    df = spark.createDataFrame([("A1", 100.0), ("A2", 200.0)], "account_id string, balance double")
    merge_upsert(df, tbl, ["account_id"])          # lần đầu: tạo bảng
    merge_upsert(df, tbl, ["account_id"])          # chạy lại: không nhân đôi
    assert spark.table(tbl).count() == 2

    upd = spark.createDataFrame([("A2", 250.0), ("A3", 5.0)], "account_id string, balance double")
    merge_upsert(upd, tbl, ["account_id"])
    got = {r.account_id: r.balance for r in spark.table(tbl).collect()}
    assert got == {"A1": 100.0, "A2": 250.0, "A3": 5.0}


def test_merge_insert_only_keeps_first_version(spark, tbl):
    df = spark.createDataFrame([("T1", 10.0)], "txn_id string, amount double")
    merge_upsert(df, tbl, ["txn_id"], insert_only=True)
    merge_upsert(spark.createDataFrame([("T1", 99.0)], "txn_id string, amount double"),
                 tbl, ["txn_id"], insert_only=True)
    assert [r.amount for r in spark.table(tbl).collect()] == [10.0]


def test_scd2_history_and_rerun(spark, tbl):
    d1 = _cust(spark, [("C1", "HA NOI", "MASS"), ("C2", "DA NANG", "MASS")])
    scd2_upsert(d1, tbl, "customer_id", TRACKED, "2026-11-01")
    scd2_upsert(d1, tbl, "customer_id", TRACKED, "2026-11-01")     # chạy lại ngày 1
    assert spark.table(tbl).count() == 2

    d2 = _cust(spark, [("C1", "HO CHI MINH", "MASS"),               # đổi tỉnh
                       ("C2", "DA NANG", "MASS"),                   # không đổi
                       ("C3", "CAN THO", "AFFLUENT")])              # KH mới
    scd2_upsert(d2, tbl, "customer_id", TRACKED, "2026-11-02")
    scd2_upsert(d2, tbl, "customer_id", TRACKED, "2026-11-02")     # chạy lại ngày 2
    t = spark.table(tbl)
    assert t.count() == 4                                           # C1 x2, C2, C3
    assert t.filter("is_current").count() == 3

    c1 = {r.province: r for r in t.filter("customer_id = 'C1'").collect()}
    assert c1["HA NOI"].is_current is False
    assert c1["HA NOI"].valid_to == date(2026, 11, 1)               # đóng hôm trước
    assert c1["HO CHI MINH"].is_current is True
    assert c1["HO CHI MINH"].valid_from == date(2026, 11, 2)
    assert c1["HO CHI MINH"].valid_to is None
    # không có khoảng hở / chồng lấn: mỗi KH đúng 1 bản hiện hành
    per = t.filter("is_current").groupBy("customer_id").count()
    assert per.filter(F.col("count") != 1).count() == 0


def test_scd2_rejects_out_of_order_date(spark, tbl):
    scd2_upsert(_cust(spark, [("C1", "HA NOI", "MASS")]), tbl, "customer_id", TRACKED,
                "2026-11-02")
    with pytest.raises(ValueError, match="thứ tự ngày"):
        scd2_upsert(_cust(spark, [("C1", "HUE", "MASS")]), tbl, "customer_id", TRACKED,
                    "2026-11-01")


def test_scd2_same_day_change_keeps_valid_range(spark, tbl):
    scd2_upsert(_cust(spark, [("C1", "HA NOI", "MASS")]), tbl, "customer_id", TRACKED,
                "2026-11-01")
    scd2_upsert(_cust(spark, [("C1", "HUE", "MASS")]), tbl, "customer_id", TRACKED,
                "2026-11-01")                                       # file sửa lại cùng ngày
    bad = spark.table(tbl).filter("valid_to IS NOT NULL AND valid_to < valid_from")
    assert bad.count() == 0


def test_overwrite_date_rejects_bad_date(spark, tbl):
    df = spark.createDataFrame([(date(2026, 11, 1), 1.0)], "as_of_date date, v double")
    with pytest.raises(ValueError):
        overwrite_date(df, tbl, "as_of_date", "2026-11-01' OR 1=1 --")


def test_overwrite_date_replaces_only_that_day(spark, tbl):
    schema = "as_of_date date, branchnbr string, balance double"
    base = spark.createDataFrame([(date(2026, 11, 1), "1", 10.0), (date(2026, 11, 2), "1", 20.0)],
                                 schema)
    overwrite_date(base, tbl, "as_of_date", "2026-11-01")           # tạo bảng
    new = spark.createDataFrame([(date(2026, 11, 2), "1", 25.0), (date(2026, 11, 2), "2", 5.0)],
                                schema)
    overwrite_date(new, tbl, "as_of_date", "2026-11-02")
    overwrite_date(new, tbl, "as_of_date", "2026-11-02")           # chạy lại
    rows = {(str(r.as_of_date), r.branchnbr): r.balance for r in spark.table(tbl).collect()}
    assert rows == {("2026-11-01", "1"): 10.0, ("2026-11-02", "1"): 25.0,
                    ("2026-11-02", "2"): 5.0}


def test_scd2_same_day_corrections_never_overlap(spark, tbl):
    """Đổi 2 lần trong cùng ngày: vẫn 1 phiên bản / ngày, không chồng lấn, sk duy nhất."""
    from pyspark.sql import functions as F

    scd2_upsert(_cust(spark, [("C1", "HA NOI", "MASS")]), tbl, "customer_id", TRACKED,
                "2026-11-01")
    scd2_upsert(_cust(spark, [("C1", "HUE", "MASS")]), tbl, "customer_id", TRACKED,
                "2026-11-02")
    scd2_upsert(_cust(spark, [("C1", "DA NANG", "MASS")]), tbl, "customer_id", TRACKED,
                "2026-11-02")                                       # sửa lại cùng ngày
    scd2_upsert(_cust(spark, [("C1", "DA NANG", "MASS")]), tbl, "customer_id", TRACKED,
                "2026-11-02")                                       # chạy lại: không đổi
    rows = spark.table(tbl).orderBy("valid_from").collect()
    assert [(str(r.valid_from), r.valid_to and str(r.valid_to), r.province, r.is_current)
            for r in rows] == [("2026-11-01", "2026-11-01", "HA NOI", False),
                               ("2026-11-02", None, "DA NANG", True)]
    # as-of join ngày 2026-11-02 chỉ khớp đúng 1 phiên bản
    hit = spark.table(tbl).filter(
        "valid_from <= DATE'2026-11-02' AND (valid_to IS NULL OR valid_to >= DATE'2026-11-02')")
    assert hit.count() == 1
    sk = spark.table(tbl).select(F.xxhash64("customer_id", "valid_from").alias("sk"))
    assert sk.distinct().count() == sk.count()


def test_scd2_change_on_first_day_is_corrected_in_place(spark, tbl):
    scd2_upsert(_cust(spark, [("C1", "HA NOI", "MASS")]), tbl, "customer_id", TRACKED,
                "2026-11-01")
    scd2_upsert(_cust(spark, [("C1", "HUE", "MASS")]), tbl, "customer_id", TRACKED,
                "2026-11-01")
    rows = spark.table(tbl).collect()
    assert len(rows) == 1 and rows[0].province == "HUE" and rows[0].is_current
