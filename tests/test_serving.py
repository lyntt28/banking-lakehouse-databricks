"""Publish nguyên tử theo batch: người đọc (gold_serving) chỉ thấy batch đã kích hoạt, kể cả khi
nguồn gửi file sửa cho ngày ĐÃ publish (2 batch cùng as_of_date)."""

import pytest
from pyspark.sql import functions as F

from common import serving as sv

D1, D2 = "2026-11-01", "2026-11-02"


def _activate(spark, day, batch, ts):
    (spark.createDataFrame([(day, batch, ts, "{}")],
                           "as_of_date string, run_id string, published_at string, checks string")
     .select(F.col("as_of_date").cast("date"), "run_id",
             F.col("published_at").cast("timestamp"), "checks")
     .write.format("delta").mode("append").saveAsTable("ops.published_batches"))


def _mart(spark, day, value):
    return spark.createDataFrame([(day, "101", value)],
                                 "as_of_date string, branchnbr string, ovd double") \
        .withColumn("as_of_date", F.col("as_of_date").cast("date"))


def _serving(spark, table):
    return {(str(r.as_of_date), r.ovd) for r in spark.table(f"gold_serving.{table}").collect()}


@pytest.fixture
def clean(spark):
    for s in ("gold", "gold_serving", "ops"):
        spark.sql(f"DROP SCHEMA IF EXISTS {s} CASCADE")
        spark.sql(f"CREATE SCHEMA {s}")
    yield
    for s in ("gold", "gold_serving", "ops"):
        spark.sql(f"DROP SCHEMA IF EXISTS {s} CASCADE")


def test_new_batch_invisible_until_activated_then_old_cleaned(spark, clean):
    t = "loan_mart_overdue_daily"
    sv.write_batch(_mart(spark, D1, 10.0), "", t, "b1")
    _activate(spark, D1, "b1", "2026-11-01 07:00:00")
    sv.create_serving_views(spark, "")
    assert _serving(spark, t) == {(D1, 10.0)}

    # file sửa cho D1 (đã publish): batch b2 ghi THÊM -> người đọc vẫn thấy b1
    sv.write_batch(_mart(spark, D1, 99.0), "", t, "b2")
    assert _serving(spark, t) == {(D1, 10.0)}
    # mart luỹ kế dựng từ trạng thái "sau kích hoạt" thấy đúng b2
    assert {r.ovd for r in sv.candidate(spark, "", t, D1, "b2").collect()} == {99.0}

    _activate(spark, D1, "b2", "2026-11-01 09:00:00")   # 1 commit -> chuyển hẳn sang b2
    assert _serving(spark, t) == {(D1, 99.0)}
    sv.cleanup_superseded(spark, "", t, D1, "b2")
    assert spark.table(f"gold.{t}").count() == 1
    assert _serving(spark, t) == {(D1, 99.0)}


def test_daily_keeps_other_days_and_failed_batch_stays_hidden(spark, clean):
    t = "loan_mart_overdue_daily"
    sv.write_batch(_mart(spark, D1, 1.0), "", t, "b1")
    _activate(spark, D1, "b1", "2026-11-01 07:00:00")
    sv.write_batch(_mart(spark, D2, 2.0), "", t, "b2")      # publish D2 lỗi, chưa kích hoạt
    sv.create_serving_views(spark, "")
    assert _serving(spark, t) == {(D1, 1.0)}
    cand = {(str(r.as_of_date), r.ovd) for r in sv.candidate(spark, "", t, D2, "b3").collect()}
    assert cand == {(D1, 1.0)}                               # b2 không lọt vào mart luỹ kế
    sv.write_batch(_mart(spark, D2, 3.0), "", t, "b3")
    _activate(spark, D2, "b3", "2026-11-02 07:00:00")
    sv.cleanup_superseded(spark, "", t, D2, "b3")             # dọn luôn rác của b2
    assert _serving(spark, t) == {(D1, 1.0), (D2, 3.0)}
    assert spark.table(f"gold.{t}").count() == 2


def test_snapshot_table_switches_whole_table(spark, clean):
    t = "dim_branch"
    df1 = spark.createDataFrame([("101", "A")], "branchnbr string, br_name string")
    df2 = spark.createDataFrame([("101", "A2"), ("102", "B")], "branchnbr string, br_name string")
    sv.write_batch(df1, "", t, "b1")
    _activate(spark, D1, "b1", "2026-11-01 07:00:00")
    sv.write_batch(df2, "", t, "b2")
    sv.create_serving_views(spark, "")
    assert spark.table(f"gold_serving.{t}").count() == 1
    assert "batch_id" not in spark.table(f"gold_serving.{t}").columns
    _activate(spark, D2, "b2", "2026-11-02 07:00:00")
    assert {r.br_name for r in spark.table(f"gold_serving.{t}").collect()} == {"A2", "B"}


def test_upgrade_adds_batch_id_to_existing_table(spark, clean):
    t = "loan_mart_overdue_daily"
    _mart(spark, D1, 5.0).write.format("delta").saveAsTable(f"gold.{t}")   # bảng cũ
    _activate(spark, D1, "run-old", "2026-11-01 07:00:00")
    sv.ensure_batch_column(spark, "", t)
    sv.create_serving_views(spark, "")
    assert _serving(spark, t) == {(D1, 5.0)}


def test_view_sql_kinds():
    assert "max_by(run_id, published_at)" in sv.serving_view_sql("c", "dep_mart_daily",
                                                                 ["as_of_date", "batch_id"])
    sql = sv.serving_view_sql("c", "core_fact_transaction", ["txn_id", "batch_id"])
    assert "IN (SELECT run_id FROM c.ops.published_batches)" in sql
    assert "`batch_id`" not in sql
    from pipelines.gold import PUBLISHED_TABLES
    assert set(PUBLISHED_TABLES) == set(sv.TABLE_KIND)
