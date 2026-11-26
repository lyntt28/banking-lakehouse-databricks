"""End-to-end Bronze -> Silver -> Gold trên Spark + Delta LOCAL, cùng code chạy trên Databricks
(pipelines.silver.run / pipelines.gold.run, catalog rỗng -> bảng `schema.table`).

Kịch bản sự cố (chạy tuần tự, mỗi test dựa trên trạng thái test trước):
    1. thiếu file ngày 2           -> ngày 1 SUCCESS, ngày 2 FAILED, watermark đứng ở ngày 1
    2. file về trễ                 -> chạy lại tự xử lý ngày 2; Gold publish đủ 2 ngày
    3. chạy lại nhiều lần          -> checksum nội dung Silver/Gold không đổi, không trùng khóa,
                                      SCD2 nhất quán
    3b. lỗi giữa chừng (bảng thứ N) -> Silver không bị ghi dở; sửa xong chạy lại hội tụ
    4. file trùng (gửi lại)        -> không nhân đôi dữ liệu, manifest báo MULTIPLE_FILES
    5. đối soát lệch               -> GateFailed, Gold đang phục vụ KHÔNG đổi, không publish
    6. backfill ngày cũ hơn SCD2   -> bị từ chối, dữ liệu hiện tại không đổi
"""

from datetime import date

import pytest

pytestmark = pytest.mark.integration

D1, D2 = "2026-11-01", "2026-11-02"
SCHEMAS = ["bronze", "silver", "silver_stage", "gold", "gold_stage", "ops"]


def _drop_all(spark):
    for s in SCHEMAS:
        spark.sql(f"DROP SCHEMA IF EXISTS {s} CASCADE")


@pytest.fixture(scope="module")
def env(spark, tmp_path_factory):
    from pyspark.sql import functions as F

    from common.bronze import SOURCES, add_bronze_metadata
    from data_generator.generate import Sizes, generate
    from tests.conftest import SMALL_SIZES

    _drop_all(spark)
    out = tmp_path_factory.mktemp("landing_e2e")
    generate(out, date(2026, 11, 1), 2, seed=11, sizes=Sizes(**SMALL_SIZES))
    spark.sql("CREATE SCHEMA bronze")
    frames = {}
    for s in SOURCES:
        path = str(out / s.table)
        df = (spark.read.option("primitivesAsString", "true").json(path) if s.fmt == "json"
              else spark.read.options(**s.options).csv(path))
        frames[s.table] = add_bronze_metadata(df, F.input_file_name())
    # Giả lập: file transactions ngày 2 CHƯA về
    for t, df in frames.items():
        first = df.filter(F.col("_as_of_date") != F.lit(D2).cast("date")) \
            if t == "transactions" else df
        first.write.format("delta").saveAsTable(f"bronze.{t}")
    yield {"frames": frames}
    _drop_all(spark)


def _runs(spark, layer):
    return [(str(r.as_of_date), r.status) for r in spark.sql(
        f"SELECT as_of_date, status FROM ops.pipeline_runs WHERE layer = '{layer}' "
        "ORDER BY started_at, status").collect()]


def _checksum(spark, table):
    """(số dòng, tổng hash nội dung từng dòng) - không phụ thuộc thứ tự, đổi 1 giá trị là lệch."""
    from pyspark.sql import functions as F

    df = spark.table(table)
    # bỏ cột lineage kỹ thuật (thời điểm nạp, tên file); so nội dung nghiệp vụ
    cols = sorted(c for c in df.columns if c not in ("_load_ts", "_source_file", "_rescued_data"))
    r = df.select(F.count("*").alias("n"),
                  F.sum(F.xxhash64(*cols).cast("decimal(38,0)")).alias("h")).first()
    return (r.n, r.h)


def _gold_fingerprint(spark):
    from pipelines.gold import PUBLISHED_TABLES

    return {t: _checksum(spark, f"gold.{t}") for t in PUBLISHED_TABLES}


def _silver_fingerprint(spark):
    from common.silver import ORDER

    return {t: _checksum(spark, f"silver.{t}") for t in ORDER}


def _assert_integrity(spark):
    """Khóa nghiệp vụ không trùng + SCD2 nhất quán (sau mọi lần chạy / chạy lại)."""
    from common.gold import GOLD_KEYS
    from common.silver import ORDER, TABLES

    for t, key in GOLD_KEYS.items():
        k = ", ".join(key)
        dup = spark.sql(f"SELECT {k} FROM gold.{t} GROUP BY {k} HAVING count(*) > 1").count()
        assert dup == 0, f"gold.{t} trùng khóa {key}"
    for t in ORDER:
        spec = TABLES[t]
        if spec.write_mode == "scd2":
            continue
        k = ", ".join(spec.key)
        dup = spark.sql(f"SELECT {k} FROM silver.{t} GROUP BY {k} HAVING count(*) > 1").count()
        assert dup == 0, f"silver.{t} trùng khóa {spec.key}"
    # SCD2: đúng 1 bản hiện hành / KH, khoảng hiệu lực không chồng lấn, valid_to >= valid_from
    assert spark.sql("SELECT customer_id FROM silver.core_customers GROUP BY customer_id "
                     "HAVING sum(CAST(is_current AS INT)) <> 1").count() == 0
    assert spark.sql("SELECT 1 FROM silver.core_customers "
                     "WHERE valid_to IS NOT NULL AND valid_to < valid_from").count() == 0
    assert spark.sql("""
        SELECT 1 FROM silver.core_customers a JOIN silver.core_customers b
          ON a.customer_id = b.customer_id AND a.valid_from < b.valid_from
         AND (a.valid_to IS NULL OR a.valid_to >= b.valid_from)""").count() == 0
    assert spark.sql("SELECT customer_sk FROM gold.dim_customer GROUP BY customer_sk "
                     "HAVING count(*) > 1").count() == 0


def test_1_missing_file_stops_at_that_day(spark, env):
    from pipelines import daily
    from pipelines.silver import SilverConfig, SourceFilesMissing

    with pytest.raises(SourceFilesMissing):
        daily.run(spark, SilverConfig(catalog=""))
    # ngày 1 đã đủ Silver + Gold trước khi ngày 2 dừng
    assert [str(r[0]) for r in spark.sql("SELECT as_of_date FROM ops.published_batches")
            .collect()] == [D1]
    assert ("2026-11-01", "SUCCESS") in _runs(spark, "silver")
    assert _runs(spark, "silver")[-1] == (D2, "FAILED")
    m = {r.table_name: r.status for r in spark.sql(
        f"SELECT table_name, status FROM ops.file_manifest WHERE as_of_date = '{D2}'").collect()}
    assert m["core_transactions"] == "MISSING"
    assert m["ref_branches"] == "NOT_SENT"
    # có ghi từng bước vào task_runs, kể cả bước lỗi
    steps = spark.sql("SELECT step, status FROM ops.task_runs "
                      f"WHERE as_of_date = '{D2}'").collect()
    assert ("file_manifest", "FAILED") in [(r.step, r.status) for r in steps]


def test_2_late_file_is_picked_up_and_gold_published(spark, env):
    from pyspark.sql import functions as F

    from pipelines import daily
    from pipelines.silver import SilverConfig

    env["frames"]["transactions"].filter(F.col("_as_of_date") == F.lit(D2).cast("date")) \
        .write.format("delta").mode("append").saveAsTable("bronze.transactions")
    assert daily.run(spark, SilverConfig(catalog="")) == [D2]     # chỉ ngày còn thiếu
    pub = [str(r[0]) for r in spark.sql(
        "SELECT as_of_date FROM ops.published_batches ORDER BY 1").collect()]
    assert pub == [D1, D2]
    _assert_integrity(spark)
    env["gold_ok"] = _gold_fingerprint(spark)
    env["silver_ok"] = _silver_fingerprint(spark)


def test_3_rerun_is_idempotent(spark, env):
    from pipelines.gold import GoldConfig
    from pipelines.gold import run as gold_run
    from pipelines.silver import SilverConfig, run

    assert run(spark, SilverConfig(catalog="")) == []             # không có ngày mới
    run(spark, SilverConfig(catalog="", as_of_date=D2))
    run(spark, SilverConfig(catalog="", as_of_date=D2))
    gold_run(spark, GoldConfig(catalog="", as_of_date=D2))
    # nội dung (checksum từng dòng), không chỉ số dòng
    assert _silver_fingerprint(spark) == env["silver_ok"]
    assert _gold_fingerprint(spark) == env["gold_ok"]
    _assert_integrity(spark)


def test_3b_failure_midway_leaves_silver_untouched_then_rerun_recovers(spark, env,
                                                                       monkeypatch):
    """Bảng thứ N lỗi DQ: các bảng trước KHÔNG được ghi (stage rồi mới publish)."""
    import pipelines.silver as ps
    from pipelines.silver import DQGateFailed, SilverConfig, run

    real = ps.should_stop
    monkeypatch.setattr(ps, "should_stop", lambda summary, pct: (
        summary.first()["table_name"] == "card_transactions_hist" or real(summary, pct)))
    with pytest.raises(DQGateFailed, match="card_transactions_hist"):
        run(spark, SilverConfig(catalog="", as_of_date=D2))
    assert _silver_fingerprint(spark) == env["silver_ok"]
    assert _runs(spark, "silver")[-1] == (D2, "FAILED")

    monkeypatch.setattr(ps, "should_stop", real)
    run(spark, SilverConfig(catalog="", as_of_date=D2))          # sửa xong -> chạy lại
    assert _silver_fingerprint(spark) == env["silver_ok"]
    assert _runs(spark, "silver")[-1] == (D2, "SUCCESS")


def test_4_duplicate_file_does_not_double_count(spark, env):
    from pyspark.sql import functions as F

    from pipelines.gold import GoldConfig
    from pipelines.gold import run as gold_run
    from pipelines.silver import SilverConfig, run

    dup = spark.table("bronze.loans").filter(F.col("_as_of_date") == F.lit(D2).cast("date")) \
        .withColumn("_source_file", F.concat(F.col("_source_file"), F.lit(".resent")))
    dup.write.format("delta").mode("append").saveAsTable("bronze.loans")
    run(spark, SilverConfig(catalog="", as_of_date=D2))
    gold_run(spark, GoldConfig(catalog="", as_of_date=D2))
    assert _silver_fingerprint(spark) == env["silver_ok"]
    assert _gold_fingerprint(spark) == env["gold_ok"]
    _assert_integrity(spark)
    st = spark.sql(f"SELECT status FROM ops.file_manifest WHERE as_of_date = '{D2}' "
                   "AND table_name = 'loan_snapshots'").first()[0]
    assert st == "MULTIPLE_FILES"


def test_5_reconciliation_failure_keeps_serving_gold(spark, env):
    from pyspark.sql import functions as F

    from pipelines.gold import PUBLISHED_TABLES, GateFailed, GoldConfig
    from pipelines.gold import run as gold_run

    n_pub = spark.table("ops.published_batches").count()
    # nguồn gửi file tổng hợp tiền gửi lệch -> đối soát phải chặn publish
    src = spark.table("silver.dep_summary_source")
    num = "balance_mil_vnd"
    tampered = src.withColumn(num, F.when(F.col("stnd_dt") == F.lit(D2).cast("date"),
                                          F.col(num) * 2 + 1).otherwise(F.col(num))
                              .cast(src.schema[num].dataType))
    tampered.write.format("delta").mode("overwrite").saveAsTable("silver.dep_summary_source_t")
    spark.sql("ALTER TABLE silver.dep_summary_source RENAME TO silver.dep_summary_source_ok")
    spark.sql("ALTER TABLE silver.dep_summary_source_t RENAME TO silver.dep_summary_source")
    try:
        with pytest.raises(GateFailed):
            gold_run(spark, GoldConfig(catalog="", as_of_date=D2))
    finally:
        spark.sql("DROP TABLE silver.dep_summary_source")
        spark.sql("ALTER TABLE silver.dep_summary_source_ok RENAME TO silver.dep_summary_source")
    assert _gold_fingerprint(spark) == env["gold_ok"]
    assert spark.table("ops.published_batches").count() == n_pub
    # có version trước publish của mọi bảng Gold (để RESTORE)
    assert spark.sql("SELECT count(DISTINCT table_name) FROM ops.publish_versions "
                     "WHERE layer = 'gold'").first()[0] == len(PUBLISHED_TABLES)
    assert _runs(spark, "gold")[-1] == (D2, "FAILED")
    gate = spark.sql("SELECT status, error FROM ops.task_runs WHERE layer = 'gold' "
                     "AND step = 'gold:gate' ORDER BY logged_at DESC LIMIT 1").first()
    assert gate.status == "FAILED" and "Gate" in gate.error


def test_6_out_of_order_rerun_rejected_without_damage(spark, env):
    from pipelines.gold import GoldConfig, NotReproducible
    from pipelines.gold import run as gold_run
    from pipelines.silver import OutOfOrder, SilverConfig, run

    before = _gold_fingerprint(spark)
    with pytest.raises(OutOfOrder):
        run(spark, SilverConfig(catalog="", as_of_date=D1))
    with pytest.raises(NotReproducible):
        gold_run(spark, GoldConfig(catalog="", as_of_date=D1))
    assert _gold_fingerprint(spark) == before


def test_7_daily_runner_catches_up_silver_and_gold_per_day(spark, env):
    """Gián đoạn nhiều ngày: Silver + Gold phải chạy XEN KẼ từng ngày (snapshot MERGE)."""
    from pipelines import daily
    from pipelines.silver import SilverConfig

    for s in ["silver", "silver_stage", "gold", "gold_stage", "ops"]:
        spark.sql(f"DROP SCHEMA IF EXISTS {s} CASCADE")
    assert daily.run(spark, SilverConfig(catalog="")) == [D1, D2]
    assert _runs(spark, "gold") == [(D1, "RUNNING"), (D1, "SUCCESS"),
                                    (D2, "RUNNING"), (D2, "SUCCESS")]
    # chạy bù nhiều ngày một lần = chạy từng ngày (cùng nội dung)
    assert _silver_fingerprint(spark) == env["silver_ok"]
    assert _gold_fingerprint(spark) == env["gold_ok"]
    _assert_integrity(spark)
