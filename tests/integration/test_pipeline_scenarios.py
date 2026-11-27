"""End-to-end Bronze -> Silver -> Gold trên Spark + Delta LOCAL, cùng code chạy trên Databricks
(pipelines.silver.run / pipelines.gold.run, catalog rỗng -> bảng `schema.table`).

Mỗi kịch bản ĐỘC LẬP: tự dựng trạng thái nó cần, không dựa vào test chạy trước -> chạy riêng
1 test (`pytest -k test_5b`), đổi thứ tự, hay debug đều cho cùng kết quả.
    - fixture `baseline` (1 lần / module): Bronze 2 ngày, chạy Silver + Gold TỪNG ngày, lưu
      checksum "đúng" và chụp mọi bảng sang schema snap_* .
    - fixture `published`: khôi phục trạng thái đã publish D1, D2 (mỗi test). Bảng còn tồn tại:
      `RESTORE TABLE ... TO VERSION AS OF` (chỉ đổi metadata, gần như tức thì); bảng đã bị xóa
      (sau kịch bản dựng lại từ Bronze): copy lại từ snap_*.
    - fixture `fresh_bronze`: chỉ có Bronze, chưa có Silver / Gold / ops (mỗi test).

Kịch bản sự cố:
    1. thiếu file ngày 2, rồi file về trễ -> ngày 1 SUCCESS, ngày 2 FAILED, watermark đứng ở
                                      ngày 1; chạy lại tự xử lý ngày 2, Gold publish đủ 2 ngày
    3. chạy lại nhiều lần          -> checksum nội dung Silver/Gold không đổi, không trùng khóa
    3b. lỗi giữa chừng (bảng thứ N) -> Silver không bị ghi dở; sửa xong chạy lại hội tụ
    3c. lỗi GIỮA bước publish Silver -> tự rollback mọi bảng Silver đã ghi
    4. file trùng (gửi lại)        -> không nhân đôi dữ liệu, manifest báo MULTIPLE_FILES
    5. đối soát lệch               -> GateFailed, Gold đang phục vụ KHÔNG đổi, không publish
    5b. lỗi GIỮA bước publish Gold -> tự rollback mọi bảng đã ghi, không publish batch
    6. backfill ngày cũ hơn SCD2   -> bị từ chối, dữ liệu hiện tại không đổi
    6b. file sửa (correction)      -> ngày mới nhất: chạy lại Silver rồi Gold; ngày cũ: dừng
    6d. file sửa có giá trị KHÁC    -> giá trị sửa vào Silver + Gold, dòng bị rút khỏi file mất
    7. gián đoạn nhiều ngày        -> daily chạy bù xen kẽ Silver/Gold = chạy từng ngày
"""

from datetime import date

import pytest

pytestmark = pytest.mark.integration

D1, D2 = "2026-11-01", "2026-11-02"
SCHEMAS = ["bronze", "silver", "silver_stage", "gold", "gold_stage", "ops"]


def _drop_all(spark, schemas=SCHEMAS):
    for s in schemas:
        spark.sql(f"DROP SCHEMA IF EXISTS {s} CASCADE")


def _write_bronze(spark, frames, skip_d2_transactions=False):
    from pyspark.sql import functions as F

    _drop_all(spark)
    spark.sql("CREATE SCHEMA bronze")
    for t, df in frames.items():
        if skip_d2_transactions and t == "transactions":   # file transactions ngày 2 CHƯA về
            df = df.filter(F.col("_as_of_date") != F.lit(D2).cast("date"))
        df.write.format("delta").saveAsTable(f"bronze.{t}")


def table_version(spark, name):
    from common.batch import table_version as v

    return v(spark, name)


def _tables(spark, schema):
    return [r.tableName for r in spark.sql(f"SHOW TABLES IN {schema}").collect()
            if not r.isTemporary]


@pytest.fixture(scope="module")
def baseline(spark, tmp_path_factory):
    from pyspark.sql import functions as F

    from common.bronze import SOURCES, add_bronze_metadata
    from data_generator.generate import Sizes, generate
    from pipelines.gold import GoldConfig
    from pipelines.gold import run as gold_run
    from pipelines.silver import SilverConfig
    from pipelines.silver import run as silver_run
    from tests.conftest import SMALL_SIZES

    out = tmp_path_factory.mktemp("landing_e2e")
    generate(out, date(2026, 11, 1), 2, seed=11, sizes=Sizes(**SMALL_SIZES))
    frames = {}
    for s in SOURCES:
        path = str(out / s.table)
        df = (spark.read.option("primitivesAsString", "true").json(path) if s.fmt == "json"
              else spark.read.options(**s.options).csv(path))
        # đóng băng (_load_ts, _source_file) 1 lần: mọi kịch bản dùng cùng Bronze
        frames[s.table] = add_bronze_metadata(df, F.input_file_name()) \
            .withColumn("_load_ts", F.lit("2026-11-03 00:00:00").cast("timestamp"))
    _write_bronze(spark, frames)
    for d in (D1, D2):                       # trạng thái "đúng": từng ngày Silver rồi Gold
        silver_run(spark, SilverConfig(catalog="", as_of_date=d))
        gold_run(spark, GoldConfig(catalog="", as_of_date=d))
    _assert_integrity(spark)
    base = {"frames": frames, "gold_ok": _gold_fingerprint(spark),
            "silver_ok": _silver_fingerprint(spark), "tables": {}, "versions": {}}
    for schema in SCHEMAS:                   # chụp trạng thái đã publish
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS snap_{schema}")
        base["tables"][schema] = _tables(spark, schema)
        for t in base["tables"][schema]:
            spark.sql(f"CREATE TABLE snap_{schema}.{t} USING DELTA AS SELECT * FROM {schema}.{t}")
            v = table_version(spark, f"{schema}.{t}")
            # (version có nội dung baseline, version hiện tại được biết là "sạch")
            base["versions"][f"{schema}.{t}"] = [v, v]
    yield base
    _drop_all(spark, SCHEMAS + [f"snap_{s}" for s in SCHEMAS])


@pytest.fixture
def published(spark, baseline):
    """Trạng thái sau khi D1, D2 đã Silver + Gold SUCCESS."""
    versions = baseline["versions"]
    for schema, tables in baseline["tables"].items():
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        existing = set(_tables(spark, schema))
        for t in existing - set(tables):                 # bảng do test trước tạo thêm
            spark.sql(f"DROP TABLE {schema}.{t}")
        for t in tables:
            name = f"{schema}.{t}"
            if t not in existing:
                spark.sql(f"CREATE TABLE {name} USING DELTA AS SELECT * FROM snap_{name}")
                v = table_version(spark, name)
                versions[name] = [v, v]
            elif table_version(spark, name) != versions[name][1]:   # test trước đã ghi
                spark.sql(f"RESTORE TABLE {name} TO VERSION AS OF {versions[name][0]}")
                versions[name][1] = table_version(spark, name)
    return baseline


@pytest.fixture
def fresh_bronze(spark, baseline):
    """Chỉ có Bronze đủ 2 ngày; chưa chạy Silver / Gold (chưa có ops.*)."""
    _write_bronze(spark, baseline["frames"])
    return baseline


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


def test_1_missing_file_stops_at_that_day_then_late_file_is_picked_up(spark, baseline):
    from pyspark.sql import functions as F

    from pipelines import daily
    from pipelines.silver import SilverConfig, SourceFilesMissing

    _write_bronze(spark, baseline["frames"], skip_d2_transactions=True)

    # (1) thiếu file transactions ngày 2
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

    # (2) file về trễ -> chạy lại job: chỉ xử lý ngày còn thiếu, Gold publish đủ 2 ngày
    baseline["frames"]["transactions"].filter(F.col("_as_of_date") == F.lit(D2).cast("date")) \
        .write.format("delta").mode("append").saveAsTable("bronze.transactions")
    assert daily.run(spark, SilverConfig(catalog="")) == [D2]
    pub = [str(r[0]) for r in spark.sql(
        "SELECT as_of_date FROM ops.published_batches ORDER BY 1").collect()]
    assert pub == [D1, D2]
    _assert_integrity(spark)
    assert _gold_fingerprint(spark) == baseline["gold_ok"]
    assert _silver_fingerprint(spark) == baseline["silver_ok"]


def test_3_rerun_is_idempotent(spark, published):
    from pipelines.gold import GoldConfig
    from pipelines.gold import run as gold_run
    from pipelines.silver import SilverConfig, run

    assert run(spark, SilverConfig(catalog="")) == []             # không có ngày mới
    run(spark, SilverConfig(catalog="", as_of_date=D2))
    run(spark, SilverConfig(catalog="", as_of_date=D2))
    gold_run(spark, GoldConfig(catalog="", as_of_date=D2))
    # nội dung (checksum từng dòng), không chỉ số dòng
    assert _silver_fingerprint(spark) == published["silver_ok"]
    assert _gold_fingerprint(spark) == published["gold_ok"]
    _assert_integrity(spark)


def test_3b_failure_midway_leaves_silver_untouched_then_rerun_recovers(spark, published,
                                                                       monkeypatch):
    """Bảng thứ N lỗi DQ: các bảng trước KHÔNG được ghi (stage rồi mới publish)."""
    import pipelines.silver as ps
    from pipelines.silver import DQGateFailed, SilverConfig, run

    real = ps.should_stop
    monkeypatch.setattr(ps, "should_stop", lambda summary, pct: (
        summary.first()["table_name"] == "card_transactions_hist" or real(summary, pct)))
    with pytest.raises(DQGateFailed, match="card_transactions_hist"):
        run(spark, SilverConfig(catalog="", as_of_date=D2))
    assert _silver_fingerprint(spark) == published["silver_ok"]
    assert _runs(spark, "silver")[-1] == (D2, "FAILED")

    monkeypatch.setattr(ps, "should_stop", real)
    run(spark, SilverConfig(catalog="", as_of_date=D2))          # sửa xong -> chạy lại
    assert _silver_fingerprint(spark) == published["silver_ok"]
    assert _runs(spark, "silver")[-1] == (D2, "SUCCESS")


def test_3c_failure_midway_through_silver_publish_is_rolled_back(spark, published,
                                                                 monkeypatch):
    """Lỗi hạ tầng khi MERGE bảng Silver thứ N: bảng 1..N-1 đã ghi phải được RESTORE."""
    import pipelines.silver as ps
    from pipelines.silver import SilverConfig, run

    real = ps.merge_upsert

    def boom(df, target, *a, **k):
        if target.endswith("card_master"):           # các bảng trước đã MERGE xong
            raise OSError("storage timeout (giả lập)")
        return real(df, target, *a, **k)

    monkeypatch.setattr(ps, "merge_upsert", boom)
    with pytest.raises(OSError):
        run(spark, SilverConfig(catalog="", as_of_date=D2))
    assert _silver_fingerprint(spark) == published["silver_ok"]
    last = spark.sql("SELECT status, message FROM ops.pipeline_runs WHERE layer = 'silver' "
                     "ORDER BY started_at DESC, status LIMIT 1").first()
    assert last.status == "FAILED" and "rollback" in last.message

    monkeypatch.setattr(ps, "merge_upsert", real)
    run(spark, SilverConfig(catalog="", as_of_date=D2))          # hạ tầng ổn -> chạy lại
    assert _silver_fingerprint(spark) == published["silver_ok"]
    _assert_integrity(spark)


def test_4_duplicate_file_does_not_double_count(spark, published):
    from pyspark.sql import functions as F

    from pipelines.gold import GoldConfig
    from pipelines.gold import run as gold_run
    from pipelines.silver import SilverConfig, run

    dup = spark.table("bronze.loans").filter(F.col("_as_of_date") == F.lit(D2).cast("date")) \
        .withColumn("_source_file", F.concat(F.col("_source_file"), F.lit(".resent")))
    dup.write.format("delta").mode("append").saveAsTable("bronze.loans")
    run(spark, SilverConfig(catalog="", as_of_date=D2))
    gold_run(spark, GoldConfig(catalog="", as_of_date=D2))
    assert _silver_fingerprint(spark) == published["silver_ok"]
    assert _gold_fingerprint(spark) == published["gold_ok"]
    _assert_integrity(spark)
    st = spark.sql(f"SELECT status FROM ops.file_manifest WHERE as_of_date = '{D2}' "
                   "AND table_name = 'loan_snapshots'").first()[0]
    assert st == "MULTIPLE_FILES"


def test_5_reconciliation_failure_keeps_serving_gold(spark, published):
    from pyspark.sql import functions as F

    from pipelines.gold import GateFailed, GoldConfig
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
    assert _gold_fingerprint(spark) == published["gold_ok"]
    assert spark.table("ops.published_batches").count() == n_pub
    # có version trước publish của mọi bảng Gold (để RESTORE)
    assert spark.sql("SELECT count(DISTINCT table_name) FROM ops.publish_versions "
                     "WHERE layer = 'gold'").first()[0] == 17
    assert _runs(spark, "gold")[-1] == (D2, "FAILED")
    gate = spark.sql("SELECT status, error FROM ops.task_runs WHERE layer = 'gold' "
                     "AND step = 'gold:gate' ORDER BY logged_at DESC LIMIT 1").first()
    assert gate.status == "FAILED" and "Gate" in gate.error


def test_5b_failure_midway_through_gold_publish_is_rolled_back(spark, published, monkeypatch):
    """Lỗi hạ tầng khi đang ghi bảng Gold thứ N/17: mọi bảng đã ghi phải được RESTORE,
    published_batches không có dòng mới -> dashboard vẫn ở batch cũ, không trộn mới / cũ."""
    import pipelines.gold as pg
    from pipelines.gold import GoldConfig
    from pipelines.gold import run as gold_run

    n_pub = spark.table("ops.published_batches").count()
    real = pg.overwrite_date

    def boom(df, target, *a, **k):
        if target.endswith("card_mart_kpi_daily"):   # ~ bảng thứ 11: 10 bảng trước đã ghi
            raise OSError("storage timeout (giả lập)")
        return real(df, target, *a, **k)

    monkeypatch.setattr(pg, "overwrite_date", boom)
    with pytest.raises(OSError):
        gold_run(spark, GoldConfig(catalog="", as_of_date=D2))
    assert _gold_fingerprint(spark) == published["gold_ok"]
    assert spark.table("ops.published_batches").count() == n_pub
    last = spark.sql("SELECT status, message FROM ops.pipeline_runs WHERE layer = 'gold' "
                     "ORDER BY started_at DESC, status LIMIT 1").first()
    assert last.status == "FAILED" and "rollback" in last.message

    monkeypatch.setattr(pg, "overwrite_date", real)
    gold_run(spark, GoldConfig(catalog="", as_of_date=D2))      # hạ tầng ổn -> chạy lại
    assert _gold_fingerprint(spark) == published["gold_ok"]
    _assert_integrity(spark)


def test_6_out_of_order_rerun_rejected_without_damage(spark, published):
    from pipelines.gold import GoldConfig, NotReproducible
    from pipelines.gold import run as gold_run
    from pipelines.silver import OutOfOrder, SilverConfig, run

    before = _gold_fingerprint(spark)
    with pytest.raises(OutOfOrder):
        run(spark, SilverConfig(catalog="", as_of_date=D1))
    with pytest.raises(NotReproducible):
        gold_run(spark, GoldConfig(catalog="", as_of_date=D1))
    assert _gold_fingerprint(spark) == before


def _resend(spark, table, day):
    """Nguồn gửi lại file của `day` SAU khi Silver ngày đó đã SUCCESS (file correction)."""
    from pyspark.sql import functions as F

    (spark.table(f"bronze.{table}").filter(F.col("_as_of_date") == F.lit(day).cast("date"))
     .withColumn("_source_file", F.concat(F.col("_source_file"), F.lit(".corrected")))
     .withColumn("_load_ts", F.current_timestamp())
     .write.format("delta").mode("append").saveAsTable(f"bronze.{table}"))


def test_6b_correction_reprocesses_silver_before_gold(spark, published):
    from pipelines import daily
    from pipelines.silver import SilverConfig

    n_silver = len(_runs(spark, "silver"))
    _resend(spark, "loans", D2)
    assert daily.run(spark, SilverConfig(catalog="")) == []        # không có ngày mới
    # Silver D2 được xử lý lại TRƯỚC Gold D2 (không dựng Gold từ Silver cũ)
    new_runs = _runs(spark, "silver")[n_silver:]
    assert new_runs == [(D2, "RUNNING"), (D2, "SUCCESS")]
    gold_last = spark.sql("SELECT max(started_at) FROM ops.pipeline_runs WHERE layer = 'gold' "
                          f"AND as_of_date = '{D2}' AND status = 'SUCCESS'").first()[0]
    silver_last = spark.sql("SELECT max(finished_at) FROM ops.pipeline_runs WHERE "
                            f"layer = 'silver' AND as_of_date = '{D2}' AND status = 'SUCCESS'"
                            ).first()[0]
    assert gold_last >= silver_last
    assert _silver_fingerprint(spark) == published["silver_ok"]     # gửi lại cùng nội dung
    assert _gold_fingerprint(spark) == published["gold_ok"]
    assert daily.run(spark, SilverConfig(catalog="")) == []        # đã xử lý: không lặp lại
    assert len(_runs(spark, "silver")) == n_silver + 2


def test_6d_correction_with_changed_values_replaces_the_day(spark, published):
    """File sửa thật: 1 khoản vay đổi số liệu, 1 khoản vay bị nguồn rút khỏi file.
    Không được: cả 2 bản cùng khóa bị quarantine (trùng khóa) và Silver giữ giá trị cũ."""
    from pyspark.sql import functions as F

    from pipelines import daily
    from pipelines.silver import SilverConfig

    day = F.lit(D2).cast("date")
    changed, removed = [r.acctnbr for r in spark.table("silver.loan_snapshots")
                        .filter(F.col("as_of_date") == day).orderBy("acctnbr").limit(2).collect()]
    (spark.table("bronze.loans").filter(F.col("_as_of_date") == day)
     .filter(F.col("acctnbr") != removed)
     .withColumn("thulai", F.when(F.col("acctnbr") == changed, F.lit("777"))
                 .otherwise(F.col("thulai")))
     .withColumn("_source_file", F.concat(F.col("_source_file"), F.lit(".corrected")))
     .withColumn("_load_ts", F.current_timestamp())
     .write.format("delta").mode("append").saveAsTable("bronze.loans"))

    assert daily.run(spark, SilverConfig(catalog="")) == []
    silver_d2 = spark.table("silver.loan_snapshots").filter(F.col("as_of_date") == day)
    assert silver_d2.filter(F.col("acctnbr") == removed).count() == 0
    assert float(silver_d2.filter(F.col("acctnbr") == changed).first().thulai) == 777.0
    gold_d2 = spark.table("gold.loan_fact_daily").filter(F.col("as_of_date") == day)
    assert gold_d2.filter(F.col("acctnbr") == removed).count() == 0
    assert gold_d2.count() == silver_d2.count()
    assert spark.sql("SELECT count(*) FROM ops.dq_quarantine WHERE table_name = "
                     "'loan_snapshots' AND array_contains(failed_rules, 'loan_key_unique')"
                     ).first()[0] == 0
    # D1 không bị đụng tới
    d1 = spark.table("silver.loan_snapshots").filter(F.col("as_of_date") == F.lit(D1).cast("date"))
    assert d1.filter(F.col("acctnbr") == removed).count() == 1
    _assert_integrity(spark)


def test_6c_correction_older_than_watermark_stops_for_replay(spark, published):
    from pipelines import daily
    from pipelines.daily import CorrectionNeedsReplay
    from pipelines.silver import SilverConfig

    _resend(spark, "loans", D1)
    with pytest.raises(CorrectionNeedsReplay, match=D1):
        daily.run(spark, SilverConfig(catalog=""))
    assert _silver_fingerprint(spark) == published["silver_ok"]
    assert _gold_fingerprint(spark) == published["gold_ok"]


def test_7_daily_runner_catches_up_silver_and_gold_per_day(spark, fresh_bronze):
    """Gián đoạn nhiều ngày: Silver + Gold phải chạy XEN KẼ từng ngày (snapshot MERGE)."""
    from pipelines import daily
    from pipelines.silver import SilverConfig
    assert daily.run(spark, SilverConfig(catalog="")) == [D1, D2]
    assert _runs(spark, "gold") == [(D1, "RUNNING"), (D1, "SUCCESS"),
                                    (D2, "RUNNING"), (D2, "SUCCESS")]
    # chạy bù nhiều ngày một lần = chạy từng ngày (cùng nội dung)
    assert _silver_fingerprint(spark) == fresh_bronze["silver_ok"]
    assert _gold_fingerprint(spark) == fresh_bronze["gold_ok"]
    _assert_integrity(spark)
