"""Pipeline Silver: bronze -> ép kiểu, khử trùng, DQ gate, MERGE / SCD2.

Notebook `src/02_silver/01_silver_transform.py` chỉ đọc widget rồi gọi `run()`. Mọi logic ở đây
import và test được trên Spark + Delta local (tests/integration).

Mỗi ngày dữ liệu:
    1. file manifest  - bảng bắt buộc thiếu file -> FAILED, dừng (ops.file_manifest)
    2. từng bảng theo ORDER: DQ -> quarantine -> DQ gate -> ghi silver -> đối soát số dòng
    3. ghi ops.pipeline_runs (RUNNING/SUCCESS/FAILED) + ops.task_runs (từng bước)
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common.batch import (
    ensure_ops_tables,
    manifest_status,
    missing_tables,
    pending_dates,
    record_run,
    required_tables,
    watermark,
)
from common.delta_io import merge_upsert, overwrite_date, scd2_upsert
from common.dq import dq_summary, should_stop
from common.names import fq
from common.ops_log import StepRecorder, log
from common.silver import CUSTOMER_TRACKED, ORDER, TABLES, prepare_silver, quarantine_records


@dataclass(frozen=True)
class SilverConfig:
    catalog: str
    as_of_date: str = ""        # trống = mọi ngày sau watermark
    max_fail_pct: float = 5.0

    def t(self, schema: str, table: str) -> str:
        return fq(self.catalog, schema, table)


class OutOfOrder(RuntimeError):
    """Chạy lại ngày cũ hơn watermark: MERGE snapshot ngày cũ sẽ ghi đè trạng thái mới hơn."""


class SourceFilesMissing(RuntimeError):
    pass


class DQGateFailed(RuntimeError):
    pass


class ReconciliationFailed(RuntimeError):
    pass


def ensure_tables(spark: SparkSession, cfg: SilverConfig) -> None:
    ensure_ops_tables(spark, cfg.catalog)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq(cfg.catalog, 'silver')}")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cfg.t('ops', 'dq_results')} (
        run_id STRING, run_ts TIMESTAMP, table_name STRING, rule_name STRING, severity STRING,
        total_rows BIGINT, failed_rows BIGINT, fail_pct DOUBLE, status STRING) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cfg.t('ops', 'dq_quarantine')} (
        run_id STRING, table_name STRING, failed_rules ARRAY<STRING>, record_json STRING,
        _source_file STRING, quarantined_at TIMESTAMP) USING DELTA""")


def bronze_for_day(spark: SparkSession, cfg: SilverConfig, day: str) -> dict[str, DataFrame]:
    return {name: spark.table(cfg.t("bronze", TABLES[name].bronze))
            .filter(F.col("_as_of_date") == F.lit(day).cast("date")) for name in ORDER}


def build_manifest(spark: SparkSession, bronze: dict[str, DataFrame], day: str) -> DataFrame:
    rows = []
    for name, df in bronze.items():
        stats = (df.groupBy("_source_file").agg(F.count("*").alias("n"),
                                                F.min("_load_ts").alias("ts")).collect())
        files = sorted(r["_source_file"] for r in stats)
        n = sum(r["n"] for r in stats)
        first = min((r["ts"] for r in stats if r["ts"] is not None), default=None)
        rows.append((day, name, name in required_tables(ORDER),
                     manifest_status(name, len(files), ORDER), len(files), n, files, first))
    return (spark.createDataFrame(
        rows, "as_of_date string, table_name string, required boolean, status string, "
              "file_count long, rows long, source_files array<string>, "
              "first_loaded_at timestamp")
        .withColumn("as_of_date", F.col("as_of_date").cast("date"))
        .withColumn("checked_at", F.current_timestamp()))


def _ref_keys(spark, cfg: SilverConfig, table: str, known: dict) -> DataFrame:
    """Khóa tham chiếu cho FK = khóa đã có ở silver + khóa xuất hiện trong lô hôm nay."""
    col = TABLES[table].key[0]
    target = cfg.t("silver", table)
    keys = known.get(table)
    if spark.catalog.tableExists(target):
        old = spark.table(target).select(col)
        keys = old if keys is None else keys.unionByName(old)
    return keys.distinct()


def process_day(spark: SparkSession, cfg: SilverConfig, day: str, rec: StepRecorder) -> list:
    run_id = rec.run_id
    bronze = bronze_for_day(spark, cfg, day)

    with rec.step("file_manifest") as s:
        manifest = build_manifest(spark, bronze, day)
        overwrite_date(manifest, cfg.t("ops", "file_manifest"), "as_of_date", day)
        counts = {r.table_name: r.rows for r in manifest.collect()}
        s.rows_in = sum(counts.values())
        missing = missing_tables(counts, ORDER)
        if missing:
            raise SourceFilesMissing(f"{day}: thiếu file nguồn {missing} (chờ file về)")

    report, known = [], {}
    for name in ORDER:
        spec = TABLES[name]
        df = bronze[name]
        n_bronze = counts[name]
        if n_bronze == 0:
            log("table_not_sent", table=name, as_of_date=day, run_id=run_id)
            continue
        with rec.step(f"silver:{name}") as s:
            s.rows_in = n_bronze
            res = prepare_silver(df, spec, _ref_keys(spark, cfg, spec.fk[1], known)
                                 if spec.fk else None)
            known[name] = res.known_keys(spec.key[0])

            summary = dq_summary(res.checked, spec.rules, name)
            (summary.select(F.lit(run_id).alias("run_id"),
                            F.current_timestamp().alias("run_ts"), "*")
             .write.format("delta").mode("append").saveAsTable(cfg.t("ops", "dq_results")))

            # quarantine: xoá bản ghi lỗi cũ của cùng file (khi chạy lại), rồi ghi lô mới
            files = [r[0] for r in df.select("_source_file").distinct().collect()]
            if files:
                in_list = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
                spark.sql(f"DELETE FROM {cfg.t('ops', 'dq_quarantine')} "
                          f"WHERE table_name = '{name}' AND _source_file IN ({in_list})")
            quarantine_records(res.quarantine, name, run_id) \
                .write.format("delta").mode("append").saveAsTable(cfg.t("ops", "dq_quarantine"))

            if should_stop(summary, cfg.max_fail_pct):
                raise DQGateFailed(f"DQ gate: {name} có rule FAIL > {cfg.max_fail_pct}% "
                                   f"(run_id={run_id}, xem ops.dq_results)")

            target = cfg.t("silver", name)
            if spec.write_mode == "scd2":
                scd2_upsert(res.valid, target, spec.key[0], CUSTOMER_TRACKED, day)
            else:
                merge_upsert(res.valid, target, spec.key, insert_only=spec.write_mode == "append")

            n_valid, n_quar = res.valid.count(), res.quarantine.count()
            s.rows_out = n_valid
            ok = n_bronze == n_valid + n_quar + res.exact_dupes
            report.append((name, n_bronze, n_valid, n_quar, res.exact_dupes, ok))
            if not ok:
                raise ReconciliationFailed(
                    f"Đối soát lệch {name}: bronze={n_bronze} silver={n_valid} "
                    f"quarantine={n_quar} dupes={res.exact_dupes}")
    return report


def run(spark: SparkSession, cfg: SilverConfig) -> list[str]:
    """Xử lý các ngày chờ, tăng dần. Trả về danh sách ngày đã SUCCESS. Lỗi -> raise."""
    ensure_tables(spark, cfg)
    bronze_dates = [r[0] for r in spark.table(cfg.t("bronze", "loans"))
                    .select("_as_of_date").distinct().collect()]
    wm = watermark(spark, cfg.catalog, "silver")
    if cfg.as_of_date and wm and cfg.as_of_date < wm:
        raise OutOfOrder(f"Silver đã ở {wm}: không chạy lại {cfg.as_of_date} (sẽ ghi đè snapshot "
                         "mới bằng snapshot cũ). Dựng lại = replay từ ngày đó, xem runbook")
    days = pending_dates(bronze_dates, wm, cfg.as_of_date or None)
    log("silver_pending", catalog=cfg.catalog, days=days)
    done = []
    for day in days:
        rec = StepRecorder(str(uuid.uuid4()), day, "silver")
        record_run(spark, cfg.catalog, day, "silver", rec.run_id, "RUNNING")
        try:
            process_day(spark, cfg, day, rec)
        except Exception as e:
            rec.flush(spark, cfg.t("ops", "task_runs"))
            record_run(spark, cfg.catalog, day, "silver", rec.run_id, "FAILED",
                       f"{type(e).__name__}: {e}")
            log("silver_failed", as_of_date=day, run_id=rec.run_id, error=str(e))
            raise   # ngày sau không chạy khi ngày trước lỗi (SCD2 cần đúng thứ tự)
        rec.flush(spark, cfg.t("ops", "task_runs"))
        record_run(spark, cfg.catalog, day, "silver", rec.run_id, "SUCCESS")
        log("silver_success", as_of_date=day, run_id=rec.run_id)
        done.append(day)
    return done
