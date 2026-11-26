# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: bronze -> ép kiểu, khử trùng, DQ gate, MERGE / SCD2
# MAGIC - `as_of_date` trống = xử lý MỌI ngày trong bronze sau watermark (`ops.pipeline_runs`),
# MAGIC   tăng dần -> job bị gián đoạn vài ngày tự bù đủ. Có `as_of_date` = chạy lại đúng 1 ngày.
# MAGIC - Ngày thiếu file của bảng bắt buộc -> dừng (không bỏ qua bảng rồi chạy tiếp).
# MAGIC - Bản ghi vi phạm rule FAIL -> `ops.dq_quarantine`; kết quả từng rule -> `ops.dq_results`.
# MAGIC - Rule FAIL vượt `max_fail_pct` % -> dừng job TRƯỚC khi ghi bảng silver đó.
# MAGIC - Đối soát mỗi bảng: bronze = silver + quarantine + dòng trùng y hệt.
# MAGIC - Chạy lại cùng ngày an toàn: MERGE theo khóa, SCD2 so hash, quarantine xoá lô cũ.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("as_of_date", "")
dbutils.widgets.text("max_fail_pct", "5")
catalog = dbutils.widgets.get("catalog")
as_of_date = dbutils.widgets.get("as_of_date").strip()
max_fail_pct = float(dbutils.widgets.get("max_fail_pct"))

# COMMAND ----------

import json
import os
import sys
import uuid
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from pyspark.sql import functions as F  # noqa: E402

from common.batch import (  # noqa: E402,E501
    PIPELINE_RUNS_DDL,
    missing_tables,
    pending_dates,
    record_run,
    watermark,
)
from common.delta_io import merge_upsert, scd2_upsert  # noqa: E402
from common.dq import dq_summary, should_stop  # noqa: E402
from common.governance import assert_pipeline_identity  # noqa: E402
from common.silver import (  # noqa: E402
    CUSTOMER_TRACKED,
    ORDER,
    TABLES,
    prepare_silver,
    quarantine_records,
)

spark.sql(PIPELINE_RUNS_DDL.format(catalog=catalog))
bronze_dates = [r[0] for r in spark.table(f"{catalog}.bronze.loans")
                .select("_as_of_date").distinct().collect()]
days = pending_dates(bronze_dates, watermark(spark, catalog, "silver"), as_of_date or None)
assert_pipeline_identity(spark, catalog)  # governance bật -> phải thấy PII
print(f"catalog={catalog} pending={days}")

# COMMAND ----------


def ref_keys(table: str, known: dict):
    """Khóa tham chiếu cho FK = khóa đã có ở silver + khóa xuất hiện trong lô hôm nay."""
    col = TABLES[table].key[0]
    keys = spark.table(f"{catalog}.silver.{table}").select(col)
    if table in known:
        keys = keys.unionByName(known[table])
    return keys.distinct()


def process_day(as_of_date: str, run_id: str) -> None:
    known = {}   # bảng -> khóa có trong lô hôm nay (kể cả bản ghi bị quarantine)
    bronze_of = {name: spark.table(f"{catalog}.bronze.{TABLES[name].bronze}").filter(
        F.col("_as_of_date") == F.lit(as_of_date).cast("date")) for name in ORDER}
    counts = {name: df.count() for name, df in bronze_of.items()}
    missing = missing_tables(counts, ORDER)
    if missing:
        raise RuntimeError(f"{as_of_date}: thiếu file nguồn {missing} - chờ file về rồi chạy lại")

    report = []
    for name in ORDER:
        spec = TABLES[name]
        bronze = bronze_of[name]
        n_bronze = counts[name]
        if n_bronze == 0:
            print(f"{name}: bảng tham chiếu không gửi ngày {as_of_date}, giữ bản hiện có")
            continue

        res = prepare_silver(bronze, spec, ref_keys(spec.fk[1], known) if spec.fk else None)
        # Không .cache(): serverless (Free Edition) báo NOT_SUPPORTED_WITH_SERVERLESS
        known[name] = res.known_keys(spec.key[0])

        # 1) Kết quả DQ
        summary = dq_summary(res.checked, spec.rules, name)
        summary.select(F.lit(run_id).alias("run_id"), F.current_timestamp().alias("run_ts"), "*") \
            .write.mode("append").saveAsTable(f"{catalog}.ops.dq_results")

        # 2) Quarantine: xoá bản ghi lỗi cũ của cùng file (khi chạy lại), rồi ghi lô mới
        files = [r[0] for r in bronze.select("_source_file").distinct().collect()]
        if files:
            in_list = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
            spark.sql(f"DELETE FROM {catalog}.ops.dq_quarantine "
                      f"WHERE table_name = '{name}' AND _source_file IN ({in_list})")
        quarantine_records(res.quarantine, name, run_id) \
            .write.mode("append").saveAsTable(f"{catalog}.ops.dq_quarantine")

        # 3) DQ gate
        if should_stop(summary, max_fail_pct):
            display(summary)
            raise RuntimeError(f"DQ gate: {name} có rule FAIL > {max_fail_pct}% (run_id={run_id})")

        # 4) Ghi silver
        target = f"{catalog}.silver.{name}"
        if spec.write_mode == "scd2":
            scd2_upsert(res.valid, target, spec.key[0], CUSTOMER_TRACKED, as_of_date)
        else:
            merge_upsert(res.valid, target, spec.key, insert_only=spec.write_mode == "append")

        n_valid, n_quar = res.valid.count(), res.quarantine.count()
        report.append((name, n_bronze, n_valid, n_quar, res.exact_dupes,
                       n_bronze == n_valid + n_quar + res.exact_dupes))

    rep = spark.createDataFrame(report, "table string, bronze long, silver long, quarantine long, "
                                        "exact_dupes long, reconciled boolean")
    display(rep)
    bad = [r.table for r in rep.collect() if not r.reconciled]
    if bad:
        raise RuntimeError(f"Đối soát lệch: {bad}")


if not days:
    print("Không có ngày mới sau watermark - không làm gì")
for d in days:
    run_id = str(uuid.uuid4())
    record_run(spark, catalog, d, "silver", run_id, "RUNNING")
    try:
        process_day(d, run_id)
    except Exception as e:
        record_run(spark, catalog, d, "silver", run_id, "FAILED", f"{type(e).__name__}: {e}")
        raise   # dừng: ngày sau không được chạy khi ngày trước lỗi (SCD2 cần đúng thứ tự)
    record_run(spark, catalog, d, "silver", run_id, "SUCCESS")
    print("silver xong", d)

try:  # chỉ Job task mới set được task value; dbutils.notebook.run (backfill) thì bỏ qua
    dbutils.jobs.taskValues.set(key="as_of_dates", value=json.dumps(days))
except Exception as e:  # noqa: BLE001
    print(f"Bỏ qua taskValues.set (không phải task run): {type(e).__name__}")
