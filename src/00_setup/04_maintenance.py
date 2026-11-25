# Databricks notebook source
# MAGIC %md
# MAGIC # 00.4 - Hiệu năng: liquid clustering + OPTIMIZE (+ VACUUM), đo trước / sau
# MAGIC - `mode=benchmark`: chạy truy vấn dashboard -> bật clustering + OPTIMIZE -> chạy lại,
# MAGIC   ghi thời gian + số file / byte đã đọc vào `ops.perf_benchmark`.
# MAGIC - `mode=maintain`: CLUSTER BY + OPTIMIZE + VACUUM (job `banking_maintenance_weekly`).
# MAGIC - Tắt cache kết quả để số đo phản ánh I/O thật.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("as_of_date", "")
dbutils.widgets.dropdown("mode", "benchmark", ["benchmark", "maintain"])
dbutils.widgets.text("repeats", "3")
catalog = dbutils.widgets.get("catalog")
day = dbutils.widgets.get("as_of_date").strip()
mode = dbutils.widgets.get("mode")
repeats = int(dbutils.widgets.get("repeats"))

# COMMAND ----------

import os
import sys
import time
import uuid
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from pyspark.sql import functions as F  # noqa: E402

from common.dates import check_date  # noqa: E402
from common.maintenance import (  # noqa: E402
    CLUSTERING,
    benchmark_sql,
    cluster_statements,
    vacuum_statements,
)

existing = {t for t in CLUSTERING if spark.catalog.tableExists(f"{catalog}.{t}")}
if not day:
    day = str(spark.table(f"{catalog}.gold.fact_loan_daily").agg(F.max("as_of_date")).first()[0])
day = check_date(day)
try:
    spark.conf.set("spark.databricks.io.cache.enabled", "false")
except Exception:  # serverless không cho đổi -> vẫn đo, ghi chú trong kết quả
    pass
spark.sql("SET use_cached_result = false")

# COMMAND ----------


def table_stats(t: str) -> tuple[int, int]:
    d = spark.sql(f"DESCRIBE DETAIL {catalog}.{t}").first()
    return int(d["numFiles"]), int(d["sizeInBytes"])


def run_benchmarks(phase: str, run_id: str) -> list[tuple]:
    rows = []
    for name, sql in benchmark_sql(catalog, day).items():
        times = []
        for _ in range(repeats):
            t0 = time.perf_counter()
            spark.sql(sql).collect()
            times.append(time.perf_counter() - t0)
        times.sort()
        rows.append((run_id, phase, name, times[len(times) // 2], min(times)))
    return rows


if mode == "benchmark":
    run_id = str(uuid.uuid4())
    before_files = {t: table_stats(t) for t in existing}
    results = run_benchmarks("before", run_id)
    for sql in cluster_statements(catalog, existing):
        print(sql)
        spark.sql(sql)
    after_files = {t: table_stats(t) for t in existing}
    results += run_benchmarks("after", run_id)

    res = spark.createDataFrame(results, "run_id string, phase string, query string, "
                                         "median_s double, min_s double")
    res.withColumn("run_ts", F.current_timestamp()).write.mode("append") \
        .saveAsTable(f"{catalog}.ops.perf_benchmark")
    display(res.groupBy("query").pivot("phase", ["before", "after"]).agg(F.first("median_s"))
            .withColumn("speedup_x", F.round(F.col("before") / F.col("after"), 2)))
    files = [(t, *before_files[t], *after_files[t]) for t in sorted(existing)]
    display(spark.createDataFrame(files, "table string, files_before long, bytes_before long, "
                                         "files_after long, bytes_after long"))
else:
    # gắn lại CLUSTER BY (gold ghi đè một số bảng) + OPTIMIZE + VACUUM
    for sql in cluster_statements(catalog, existing) + vacuum_statements(catalog,
                                                                         existing=existing):
        print(sql)
        spark.sql(sql)
