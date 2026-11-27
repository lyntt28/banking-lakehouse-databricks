# Databricks notebook source
# MAGIC %md
# MAGIC # 00.5 - Backfill Silver + Gold
# MAGIC Gọi `02_silver/00_daily_silver_gold` 1 lần: xử lý mọi ngày sau watermark
# MAGIC (`ops.pipeline_runs`), mỗi ngày Silver rồi Gold.
# MAGIC - `reset=true` (CHỈ dev/test): xoá bảng Silver + Gold + trạng thái batch rồi dựng lại từ đầu.
# MAGIC   Bị chặn trên catalog prod: prod dùng quy trình staging -> gate -> publish (docs/runbook.md),
# MAGIC   không drop bảng mà người dùng đang đọc. Sau reset phải chạy lại governance (mask/filter
# MAGIC   gắn trên bảng bị drop sẽ mất).

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.dropdown("reset", "false", ["false", "true"])
catalog = dbutils.widgets.get("catalog")
reset = dbutils.widgets.get("reset") == "true"

# COMMAND ----------

import os
import sys
import uuid
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from common.batch import assert_reset_allowed  # noqa: E402
from common.writer_lock import acquire, release  # noqa: E402

# giữ khóa ghi suốt reset + backfill; notebook daily con dùng lại cùng owner (re-entrant)
lock_owner = str(uuid.uuid4())
acquire(spark, catalog, lock_owner, "backfill")

try:
    if reset:
        assert_reset_allowed(catalog)
        for schema in ["silver", "silver_stage", "gold", "gold_stage", "gold_serving"]:
            if not spark.catalog.databaseExists(f"{catalog}.{schema}"):
                continue
            for t in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect():
                if not t.isTemporary:
                    kind = "VIEW" if schema == "gold_serving" else "TABLE"   # chỉ chứa view
                    spark.sql(f"DROP {kind} IF EXISTS {catalog}.{schema}.`{t.tableName}`")
                    print(f"dropped {catalog}.{schema}.{t.tableName}")
        for t in ["pipeline_runs", "published_batches", "publish_versions"]:
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.ops.{t}")
except BaseException:
    release(spark, catalog, lock_owner)   # reset lỗi: nhả khóa, không chờ hết hạn
    raise

# COMMAND ----------

# Silver + Gold xen kẽ từng ngày (bắt buộc: Silver snapshot là MERGE, xem pipelines/daily.py)
try:
    dbutils.notebook.run("../02_silver/00_daily_silver_gold", 14400,
                         {"catalog": catalog, "as_of_date": "", "lock_owner": lock_owner})
finally:
    release(spark, catalog, lock_owner)
display(spark.table(f"{catalog}.ops.pipeline_runs").orderBy("as_of_date", "layer", "started_at"))
