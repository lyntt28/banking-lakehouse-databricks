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
# trước ngày này nguồn chỉ có snapshot cuối tháng (history_months của dữ liệu mẫu)
dbutils.widgets.text("daily_feed_start", "2026-11-01")
catalog = dbutils.widgets.get("catalog")
reset = dbutils.widgets.get("reset") == "true"

# COMMAND ----------

import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from common.batch import assert_reset_allowed  # noqa: E402

if reset:
    assert_reset_allowed(catalog)
    for schema in ["silver", "silver_stage", "gold", "gold_stage"]:
        if not spark.catalog.databaseExists(f"{catalog}.{schema}"):
            continue
        for t in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect():
            if not t.isTemporary:
                spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.`{t.tableName}`")
                print(f"dropped {catalog}.{schema}.{t.tableName}")
    for t in ["pipeline_runs", "published_batches", "publish_versions"]:
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.ops.{t}")

# COMMAND ----------

# Silver + Gold xen kẽ từng ngày (bắt buộc: Silver snapshot là MERGE, xem pipelines/daily.py)
dbutils.notebook.run("../02_silver/00_daily_silver_gold", 14400,
                     {"catalog": catalog, "as_of_date": "",
                      "daily_feed_start": dbutils.widgets.get("daily_feed_start")})
display(spark.table(f"{catalog}.ops.pipeline_runs").orderBy("as_of_date", "layer", "started_at"))
