# Databricks notebook source
# MAGIC %md
# MAGIC # 00.5 - Backfill Silver + Gold
# MAGIC Silver/Gold tự xử lý mọi ngày sau watermark (`ops.pipeline_runs`), nên backfill chỉ cần
# MAGIC gọi 2 notebook 1 lần, không truyền ngày.
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
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from common.batch import assert_reset_allowed  # noqa: E402

if reset:
    assert_reset_allowed(catalog)
    for schema in ["silver", "gold", "gold_stage"]:
        if not spark.catalog.databaseExists(f"{catalog}.{schema}"):
            continue
        for t in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect():
            if not t.isTemporary:
                spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.`{t.tableName}`")
                print(f"dropped {catalog}.{schema}.{t.tableName}")
    for t in ["pipeline_runs", "published_batches"]:
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.ops.{t}")

# COMMAND ----------

args = {"catalog": catalog, "as_of_date": ""}
dbutils.notebook.run("../02_silver/01_silver_transform", 7200, args)
dbutils.notebook.run("../03_gold/01_gold_build", 7200, args)
display(spark.table(f"{catalog}.ops.pipeline_runs").orderBy("as_of_date", "layer", "started_at"))
