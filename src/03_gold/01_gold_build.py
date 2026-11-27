# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: star schema + mart cho dashboard
# MAGIC Notebook mỏng: gọi `pipelines.gold.run` (logic + test ở `src/pipelines`).
# MAGIC - Dimension: `dim_date`, `dim_branch`, `dim_product`, `dim_customer` (SCD2).
# MAGIC - Fact: `loan_fact_daily`, `core_fact_transaction`. Mart: xem `docs/gold_dashboards.md`.
# MAGIC - **Gate trước publish**: mart đối soát ghi vào `gold_stage`, đối soát (dư nợ Silver–Gold,
# MAGIC   KPI thẻ, tiền gửi bang1247) chạy trên bản stage; lệch -> dừng, bảng `gold` giữ nguyên.
# MAGIC   Đạt -> publish + `ops.published_batches`.
# MAGIC - `as_of_date` trống = mọi ngày Silver SUCCESS mà Gold chưa SUCCESS.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("as_of_date", "")

# COMMAND ----------

import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from common.dates import check_date
from common.governance import assert_pipeline_identity
from common.writer_lock import writer_lock
from pipelines.gold import GoldConfig, run

as_of = dbutils.widgets.get("as_of_date").strip()
cfg = GoldConfig(catalog=dbutils.widgets.get("catalog"),
                 as_of_date=check_date(as_of) if as_of else "")
assert_pipeline_identity(spark, cfg.catalog)  # governance bật -> phải thấy PII
with writer_lock(spark, cfg.catalog, "gold"):   # 1 writer / lakehouse
    days = run(spark, cfg)

# COMMAND ----------

if days:
    display(spark.table(f"{cfg.catalog}.gold.loan_mart_npl_daily")
            .orderBy("as_of_date", ascending=False).limit(10))
