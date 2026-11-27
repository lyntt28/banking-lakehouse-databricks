# Databricks notebook source
# MAGIC %md
# MAGIC # 02.0 - Daily: Silver -> Gold xen kẽ từng ngày (task chính của job hằng ngày)
# MAGIC - `as_of_date` trống: mọi ngày bronze sau watermark; mỗi ngày chạy Silver rồi Gold (gate +
# MAGIC   publish) trước khi sang ngày sau – bắt buộc vì Silver snapshot (TK, thẻ) là MERGE.
# MAGIC - Gold lần trước fail gate -> dựng lại ngày đó trước.
# MAGIC - Notebook `01_silver_transform` / `03_gold/01_gold_build` vẫn dùng được để chạy riêng 1 ngày.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("as_of_date", "")
dbutils.widgets.text("max_fail_pct", "5")
dbutils.widgets.text("expected_lag_days", "")   # "" = tắt ngày dự kiến / SLA
dbutils.widgets.text("sla_hour", "9")
dbutils.widgets.text("daily_from", "")          # "" = tự suy ra

# COMMAND ----------

import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from common.dates import check_date
from common.governance import assert_pipeline_identity
from pipelines import daily
from pipelines.silver import SilverConfig

as_of = dbutils.widgets.get("as_of_date").strip()
cfg = SilverConfig(catalog=dbutils.widgets.get("catalog"),
                   as_of_date=check_date(as_of) if as_of else "",
                   max_fail_pct=float(dbutils.widgets.get("max_fail_pct")),
                   expected_lag_days=(int(lag) if (lag := dbutils.widgets.get(
                       "expected_lag_days").strip()) else None),
                   sla_hour=int(dbutils.widgets.get("sla_hour") or 9),
                   daily_from=dbutils.widgets.get("daily_from").strip())
assert_pipeline_identity(spark, cfg.catalog)
days = daily.run(spark, cfg)
print("processed:", days)
