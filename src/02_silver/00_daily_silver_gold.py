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
# data contract: ngày nguồn bắt đầu gửi file hằng ngày (trước đó: snapshot cuối tháng)
dbutils.widgets.text("daily_feed_start", "")
# SLA file thiếu: job chạy ngày T phải có dữ liệu ngày T - lag (giờ VN). Trống = tắt.
dbutils.widgets.text("sla_lag_days", "")

# COMMAND ----------

import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from datetime import datetime
from zoneinfo import ZoneInfo

from common.batch import expected_through
from common.dates import check_date
from common.governance import assert_pipeline_identity
from pipelines import daily
from pipelines.silver import SilverConfig

as_of = dbutils.widgets.get("as_of_date").strip()
daily_from = dbutils.widgets.get("daily_feed_start").strip()
lag = dbutils.widgets.get("sla_lag_days").strip()
today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date().isoformat()
cfg = SilverConfig(catalog=dbutils.widgets.get("catalog"),
                   as_of_date=check_date(as_of) if as_of else "",
                   max_fail_pct=float(dbutils.widgets.get("max_fail_pct")),
                   daily_from=check_date(daily_from) if daily_from else "",
                   expected_until=expected_through(today, int(lag)) if lag else "")
print("daily_feed_start:", cfg.daily_from or "-", "| expected_until:", cfg.expected_until or "-")
assert_pipeline_identity(spark, cfg.catalog)
days = daily.run(spark, cfg)
print("processed:", days)
