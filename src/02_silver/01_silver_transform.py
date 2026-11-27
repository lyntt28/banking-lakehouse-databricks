# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: bronze -> ép kiểu, khử trùng, DQ gate, MERGE / SCD2
# MAGIC Notebook mỏng: đọc tham số rồi gọi `pipelines.silver.run` (logic + test ở `src/pipelines`).
# MAGIC - `as_of_date` trống = mọi ngày bronze sau watermark (`ops.pipeline_runs`), tăng dần.
# MAGIC - Thiếu file bảng bắt buộc / DQ gate / đối soát lệch -> task FAILED (xem `ops.task_runs`,
# MAGIC   `ops.file_manifest`, `ops.dq_results`, dashboard *Lakehouse Operations Monitor*).

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
from pipelines.silver import SilverConfig, run

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
assert_pipeline_identity(spark, cfg.catalog)  # governance bật -> phải thấy PII
days = run(spark, cfg)
print("processed:", days)
