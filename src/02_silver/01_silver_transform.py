# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: bronze -> ép kiểu, khử trùng, DQ gate, MERGE / SCD2
# MAGIC - Xử lý 1 ngày (`as_of_date`, để trống = ngày mới nhất có trong bronze).
# MAGIC - Bản ghi vi phạm rule FAIL -> `ops.dq_quarantine`; kết quả từng rule -> `ops.dq_results`.
# MAGIC - Rule FAIL vượt `max_fail_pct` % -> dừng job TRƯỚC khi ghi bảng silver đó.
# MAGIC - Đối soát mỗi bảng: bronze = silver + quarantine + dòng trùng y hệt.
# MAGIC - Chạy lại cùng ngày an toàn: MERGE theo khóa, SCD2 so hash, quarantine xoá lô cũ.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("as_of_date", "")   # trống = ngày mới nhất trong bronze
dbutils.widgets.text("max_fail_pct", "5")
catalog = dbutils.widgets.get("catalog")
as_of_date = dbutils.widgets.get("as_of_date").strip()
max_fail_pct = float(dbutils.widgets.get("max_fail_pct"))

# COMMAND ----------

import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from common.pipeline import run_silver  # noqa: E402

# Toàn bộ logic ở src/common/pipeline.py (test: tests/test_pipeline_e2e.py)
result = run_silver(spark, catalog, as_of_date, max_fail_pct)
display(result.report)

# COMMAND ----------

try:  # chỉ Job task mới set được task value; dbutils.notebook.run thì bỏ qua
    dbutils.jobs.taskValues.set(key="as_of_date", value=result.as_of_date)
except Exception as e:  # noqa: BLE001
    print(f"Bỏ qua taskValues.set (không phải task run): {type(e).__name__}")
