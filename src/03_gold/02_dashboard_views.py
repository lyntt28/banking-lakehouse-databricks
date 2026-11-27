# Databricks notebook source
# MAGIC %md
# MAGIC # 03.2 - View cho 3 dashboard (Card, Overdue, Deposit)
# MAGIC Mỗi visual = 1 view `gold_serving.v_*`, COMMENT ghi dashboard, thứ tự và mô tả visual.
# MAGIC Dashboard chỉ cần `SELECT * FROM <view>`. Row filter / column mask của bảng gốc
# MAGIC vẫn áp dụng khi người dùng đọc qua view.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from common.dashboard_views import VIEWS, view_statements  # noqa: E402

for sql in view_statements(catalog):
    spark.sql(sql)
print(f"Đã tạo {len(VIEWS)} view")
display(spark.createDataFrame([(d, o, n, desc) for d, o, n, desc, _ in VIEWS],
                              "dashboard string, visual_order int, view string, visual string")
        .orderBy("dashboard", "visual_order"))
