# Databricks notebook source
# MAGIC %md
# MAGIC # 00.3 - Governance: GRANT, che cột PII, lọc dòng theo chi nhánh
# MAGIC - Nhóm `bank_admins`, `data_engineers`, `risk_analysts`, `branch_users` phải có sẵn
# MAGIC   (Account Console). Free Edition không tạo được group -> chạy với `dry_run=true` để xem SQL.
# MAGIC - Gán chi nhánh cho người dùng: `INSERT INTO <catalog>.ops.user_branch VALUES ('a@b.com', '101')`.
# MAGIC - Chạy lại an toàn (CREATE OR REPLACE / IF NOT EXISTS / SET MASK ghi đè).
# MAGIC - Gold ghi đè một số bảng mỗi ngày -> task này chạy SAU gold trong job để gắn lại mask/filter.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.dropdown("dry_run", "false", ["true", "false"])
catalog = dbutils.widgets.get("catalog")
dry_run = dbutils.widgets.get("dry_run") == "true"

# COMMAND ----------

import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from common.governance import statements, target_tables  # noqa: E402

missing = {t for t in target_tables() if not spark.catalog.tableExists(f"{catalog}.{t}")}
if missing:
    print("Bỏ qua (bảng chưa tồn tại):", sorted(missing))

# COMMAND ----------

done, skipped = 0, 0
for sql in statements(catalog):
    if any(f"{catalog}.{t} " in sql + " " for t in missing):
        skipped += 1
        continue
    if dry_run:
        print(sql + ";")
        continue
    spark.sql(sql)
    done += 1
print(f"Đã chạy {done} câu lệnh, bỏ qua {skipped}")
