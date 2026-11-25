# Databricks notebook source
# MAGIC %md
# MAGIC # 00.2 - Sinh dữ liệu giả lập vào Volume landing
# MAGIC Ghi file CSV/JSON (core banking GIẢ LẬP, có lỗi cố ý) vào
# MAGIC `/Volumes/<catalog>/landing/raw_files/<table>/as_of_date=YYYY-MM-DD/`.
# MAGIC Chạy lại với cùng tham số -> ghi đè đúng bộ file cũ (seed cố định).

# COMMAND ----------

# Tham số: catalog, ngày bắt đầu, số ngày cần sinh
dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("start_date", "2026-11-01")
dbutils.widgets.text("days", "3")
catalog = dbutils.widgets.get("catalog")
start_date = dbutils.widgets.get("start_date")
days = int(dbutils.widgets.get("days"))

# COMMAND ----------

# Thêm thư mục gốc repo vào sys.path để import data_generator
# (notebook nằm ở <repo>/src/00_setup -> lùi 2 cấp)
import os
import sys
from datetime import date
from pathlib import Path

repo_root = str(Path(os.getcwd()).parents[1])
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from data_generator.generate import generate  # noqa: E402

# COMMAND ----------

# Sinh data thẳng vào Volume (Volume được mount như thư mục thường)
out = Path(f"/Volumes/{catalog}/landing/raw_files")
counts = generate(out, date.fromisoformat(start_date), days)
for table, n in counts.items():
    print(f"{table:<13} {n:>7} rows")

# COMMAND ----------

display(dbutils.fs.ls(f"/Volumes/{catalog}/landing/raw_files/"))
