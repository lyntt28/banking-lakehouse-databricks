# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: Auto Loader từ Volume landing -> bảng Delta bronze
# MAGIC - Đọc `/Volumes/<catalog>/landing/raw_files/<table>/` (mọi ngày `as_of_date=...`).
# MAGIC - Giữ nguyên dữ liệu (mọi cột STRING) + `_source_file`, `_as_of_date`, `_load_ts`, `_rescued_data`.
# MAGIC - `trigger(availableNow=True)`: chạy như batch, chỉ đọc file MỚI so với lần trước
# MAGIC   (checkpoint ghi nhớ file đã đọc) -> chạy lại không bị nhân đôi dữ liệu.

# COMMAND ----------

# Tham số: catalog (bank_dev / bank_test / bank_prod), danh sách bảng (rỗng = tất cả)
dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("tables", "")
catalog = dbutils.widgets.get("catalog")
only = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]

# COMMAND ----------

# Import code dùng chung trong src/common (notebook nằm ở <repo>/src/01_bronze)
import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from pyspark.sql import functions as F  # noqa: E402

from common.bronze import SOURCES, add_bronze_metadata, autoloader_options  # noqa: E402

# COMMAND ----------

# Volume chứa checkpoint + schema của Auto Loader (tách khỏi dữ liệu)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.ops.checkpoints")
landing = f"/Volumes/{catalog}/landing/raw_files"
ckpt = f"/Volumes/{catalog}/ops/checkpoints/bronze"

# COMMAND ----------

# Nạp từng bảng. availableNow: đọc hết file mới rồi dừng.
results = []
for src in SOURCES:
    if only and src.table not in only:
        continue
    target = f"{catalog}.bronze.{src.table}"
    df = (
        spark.readStream.format("cloudFiles")
        .options(**autoloader_options(src, f"{ckpt}/{src.table}/schema"))
        .load(f"{landing}/{src.table}/")
    )
    df = add_bronze_metadata(df, F.col("_metadata.file_path"))
    q = (
        df.writeStream.option("checkpointLocation", f"{ckpt}/{src.table}/checkpoint")
        .trigger(availableNow=True)
        .toTable(target)
    )
    q.awaitTermination()
    n = spark.table(target).count()
    results.append((src.table, n))
    print(f"{target:<45} {n:>10,} rows")

# COMMAND ----------

# Kiểm tra: số dòng theo bảng và theo ngày
display(spark.createDataFrame(results, "table string, rows long"))
display(spark.sql(f"""
  SELECT _as_of_date, count(*) AS rows, count(DISTINCT _source_file) AS files
  FROM {catalog}.bronze.loans GROUP BY _as_of_date ORDER BY _as_of_date
"""))
