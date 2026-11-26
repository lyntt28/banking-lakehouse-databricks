# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: catalog, schema, volume
# MAGIC Tạo cấu trúc Unity Catalog cho 1 môi trường.
# MAGIC - Tên catalog lấy từ tham số `catalog` (bank_dev / bank_test / bank_prod), không viết cứng.
# MAGIC - Chạy lại nhiều lần vẫn an toàn (IF NOT EXISTS).

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
catalog = dbutils.widgets.get("catalog")
print(f"Setting up catalog: {catalog}")

# COMMAND ----------

# Free Edition: nếu không có quyền tạo catalog, tạo tay 1 lần trên giao diện
# (Catalog > + > Create catalog) rồi chạy lại notebook này.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

for schema in ["landing", "bronze", "silver", "gold", "gold_stage", "ops"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# Volume chứa file nguồn (CSV/JSON) đổ vào hằng ngày
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.landing.raw_files")

# COMMAND ----------

# Bảng vận hành: kết quả Data Quality và bản ghi lỗi (quarantine)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.dq_results (
  run_id STRING,
  run_ts TIMESTAMP,
  table_name STRING,
  rule_name STRING,
  severity STRING,
  total_rows BIGINT,
  failed_rows BIGINT,
  fail_pct DOUBLE,
  status STRING
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.ops.dq_quarantine (
  run_id STRING,
  table_name STRING,
  failed_rules ARRAY<STRING>,
  record_json STRING,
  _source_file STRING,
  quarantined_at TIMESTAMP
)
""")

# Batch control: trạng thái từng ngày / layer + batch Gold đã qua gate
import os  # noqa: E402
import sys  # noqa: E402
from pathlib import Path  # noqa: E402

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)
from common.batch import ensure_ops_tables  # noqa: E402

# pipeline_runs, published_batches, task_runs, file_manifest
ensure_ops_tables(spark, catalog)

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
