# Databricks notebook source
# MAGIC %md
# MAGIC # 00.5 - Backfill Silver + Gold cho mọi ngày có trong Bronze (cũ -> mới)
# MAGIC Job hằng ngày chỉ xử lý ngày mới nhất. Sau khi sinh dữ liệu có `history_months`
# MAGIC (13 snapshot cuối tháng), chạy notebook này 1 lần để Silver (SCD2) và Gold (so sánh
# MAGIC D-1 / M-1 / Y-1) được dựng đúng thứ tự thời gian. Ngày đã xử lý chạy lại cũng an toàn.
# MAGIC - `reset=true`: xoá toàn bộ bảng Silver + Gold rồi dựng lại từ ngày sớm nhất (cần khi
# MAGIC   sinh lại dữ liệu nguồn, hoặc SCD2 báo "đã có bản ghi hiệu lực sau ..."). Bronze giữ nguyên.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("from_date", "")   # trống = ngày sớm nhất trong bronze
catalog = dbutils.widgets.get("catalog")
from_date = dbutils.widgets.get("from_date").strip()
dbutils.widgets.dropdown("reset", "false", ["false", "true"])
reset = dbutils.widgets.get("reset") == "true"
if reset and from_date:
    raise ValueError("reset=true dựng lại từ đầu: để trống from_date")

# COMMAND ----------

if reset:
    for schema in ["silver", "gold"]:
        for t in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect():
            if not t.isTemporary:
                spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.`{t.tableName}`")
                print(f"dropped {catalog}.{schema}.{t.tableName}")

# COMMAND ----------

from pyspark.sql import functions as F

days = [str(r[0]) for r in spark.table(f"{catalog}.bronze.loans")
        .select("_as_of_date").distinct().orderBy("_as_of_date").collect()]
if from_date:
    days = [d for d in days if d >= from_date]
print(f"{len(days)} ngày: {days[:3]} ... {days[-3:]}")

# COMMAND ----------

for d in days:
    args = {"catalog": catalog, "as_of_date": d}
    dbutils.notebook.run("../02_silver/01_silver_transform", 3600, args)
    dbutils.notebook.run("../03_gold/01_gold_build", 3600, args)
    print("xong", d)

display(spark.table(f"{catalog}.gold.loan_mart_npl_daily").orderBy(F.desc("as_of_date")))
