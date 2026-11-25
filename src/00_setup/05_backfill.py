# Databricks notebook source
# MAGIC %md
# MAGIC # 00.5 - Backfill Silver + Gold cho mọi ngày có trong Bronze (cũ -> mới)
# MAGIC Job hằng ngày chỉ xử lý ngày mới nhất. Sau khi sinh dữ liệu có `history_months`
# MAGIC (13 snapshot cuối tháng), chạy notebook này 1 lần để Silver (SCD2) và Gold (so sánh
# MAGIC D-1 / M-1 / Y-1) được dựng đúng thứ tự thời gian. Ngày đã xử lý chạy lại cũng an toàn.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("from_date", "")   # trống = ngày sớm nhất trong bronze
catalog = dbutils.widgets.get("catalog")
from_date = dbutils.widgets.get("from_date").strip()

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

display(spark.table(f"{catalog}.gold.mart_npl_daily").orderBy(F.desc("as_of_date")))
