# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: star schema + mart cho dashboard
# MAGIC - Dimension: `dim_date`, `dim_branch`, `dim_product`, `dim_customer` (SCD2).
# MAGIC - Fact: `loan_fact_daily` (ngày x khoản vay), `core_fact_transaction`.
# MAGIC - Mart: `loan_mart_overdue_daily`, `loan_mart_npl_daily` (so D-1 / M-1 / Y-1),
# MAGIC   `dep_mart_daily`, `cust_mart_360`. Xem `docs/gold_dashboards.md`.
# MAGIC - Thẻ: `card_mart_kpi_daily`, `card_mart_spend_mix`, `card_mart_kpi_monthly` (MoM/YoY, % chỉ tiêu).
# MAGIC - Tiền gửi: `dep_mart_trend` (D-1/M-1/Y-1), `dep_mart_by_tenor`,
# MAGIC   `dep_mart_casa_customer`, `dep_mart_new_accounts` (Edu-Save).
# MAGIC - Đối soát với file tổng hợp nguồn -> `ops.recon_card_kpi`, `ops.recon_deposit`;
# MAGIC   có MISMATCH -> job lỗi.
# MAGIC - Chạy lại cùng ngày an toàn: fact MERGE theo khóa, mart ghi đè đúng ngày.

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("as_of_date", "")   # trống = ngày của task silver / ngày mới nhất
catalog = dbutils.widgets.get("catalog")
as_of_date = dbutils.widgets.get("as_of_date").strip()
if not as_of_date:
    try:  # chạy trong Job: lấy ngày từ task silver
        as_of_date = dbutils.jobs.taskValues.get(taskKey="silver", key="as_of_date",
                                                 debugValue="")
    except Exception:  # noqa: BLE001
        as_of_date = ""

# COMMAND ----------

import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from common.pipeline import run_gold  # noqa: E402

# Toàn bộ logic ở src/common/pipeline.py (test: tests/test_pipeline_e2e.py).
# Đối soát lệch -> ReconciliationError (job đỏ); chi tiết trong ops.recon_card_kpi / recon_deposit.
result = run_gold(spark, catalog, as_of_date)
print(f"Dư nợ silver={result.silver_loan_balance:,.0f} gold={result.gold_loan_balance:,.0f}")
display(result.recon_card.groupBy("status").count())
display(spark.table(f"{catalog}.gold.loan_mart_npl_daily").orderBy("as_of_date", ascending=False)
        .limit(10))
