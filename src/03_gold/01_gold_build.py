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
dbutils.widgets.text("as_of_date", "")
catalog = dbutils.widgets.get("catalog")
as_of_date = dbutils.widgets.get("as_of_date").strip()

# COMMAND ----------

import os
import sys
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from pyspark.sql import functions as F  # noqa: E402

from common import gold as g  # noqa: E402
from common import gold_cards as gc  # noqa: E402
from common import gold_deposit as gd  # noqa: E402
from common.dates import check_date  # noqa: E402
from common.delta_io import merge_upsert, overwrite, overwrite_date  # noqa: E402
from common.governance import assert_pipeline_identity  # noqa: E402

if not as_of_date:
    try:  # chạy trong Job: lấy ngày từ task silver
        as_of_date = dbutils.jobs.taskValues.get(taskKey="silver", key="as_of_date",
                                                 debugValue="")
    except Exception:
        as_of_date = ""
if not as_of_date:
    as_of_date = str(spark.table(f"{catalog}.silver.loan_snapshots").agg(F.max("as_of_date")).first()[0])
as_of_date = check_date(as_of_date)
print(f"catalog={catalog} as_of_date={as_of_date}")
assert_pipeline_identity(spark, catalog)  # governance bật -> phải thấy PII

S = f"{catalog}.silver"
G = f"{catalog}.gold"
day = F.lit(as_of_date).cast("date")

# COMMAND ----------

# Dimension (nhỏ -> ghi đè toàn bộ)
first = spark.table(f"{S}.loan_snapshots").agg(F.min("as_of_date")).first()[0]
start = min(str(first), f"{int(as_of_date[:4]) - 1}-01-01")
overwrite(g.build_dim_date(spark, start, f"{int(as_of_date[:4]) + 1}-12-31"), f"{G}.dim_date")
overwrite(g.build_dim_branch(spark.table(f"{S}.ref_branches")), f"{G}.dim_branch")
dim_product = g.build_dim_product(spark.table(f"{S}.ref_products"))
overwrite(dim_product, f"{G}.dim_product")
overwrite(g.build_dim_customer(spark.table(f"{S}.core_customers")), f"{G}.dim_customer")
dim_customer = spark.table(f"{G}.dim_customer")

# COMMAND ----------

# Fact
loans_day = spark.table(f"{S}.loan_snapshots").filter(F.col("as_of_date") == day)
merge_upsert(g.build_fact_loan_daily(loans_day, dim_customer), f"{G}.loan_fact_daily",
             g.GOLD_KEYS["loan_fact_daily"])

txns_day = spark.table(f"{S}.core_transactions").filter(F.col("_as_of_date") == day)
merge_upsert(g.build_fact_transaction(txns_day, spark.table(f"{S}.core_accounts"), dim_customer),
             f"{G}.core_fact_transaction", g.GOLD_KEYS["core_fact_transaction"], insert_only=True)

# COMMAND ----------

# Mart
fact_loan = spark.table(f"{G}.loan_fact_daily")
overdue_day = g.build_mart_loan_overdue_daily(fact_loan.filter(F.col("as_of_date") == day),
                                              dim_product)
overwrite_date(overdue_day, f"{G}.loan_mart_overdue_daily", "as_of_date", as_of_date)
overwrite(g.build_mart_npl_daily(spark.table(f"{G}.loan_mart_overdue_daily")),
          f"{G}.loan_mart_npl_daily")

# Snapshot TK đúng ngày: TK bị cách ly (DQ lỗi) hôm nay vẫn còn dòng cũ trong silver.core_accounts
# -> phải lọc theo _as_of_date, không thì Gold cộng thừa so với file bang1247.
acc_day = spark.table(f"{S}.core_accounts").filter(F.col("_as_of_date") == day)
overwrite_date(g.build_mart_deposit_daily(acc_day, as_of_date),
               f"{G}.dep_mart_daily", "as_of_date", as_of_date)
overwrite(g.build_mart_customer_360(dim_customer, fact_loan, acc_day,
                                    spark.table(f"{G}.core_fact_transaction"), as_of_date),
          f"{G}.cust_mart_360")

# COMMAND ----------

# Thẻ tín dụng
cards_day = spark.table(f"{S}.card_master").filter(F.col("_as_of_date") == day)
ctx_day = spark.table(f"{S}.card_transactions_hist").filter(F.col("_as_of_date") == day)
stm_day = spark.table(f"{S}.card_statements_hist").filter(F.col("stnd_dt") == day)
card_daily = gc.build_mart_card_kpi_daily(cards_day, ctx_day, stm_day, as_of_date)
overwrite_date(card_daily, f"{G}.card_mart_kpi_daily", "as_of_date", as_of_date)
overwrite_date(gc.build_mart_card_spend_mix(ctx_day, as_of_date), f"{G}.card_mart_spend_mix",
               "as_of_date", as_of_date)
overwrite(gc.build_mart_card_kpi_monthly(spark.table(f"{G}.card_mart_kpi_daily"),
                                         spark.table(f"{S}.ref_targets")),
          f"{G}.card_mart_kpi_monthly")

# COMMAND ----------

# Tiền gửi
acc = acc_day
fx_day = spark.table(f"{S}.ref_fx_rates").filter(F.col("rate_date") == day)
trend_hist = (spark.table(f"{G}.dep_mart_trend")
              if spark.catalog.tableExists(f"{G}.dep_mart_trend") else None)
overwrite_date(gd.build_mart_deposit_trend(acc, dim_product, fx_day, spark.table(f"{S}.ref_branches"),
                                           as_of_date, history=trend_hist),
               f"{G}.dep_mart_trend", "as_of_date", as_of_date)
overwrite_date(gd.build_mart_deposit_by_tenor(acc, dim_product, fx_day, as_of_date),
               f"{G}.dep_mart_by_tenor", "as_of_date", as_of_date)
overwrite_date(gd.build_mart_casa_customer(acc, dim_product, dim_customer.filter("is_current"),
                                           as_of_date),
               f"{G}.dep_mart_casa_customer", "as_of_date", as_of_date)
overwrite(gd.build_mart_deposit_new_accounts(spark.table(f"{S}.core_accounts")), f"{G}.dep_mart_new_accounts")

# COMMAND ----------

# Đối soát Gold vs file tổng hợp của hệ thống nguồn
recon_card = gc.reconcile(gc.card_kpi_metrics(card_daily),
                          spark.table(f"{S}.card_kpi_source").filter(F.col("trx_date") == day))
overwrite_date(recon_card, f"{catalog}.ops.recon_card_kpi", "as_of_date", as_of_date)

groups = {r.prodcode: r.product_name for r in
          dim_product.filter("product_family = 'DEPOSIT'").collect()}
recon_dep = gc.deposit_reconciliation(
    spark.table(f"{G}.dep_mart_daily").filter(F.col("as_of_date") == day),
    spark.table(f"{S}.dep_summary_source").filter(F.col("stnd_dt") == day),
    spark.table(f"{S}.ref_fx_rates").filter(F.col("rate_date") == day), groups)
overwrite_date(recon_dep, f"{catalog}.ops.recon_deposit", "as_of_date", as_of_date)

display(spark.table(f"{catalog}.ops.recon_card_kpi").filter(F.col("as_of_date") == day)
        .groupBy("status").count())
bad_card = recon_card.filter("status NOT IN ('MATCH', 'WITHIN_TOLERANCE')").count()
bad_dep = recon_dep.filter("status <> 'MATCH'").count()

# COMMAND ----------

# Kiểm tra: Gold khớp Silver
silver_bal = loans_day.agg(F.sum(F.coalesce("ducuoiqd", "ducuoi"))).first()[0]
gold_bal = spark.table(f"{G}.loan_mart_overdue_daily").filter(F.col("as_of_date") == day) \
    .agg(F.sum("portfolio_balance")).first()[0]
print(f"Dư nợ silver={silver_bal:,.0f} gold={gold_bal:,.0f}")
assert silver_bal == gold_bal, "Đối soát dư nợ Silver - Gold lệch"
display(spark.table(f"{G}.loan_mart_npl_daily").orderBy(F.desc("as_of_date")).limit(10))
assert bad_card == 0, f"Đối soát KPI thẻ lệch {bad_card} chỉ tiêu (xem ops.recon_card_kpi)"
assert bad_dep == 0, f"Đối soát tiền gửi lệch {bad_dep} dòng (xem ops.recon_deposit)"
