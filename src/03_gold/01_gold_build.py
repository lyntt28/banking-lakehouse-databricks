# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: star schema + mart cho dashboard
# MAGIC - Dimension: `dim_date`, `dim_branch`, `dim_product`, `dim_customer` (SCD2).
# MAGIC - Fact: `loan_fact_daily` (ngày x khoản vay), `core_fact_transaction`.
# MAGIC - Mart: `loan_mart_overdue_daily`, `loan_mart_npl_daily` (so D-1 / M-1 / Y-1),
# MAGIC   `dep_mart_daily`, `cust_mart_360`. Xem `docs/gold_dashboards.md`.
# MAGIC - Thẻ: `card_mart_kpi_daily`, `card_mart_spend_mix`, `card_mart_kpi_monthly` (MoM/YoY, % chỉ tiêu).
# MAGIC - Tiền gửi: `dep_mart_trend` (D-1/M-1/Y-1), `dep_mart_by_tenor`,
# MAGIC   `dep_mart_casa_customer`, `dep_mart_new_accounts` (K-Study).
# MAGIC - Đối soát với file tổng hợp nguồn -> `ops.recon_card_kpi`, `ops.recon_deposit`;
# MAGIC   có MISMATCH -> job lỗi.
# MAGIC - Chạy lại cùng ngày an toàn: fact MERGE theo khóa, mart ghi đè đúng ngày.
# MAGIC - **Gate trước khi publish**: mart dùng để đối soát được ghi vào `gold_stage` trước,
# MAGIC   đối soát chạy trên bản stage; lệch -> dừng, bảng Gold (dashboard đọc) giữ nguyên bản cũ.
# MAGIC   Đạt -> mới ghi vào Gold và thêm dòng vào `ops.published_batches`.
# MAGIC - `as_of_date` trống = mọi ngày Silver SUCCESS mà Gold chưa SUCCESS (`ops.pipeline_runs`).

# COMMAND ----------

dbutils.widgets.text("catalog", "bank_dev")
dbutils.widgets.text("as_of_date", "")
catalog = dbutils.widgets.get("catalog")
as_of_date = dbutils.widgets.get("as_of_date").strip()

# COMMAND ----------

import json
import os
import sys
import uuid
from pathlib import Path

src_dir = str(Path(os.getcwd()).parents[0])
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from pyspark.sql import functions as F  # noqa: E402

from common import gold as g  # noqa: E402
from common import gold_cards as gc  # noqa: E402
from common import gold_deposit as gd  # noqa: E402
from common.batch import PUBLISHED_BATCHES_DDL, record_run, watermark  # noqa: E402
from common.dates import check_date  # noqa: E402
from common.delta_io import merge_upsert, overwrite, overwrite_date  # noqa: E402
from common.governance import assert_pipeline_identity  # noqa: E402

S = f"{catalog}.silver"
G = f"{catalog}.gold"
STG = f"{catalog}.gold_stage"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {STG}")
spark.sql(PUBLISHED_BATCHES_DDL.format(catalog=catalog))
assert_pipeline_identity(spark, catalog)  # governance bật -> phải thấy PII

if as_of_date:
    days = [check_date(as_of_date)]
else:
    gold_wm = watermark(spark, catalog, "gold")
    cond = f"AND as_of_date > DATE'{check_date(gold_wm)}'" if gold_wm else ""
    days = [str(r[0]) for r in spark.sql(
        f"SELECT DISTINCT as_of_date FROM {catalog}.ops.pipeline_runs "
        f"WHERE layer = 'silver' AND status = 'SUCCESS' {cond} ORDER BY as_of_date").collect()]
print(f"catalog={catalog} pending={days}")

# COMMAND ----------


def stage(df, name: str):
    """Ghi bản ứng viên vào gold_stage rồi đọc lại (đóng băng kết quả, thay cho cache)."""
    overwrite(df, f"{STG}.{name}")
    return spark.table(f"{STG}.{name}")


def build_and_publish(as_of_date: str, run_id: str) -> None:
    day = F.lit(as_of_date).cast("date")

    # ---------- 1) Tính toán (chưa ghi Gold)
    first = spark.table(f"{S}.loan_snapshots").agg(F.min("as_of_date")).first()[0]
    start = min(str(first), f"{int(as_of_date[:4]) - 1}-01-01")
    dim_date = g.build_dim_date(spark, start, f"{int(as_of_date[:4]) + 1}-12-31")
    dim_branch = g.build_dim_branch(spark.table(f"{S}.ref_branches"))
    dim_product = stage(g.build_dim_product(spark.table(f"{S}.ref_products")), "dim_product")
    dim_customer = stage(g.build_dim_customer(spark.table(f"{S}.core_customers")), "dim_customer")

    loans_day = spark.table(f"{S}.loan_snapshots").filter(F.col("as_of_date") == day)
    fact_loan_day = stage(g.build_fact_loan_daily(loans_day, dim_customer), "loan_fact_daily")
    overdue_day = stage(g.build_mart_loan_overdue_daily(fact_loan_day, dim_product),
                        "loan_mart_overdue_daily")

    # Snapshot TK đúng ngày: TK bị cách ly hôm nay vẫn còn dòng cũ trong silver.core_accounts
    acc_day = spark.table(f"{S}.core_accounts").filter(F.col("_as_of_date") == day)
    dep_daily = stage(g.build_mart_deposit_daily(acc_day, as_of_date), "dep_mart_daily")

    cards_day = spark.table(f"{S}.card_master").filter(F.col("_as_of_date") == day)
    ctx_day = spark.table(f"{S}.card_transactions_hist").filter(F.col("_as_of_date") == day)
    stm_day = spark.table(f"{S}.card_statements_hist").filter(F.col("stnd_dt") == day)
    card_daily = stage(gc.build_mart_card_kpi_daily(cards_day, ctx_day, stm_day, as_of_date),
                       "card_mart_kpi_daily")

    # ---------- 2) Gate: đối soát trên bản stage
    recon_card = gc.reconcile(gc.card_kpi_metrics(card_daily),
                              spark.table(f"{S}.card_kpi_source").filter(F.col("trx_date") == day))
    overwrite_date(recon_card, f"{catalog}.ops.recon_card_kpi", "as_of_date", as_of_date)
    groups = {r.prodcode: r.product_name for r in
              dim_product.filter("product_family = 'DEPOSIT'").collect()}
    recon_dep = gc.deposit_reconciliation(
        dep_daily,
        spark.table(f"{S}.dep_summary_source").filter(F.col("stnd_dt") == day),
        spark.table(f"{S}.ref_fx_rates").filter(F.col("rate_date") == day), groups)
    overwrite_date(recon_dep, f"{catalog}.ops.recon_deposit", "as_of_date", as_of_date)

    bad_card = spark.table(f"{catalog}.ops.recon_card_kpi").filter(F.col("as_of_date") == day) \
        .filter("status NOT IN ('MATCH', 'WITHIN_TOLERANCE')").count()
    bad_dep = spark.table(f"{catalog}.ops.recon_deposit").filter(F.col("as_of_date") == day) \
        .filter("status <> 'MATCH'").count()
    silver_bal = loans_day.agg(F.sum(F.coalesce("ducuoiqd", "ducuoi"))).first()[0] or 0
    gold_bal = overdue_day.agg(F.sum("portfolio_balance")).first()[0] or 0
    checks = {"silver_balance": float(silver_bal), "gold_balance": float(gold_bal),
              "recon_card_bad": bad_card, "recon_deposit_bad": bad_dep}
    print(as_of_date, checks)
    errors = []
    if silver_bal != gold_bal:
        errors.append("Đối soát dư nợ Silver - Gold lệch")
    if bad_card:
        errors.append(f"Đối soát KPI thẻ lệch {bad_card} chỉ tiêu (ops.recon_card_kpi)")
    if bad_dep:
        errors.append(f"Đối soát tiền gửi lệch {bad_dep} dòng (ops.recon_deposit)")
    if errors:
        # Gold production CHƯA bị ghi -> dashboard vẫn thấy batch hợp lệ gần nhất
        raise RuntimeError(f"Gate chặn publish {as_of_date}: {errors}")

    # ---------- 3) Publish (chỉ tới đây khi gate đạt)
    overwrite(dim_date, f"{G}.dim_date")
    overwrite(dim_branch, f"{G}.dim_branch")
    overwrite(dim_product, f"{G}.dim_product")
    overwrite(dim_customer, f"{G}.dim_customer")
    dim_customer_g = spark.table(f"{G}.dim_customer")

    # Snapshot ngày: ghi đè cả ngày (không MERGE) -> khoản vay bị nguồn rút khỏi file sửa lại
    # cũng biến mất khỏi fact, không còn dòng "ma".
    overwrite_date(fact_loan_day, f"{G}.loan_fact_daily", "as_of_date", as_of_date)
    txns_day = spark.table(f"{S}.core_transactions").filter(F.col("_as_of_date") == day)
    merge_upsert(g.build_fact_transaction(txns_day, spark.table(f"{S}.core_accounts"),
                                          dim_customer_g),
                 f"{G}.core_fact_transaction", g.GOLD_KEYS["core_fact_transaction"],
                 insert_only=True)

    overwrite_date(overdue_day, f"{G}.loan_mart_overdue_daily", "as_of_date", as_of_date)
    overwrite(g.build_mart_npl_daily(spark.table(f"{G}.loan_mart_overdue_daily")),
              f"{G}.loan_mart_npl_daily")
    overwrite_date(dep_daily, f"{G}.dep_mart_daily", "as_of_date", as_of_date)
    overwrite(g.build_mart_customer_360(dim_customer_g, spark.table(f"{G}.loan_fact_daily"),
                                        acc_day, spark.table(f"{G}.core_fact_transaction"),
                                        as_of_date),
              f"{G}.cust_mart_360")

    overwrite_date(card_daily, f"{G}.card_mart_kpi_daily", "as_of_date", as_of_date)
    overwrite_date(gc.build_mart_card_spend_mix(ctx_day, as_of_date), f"{G}.card_mart_spend_mix",
                   "as_of_date", as_of_date)
    overwrite(gc.build_mart_card_kpi_monthly(spark.table(f"{G}.card_mart_kpi_daily"),
                                             spark.table(f"{S}.ref_targets")),
              f"{G}.card_mart_kpi_monthly")

    fx_day = spark.table(f"{S}.ref_fx_rates").filter(F.col("rate_date") == day)
    trend_hist = (spark.table(f"{G}.dep_mart_trend")
                  if spark.catalog.tableExists(f"{G}.dep_mart_trend") else None)
    overwrite_date(gd.build_mart_deposit_trend(acc_day, dim_product, fx_day,
                                               spark.table(f"{S}.ref_branches"),
                                               as_of_date, history=trend_hist),
                   f"{G}.dep_mart_trend", "as_of_date", as_of_date)
    overwrite_date(gd.build_mart_deposit_by_tenor(acc_day, dim_product, fx_day, as_of_date),
                   f"{G}.dep_mart_by_tenor", "as_of_date", as_of_date)
    overwrite_date(gd.build_mart_casa_customer(acc_day, dim_product,
                                               dim_customer_g.filter("is_current"), as_of_date),
                   f"{G}.dep_mart_casa_customer", "as_of_date", as_of_date)
    overwrite(gd.build_mart_deposit_new_accounts(spark.table(f"{S}.core_accounts")),
              f"{G}.dep_mart_new_accounts")

    spark.createDataFrame([(as_of_date, run_id, json.dumps(checks))],
                          "as_of_date string, run_id string, checks string") \
        .select(F.col("as_of_date").cast("date"), "run_id",
                F.current_timestamp().alias("published_at"), "checks") \
        .write.mode("append").saveAsTable(f"{catalog}.ops.published_batches")


# COMMAND ----------

if not days:
    print("Không có ngày Silver mới cần dựng Gold")
for d in days:
    run_id = str(uuid.uuid4())
    record_run(spark, catalog, d, "gold", run_id, "RUNNING")
    try:
        build_and_publish(d, run_id)
    except Exception as e:
        record_run(spark, catalog, d, "gold", run_id, "FAILED", f"{type(e).__name__}: {e}")
        raise
    record_run(spark, catalog, d, "gold", run_id, "SUCCESS")
    print("gold published", d)

if days:
    display(spark.table(f"{G}.loan_mart_npl_daily").orderBy(F.desc("as_of_date")).limit(10))
