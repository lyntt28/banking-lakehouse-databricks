"""Pipeline Gold: star schema + mart, có gate đối soát TRƯỚC khi publish.

Mỗi ngày dữ liệu:
    1. build  - tính dimension + mart dùng để đối soát, ghi vào `gold_stage` (bản ứng viên)
    2. gate   - đối soát trên bản stage: dư nợ Silver = Gold, KPI thẻ vs file nguồn,
                tiền gửi vs bang1247. Lệch -> GateFailed, bảng `gold` KHÔNG bị ghi
                (dashboard vẫn thấy batch hợp lệ gần nhất)
    3. publish - ghi vào `gold` (idempotent: replaceWhere / MERGE / overwrite) rồi thêm dòng
                vào `ops.published_batches`. Lỗi giữa chừng (bảng thứ N/17) -> tự RESTORE mọi bảng
                về version trước publish (rollback) -> Gold không bị trộn bảng mới / cũ.
                Dashboard chỉ đọc tới ngày trong `published_batches` (dòng cuối cùng được ghi).

Ngày cần dựng = ngày Silver SUCCESS mà Gold chưa SUCCESS (hoặc đúng `as_of_date`).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import gold as g
from common import gold_cards as gc
from common import gold_deposit as gd
from common.batch import (
    ensure_ops_tables,
    record_run,
    record_versions,
    rollback_publish,
    watermark,
)
from common.dates import check_date
from common.delta_io import merge_upsert, overwrite, overwrite_date
from common.names import fq
from common.ops_log import StepRecorder, log


@dataclass(frozen=True)
class GoldConfig:
    catalog: str
    as_of_date: str = ""

    def t(self, schema: str, table: str) -> str:
        return fq(self.catalog, schema, table)


# Bảng Gold ghi ở bước publish (lưu version trước khi ghi để RESTORE nếu lỗi giữa chừng)
PUBLISHED_TABLES = ["dim_date", "dim_branch", "dim_product", "dim_customer", "loan_fact_daily",
                    "core_fact_transaction", "loan_mart_overdue_daily", "loan_mart_npl_daily",
                    "dep_mart_daily", "cust_mart_360", "card_mart_kpi_daily",
                    "card_mart_spend_mix", "card_mart_kpi_monthly", "dep_mart_trend",
                    "dep_mart_by_tenor", "dep_mart_casa_customer", "dep_mart_new_accounts"]


class GateFailed(RuntimeError):
    pass


class NotReproducible(RuntimeError):
    """Silver đã tiến qua ngày này: snapshot MERGE (TK, thẻ) không còn trạng thái của ngày đó."""


def assert_reproducible(day: str, silver_watermark: str | None) -> None:
    if silver_watermark and day < silver_watermark:
        raise NotReproducible(
            f"Không dựng Gold {day}: Silver đã ở {silver_watermark}. Dựng lại ngày cũ = replay "
            "Silver + Gold từ ngày đó (dev/test: 05_backfill reset=true), xem docs/runbook.md")


def gate_errors(checks: dict) -> list[str]:
    """Quyết định publish (thuần Python, test được không cần Spark)."""
    errors = []
    if checks["silver_balance"] != checks["gold_balance"]:
        errors.append(f"Dư nợ Silver ({checks['silver_balance']:,.0f}) != "
                      f"Gold ({checks['gold_balance']:,.0f})")
    if checks["recon_card_bad"]:
        errors.append(f"Đối soát KPI thẻ lệch {checks['recon_card_bad']} chỉ tiêu "
                      "(ops.recon_card_kpi)")
    if checks["recon_deposit_bad"]:
        errors.append(f"Đối soát tiền gửi lệch {checks['recon_deposit_bad']} dòng "
                      "(ops.recon_deposit)")
    return errors


def pending_days(spark: SparkSession, cfg: GoldConfig) -> list[str]:
    if cfg.as_of_date:
        return [check_date(cfg.as_of_date)]
    wm = watermark(spark, cfg.catalog, "gold")
    cond = f"AND as_of_date > DATE'{check_date(wm)}'" if wm else ""
    return [str(r[0]) for r in spark.sql(
        f"SELECT DISTINCT as_of_date FROM {cfg.t('ops', 'pipeline_runs')} "
        f"WHERE layer = 'silver' AND status = 'SUCCESS' {cond} ORDER BY as_of_date").collect()]


def publish_batch_snapshot(df, target: str, day_s: str) -> None:
    """Mart tính lại TOÀN BỘ mỗi ngày (tháng, luỹ kế) nhưng dashboard đọc: lưu 1 snapshot / batch
    (cột as_of_date = ngày batch) thay vì ghi đè cả bảng -> trong lúc publish đang chạy, view
    (lọc as_of_date = batch đã publish) vẫn đọc bản cũ, không thấy bản mới dở dang.
    Bảng cũ chưa có cột as_of_date (trước nâng cấp): ghi đè 1 lần để đổi schema."""
    spark = df.sparkSession
    df = df.withColumn("as_of_date", F.lit(day_s).cast("date"))
    if spark.catalog.tableExists(target) and \
            "as_of_date" not in spark.table(target).columns:
        overwrite(df, target)
        return
    overwrite_date(df, target, "as_of_date", day_s)


def build_and_publish(spark: SparkSession, cfg: GoldConfig, day_s: str,
                      rec: StepRecorder) -> dict:
    S = lambda t: cfg.t("silver", t)  # noqa: E731
    G = lambda t: cfg.t("gold", t)  # noqa: E731
    ops = lambda t: cfg.t("ops", t)  # noqa: E731

    def stage(df, name: str):
        """Ghi bản ứng viên vào gold_stage rồi đọc lại (đóng băng kết quả, thay cho cache)."""
        overwrite(df, cfg.t("gold_stage", name))
        return spark.table(cfg.t("gold_stage", name))

    day = F.lit(day_s).cast("date")

    # ---------- 1) build (chưa ghi gold)
    with rec.step("gold:build_stage") as s:
        first = spark.table(S("loan_snapshots")).agg(F.min("as_of_date")).first()[0]
        start = min(str(first), f"{int(day_s[:4]) - 1}-01-01")
        dim_date = g.build_dim_date(spark, start, f"{int(day_s[:4]) + 1}-12-31")
        dim_branch = g.build_dim_branch(spark.table(S("ref_branches")))
        dim_product = stage(g.build_dim_product(spark.table(S("ref_products"))), "dim_product")
        dim_customer = stage(g.build_dim_customer(spark.table(S("core_customers"))),
                             "dim_customer")

        loans_day = spark.table(S("loan_snapshots")).filter(F.col("as_of_date") == day)
        fact_loan_day = stage(g.build_fact_loan_daily(loans_day, dim_customer),
                              "loan_fact_daily")
        overdue_day = stage(g.build_mart_loan_overdue_daily(fact_loan_day, dim_product),
                            "loan_mart_overdue_daily")
        # snapshot TK đúng ngày: TK bị cách ly hôm nay vẫn còn dòng cũ trong silver
        acc_day = spark.table(S("core_accounts")).filter(F.col("_as_of_date") == day)
        dep_daily = stage(g.build_mart_deposit_daily(acc_day, day_s), "dep_mart_daily")
        cards_day = spark.table(S("card_master")).filter(F.col("_as_of_date") == day)
        ctx_day = spark.table(S("card_transactions_hist")).filter(F.col("_as_of_date") == day)
        stm_day = spark.table(S("card_statements_hist")).filter(F.col("stnd_dt") == day)
        card_daily = stage(gc.build_mart_card_kpi_daily(cards_day, ctx_day, stm_day, day_s),
                           "card_mart_kpi_daily")
        s.rows_in = loans_day.count()
        s.rows_out = fact_loan_day.count()

    # ---------- 2) gate
    with rec.step("gold:gate") as s:
        recon_card = gc.reconcile(gc.card_kpi_metrics(card_daily),
                                  spark.table(S("card_kpi_source"))
                                  .filter(F.col("trx_date") == day))
        overwrite_date(recon_card, ops("recon_card_kpi"), "as_of_date", day_s)
        groups = {r.prodcode: r.product_name for r in
                  dim_product.filter("product_family = 'DEPOSIT'").collect()}
        recon_dep = gc.deposit_reconciliation(
            dep_daily, spark.table(S("dep_summary_source")).filter(F.col("stnd_dt") == day),
            spark.table(S("ref_fx_rates")).filter(F.col("rate_date") == day), groups)
        overwrite_date(recon_dep, ops("recon_deposit"), "as_of_date", day_s)

        checks = {
            "silver_balance": float(loans_day.agg(
                F.sum(F.coalesce("ducuoiqd", "ducuoi"))).first()[0] or 0),
            "gold_balance": float(overdue_day.agg(F.sum("portfolio_balance")).first()[0] or 0),
            "recon_card_bad": spark.table(ops("recon_card_kpi"))
            .filter((F.col("as_of_date") == day)
                    & ~F.col("status").isin("MATCH", "WITHIN_TOLERANCE")).count(),
            "recon_deposit_bad": spark.table(ops("recon_deposit"))
            .filter((F.col("as_of_date") == day) & (F.col("status") != "MATCH")).count(),
        }
        log("gold_gate", as_of_date=day_s, run_id=rec.run_id, **checks)
        errors = gate_errors(checks)
        if errors:
            raise GateFailed(f"Gate chặn publish {day_s}: {errors}")

    # ---------- 3) publish
    with rec.step("gold:publish") as s:
        record_versions(spark, cfg.catalog, rec.run_id, day_s, "gold",
                        [G(t) for t in PUBLISHED_TABLES])
        overwrite(dim_date, G("dim_date"))
        overwrite(dim_branch, G("dim_branch"))
        overwrite(dim_product, G("dim_product"))
        overwrite(dim_customer, G("dim_customer"))
        dim_customer_g = spark.table(G("dim_customer"))

        # snapshot ngày: ghi đè cả ngày -> khoản vay bị nguồn rút khỏi file sửa cũng biến mất
        overwrite_date(fact_loan_day, G("loan_fact_daily"), "as_of_date", day_s)
        txns_day = spark.table(S("core_transactions")).filter(F.col("_as_of_date") == day)
        merge_upsert(g.build_fact_transaction(txns_day, spark.table(S("core_accounts")),
                                              dim_customer_g),
                     G("core_fact_transaction"), g.GOLD_KEYS["core_fact_transaction"],
                     insert_only=True)

        overwrite_date(overdue_day, G("loan_mart_overdue_daily"), "as_of_date", day_s)
        overwrite(g.build_mart_npl_daily(spark.table(G("loan_mart_overdue_daily"))),
                  G("loan_mart_npl_daily"))
        overwrite_date(dep_daily, G("dep_mart_daily"), "as_of_date", day_s)
        overwrite(g.build_mart_customer_360(dim_customer_g, spark.table(G("loan_fact_daily")),
                                            acc_day, spark.table(G("core_fact_transaction")),
                                            day_s), G("cust_mart_360"))

        overwrite_date(card_daily, G("card_mart_kpi_daily"), "as_of_date", day_s)
        overwrite_date(gc.build_mart_card_spend_mix(ctx_day, day_s), G("card_mart_spend_mix"),
                       "as_of_date", day_s)
        publish_batch_snapshot(gc.build_mart_card_kpi_monthly(
            spark.table(G("card_mart_kpi_daily")), spark.table(S("ref_targets"))),
            G("card_mart_kpi_monthly"), day_s)

        fx_day = spark.table(S("ref_fx_rates")).filter(F.col("rate_date") == day)
        trend_hist = (spark.table(G("dep_mart_trend"))
                      if spark.catalog.tableExists(G("dep_mart_trend")) else None)
        overwrite_date(gd.build_mart_deposit_trend(acc_day, dim_product, fx_day,
                                                   spark.table(S("ref_branches")), day_s,
                                                   history=trend_hist),
                       G("dep_mart_trend"), "as_of_date", day_s)
        overwrite_date(gd.build_mart_deposit_by_tenor(acc_day, dim_product, fx_day, day_s),
                       G("dep_mart_by_tenor"), "as_of_date", day_s)
        overwrite_date(gd.build_mart_casa_customer(acc_day, dim_product,
                                                   dim_customer_g.filter("is_current"), day_s),
                       G("dep_mart_casa_customer"), "as_of_date", day_s)
        publish_batch_snapshot(gd.build_mart_deposit_new_accounts(spark.table(S("core_accounts"))),
                               G("dep_mart_new_accounts"), day_s)

        (spark.createDataFrame([(day_s, rec.run_id, json.dumps(checks))],
                               "as_of_date string, run_id string, checks string")
         .select(F.col("as_of_date").cast("date"), "run_id",
                 F.current_timestamp().alias("published_at"), "checks")
         .write.format("delta").mode("append").saveAsTable(ops("published_batches")))
        s.rows_out = fact_loan_day.count()
    return checks


def run(spark: SparkSession, cfg: GoldConfig) -> list[str]:
    ensure_ops_tables(spark, cfg.catalog)
    for schema in ("gold", "gold_stage"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq(cfg.catalog, schema)}")
    days = pending_days(spark, cfg)
    log("gold_pending", catalog=cfg.catalog, days=days)
    done = []
    silver_wm = watermark(spark, cfg.catalog, "silver")
    for day in days:
        assert_reproducible(day, silver_wm)
        rec = StepRecorder(str(uuid.uuid4()), day, "gold")
        record_run(spark, cfg.catalog, day, "gold", rec.run_id, "RUNNING")
        try:
            build_and_publish(spark, cfg, day, rec)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            # lỗi trong bước publish: đã có version trước publish của run này -> rollback
            try:
                undone = rollback_publish(spark, cfg.catalog, rec.run_id)
                if undone:
                    log("gold_rolled_back", as_of_date=day, run_id=rec.run_id, statements=undone)
                    msg += f" | rollback {len(undone)} bảng về version trước publish"
            except Exception as rb:  # rollback cũng lỗi: phải xử lý tay (runbook mục 5)
                msg += f" | ROLLBACK FAILED {type(rb).__name__}: {rb}"
                log("gold_rollback_failed", as_of_date=day, run_id=rec.run_id, error=str(rb))
            rec.flush(spark, cfg.t("ops", "task_runs"))
            record_run(spark, cfg.catalog, day, "gold", rec.run_id, "FAILED", msg)
            log("gold_failed", as_of_date=day, run_id=rec.run_id, error=str(e))
            raise
        rec.flush(spark, cfg.t("ops", "task_runs"))
        record_run(spark, cfg.catalog, day, "gold", rec.run_id, "SUCCESS")
        log("gold_published", as_of_date=day, run_id=rec.run_id)
        done.append(day)
    return done
