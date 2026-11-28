"""Pipeline Gold: star schema + mart, có gate đối soát TRƯỚC khi publish.

Mỗi ngày dữ liệu:
    1. build  - tính dimension + mart dùng để đối soát, ghi vào `gold_stage` (bản ứng viên)
    2. gate   - đối soát trên bản stage: dư nợ Silver = Gold, KPI thẻ vs file nguồn,
                tiền gửi vs bang1247. Lệch -> GateFailed, bảng `gold` KHÔNG bị ghi
                (dashboard vẫn thấy batch hợp lệ gần nhất)
    3. publish - ghi THÊM dòng của batch (batch_id = run_id) vào `gold`, vô hình với người đọc
                (`gold_serving` chỉ thấy batch đã kích hoạt, xem common/serving.py). Kích hoạt =
                1 dòng append vào `ops.published_batches` (1 commit = nguyên tử).
                - Lỗi TRƯỚC kích hoạt: RESTORE các bảng đã ghi (dọn dòng batch lỗi); người đọc
                  vốn không thấy batch này.
                - Lỗi SAU kích hoạt: KHÔNG rollback (con trỏ đã trỏ batch mới, RESTORE Gold sẽ
                  làm view trỏ vào batch không còn dữ liệu). Run báo FAILED để cảnh báo.
    4. sau kích hoạt - dọn dòng batch cũ (lỗi chỉ ghi log), tạo + kiểm tra view gold_serving
                (lỗi -> FAILED: người đọc có thể không thấy cột / bảng mới).

Ngày cần dựng = ngày Silver SUCCESS mà Gold chưa SUCCESS (hoặc đúng `as_of_date`).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import gold as g
from common import gold_cards as gc
from common import gold_deposit as gd
from common import serving as sv
from common.batch import (
    ensure_ops_tables,
    record_run,
    record_versions,
    rollback_publish,
    watermark,
)
from common.dates import check_date
from common.delta_io import overwrite, overwrite_date
from common.lock import LockLost, ensure_held, writer_lock
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
                    "dep_mart_by_tenor", "dep_mart_casa_customer", "dep_mart_new_accounts",
                    "card_mart_overdue_daily"]


class GateFailed(RuntimeError):
    pass


class ServingViewsInvalid(RuntimeError):
    """Batch đã kích hoạt nhưng view gold_serving thiếu / không truy vấn được."""


@dataclass
class PublishState:
    """Trạng thái publish, để `run` biết lỗi xảy ra trước hay sau khi kích hoạt batch."""
    activated: bool = False


def failure_action(activated: bool) -> str:
    """Lỗi trước kích hoạt -> rollback; sau kích hoạt -> giữ nguyên (thuần Python, test được)."""
    return "keep_active" if activated else "rollback"


def serving_view_errors(expected: list[str], created: list[str],
                        broken: dict[str, str]) -> list[str]:
    errors = [f"thiếu view gold_serving.{t}" for t in expected if t not in created]
    errors += [f"view gold_serving.{t} lỗi: {e}" for t, e in broken.items()]
    return errors


class NotReproducible(RuntimeError):
    """Silver đã tiến qua ngày này: snapshot MERGE (TK, thẻ) không còn trạng thái của ngày đó."""


def assert_reproducible(day: str, silver_watermark: str | None) -> None:
    if silver_watermark and day < silver_watermark:
        raise NotReproducible(
            f"Không dựng Gold {day}: Silver đã ở {silver_watermark}. Dựng lại ngày cũ = replay "
            "Silver + Gold từ ngày đó (dev/test: 05_backfill reset=true), xem docs/runbook.md")


def as_money(v) -> Decimal:
    """Tổng tiền từ Spark (Decimal / None) -> Decimal. Không đi qua float."""
    if v is None:
        return Decimal(0)
    if isinstance(v, float):
        raise TypeError("Số tiền phải là DECIMAL, không phải float/double (mất độ chính xác)")
    return Decimal(v)


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
    if checks.get("recon_overdue_bad"):
        errors.append("Đối soát Overdue Silver-Gold lệch: "
                      + ", ".join(checks["recon_overdue_bad"]))
    return errors


def pending_days(spark: SparkSession, cfg: GoldConfig) -> list[str]:
    if cfg.as_of_date:
        return [check_date(cfg.as_of_date)]
    wm = watermark(spark, cfg.catalog, "gold")
    cond = f"AND as_of_date > DATE'{check_date(wm)}'" if wm else ""
    return [str(r[0]) for r in spark.sql(
        f"SELECT DISTINCT as_of_date FROM {cfg.t('ops', 'pipeline_runs')} "
        f"WHERE layer = 'silver' AND status = 'SUCCESS' {cond} ORDER BY as_of_date").collect()]


def build_and_publish(spark: SparkSession, cfg: GoldConfig, day_s: str,
                      rec: StepRecorder, state: PublishState | None = None) -> dict:
    state = state if state is not None else PublishState()
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
        ovd_src = spark.table(S("card_overdue_source")).filter(F.col("report_dt") == day)
        card_overdue = stage(gc.build_mart_card_overdue_daily(ovd_src), "card_mart_overdue_daily")
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
            # Decimal (không float): tổng dư nợ VND có thể > 2^53, float sẽ mất đơn vị lẻ và
            # so sánh bằng nhau không còn chính xác
            "silver_balance": as_money(loans_day.agg(
                F.sum(F.coalesce("ducuoiqd", "ducuoi"))).first()[0]),
            "gold_balance": as_money(overdue_day.agg(F.sum("portfolio_balance")).first()[0]),
            "recon_card_bad": spark.table(ops("recon_card_kpi"))
            .filter((F.col("as_of_date") == day)
                    & ~F.col("status").isin("MATCH", "WITHIN_TOLERANCE")).count(),
            "recon_deposit_bad": spark.table(ops("recon_deposit"))
            .filter((F.col("as_of_date") == day) & (F.col("status") != "MATCH")).count(),
            # Overdue: Silver vs Gold khớp tuyệt đối 4 chỉ tiêu (Decimal / BIGINT)
            "recon_overdue_bad": [m for m, r in gc.overdue_reconciliation(
                ovd_src, card_overdue).items() if not r["match"]],
        }
        log("gold_gate", as_of_date=day_s, run_id=rec.run_id, **checks)
        errors = gate_errors(checks)
        if errors:
            raise GateFailed(f"Gate chặn publish {day_s}: {errors}")

    # ---------- 3) publish: ghi THÊM dòng của batch này (vô hình với người đọc), rồi kích hoạt
    # bằng 1 dòng vào ops.published_batches (1 commit = nguyên tử), rồi mới dọn batch cũ.
    batch = rec.run_id
    cat = cfg.catalog

    def put(df, table: str, key: list[str] | None = None) -> None:
        sv.write_batch(df, cat, table, batch, key, day_s=day_s)

    def cand(table: str):
        """Bảng như sau khi batch này được kích hoạt (cho mart đọc lại Gold)."""
        return sv.candidate(spark, cat, table, day_s, batch)

    with rec.step("gold:publish") as s:
        ensure_held(spark, cfg.catalog)   # mất khoá ghi -> dừng trước khi ghi Gold
        record_versions(spark, cfg.catalog, rec.run_id, day_s, "gold",
                        [G(t) for t in PUBLISHED_TABLES])
        put(dim_date, "dim_date")
        put(dim_branch, "dim_branch")
        put(dim_product, "dim_product")
        put(dim_customer, "dim_customer")
        dim_customer_g = dim_customer   # bản stage = nội dung batch này

        # snapshot ngày: batch mới thay cả ngày -> khoản vay bị nguồn rút khỏi file sửa biến mất
        put(fact_loan_day, "loan_fact_daily")
        txns_day = spark.table(S("core_transactions")).filter(F.col("_as_of_date") == day)
        put(g.build_fact_transaction(txns_day, spark.table(S("core_accounts")), dim_customer_g),
            "core_fact_transaction", g.GOLD_KEYS["core_fact_transaction"])

        put(overdue_day, "loan_mart_overdue_daily")
        put(g.build_mart_npl_daily(cand("loan_mart_overdue_daily")), "loan_mart_npl_daily")
        put(dep_daily, "dep_mart_daily")
        put(g.build_mart_customer_360(dim_customer_g, cand("loan_fact_daily"), acc_day,
                                      cand("core_fact_transaction"), day_s), "cust_mart_360")

        put(card_daily, "card_mart_kpi_daily")
        put(gc.build_mart_card_spend_mix(ctx_day, day_s), "card_mart_spend_mix")
        put(gc.build_mart_card_kpi_monthly(cand("card_mart_kpi_daily"),
                                           spark.table(S("ref_targets")))
            .withColumn("as_of_date", day), "card_mart_kpi_monthly")
        put(card_overdue, "card_mart_overdue_daily")

        fx_day = spark.table(S("ref_fx_rates")).filter(F.col("rate_date") == day)
        trend_hist = (cand("dep_mart_trend") if spark.catalog.tableExists(G("dep_mart_trend"))
                      else None)
        put(gd.build_mart_deposit_trend(acc_day, dim_product, fx_day,
                                        spark.table(S("ref_branches")), day_s,
                                        history=trend_hist,
                                        customers=dim_customer_g.filter("is_current")),
            "dep_mart_trend")
        put(gd.build_mart_deposit_by_tenor(acc_day, dim_product, fx_day, day_s),
            "dep_mart_by_tenor")
        put(gd.build_mart_casa_customer(acc_day, dim_product,
                                        dim_customer_g.filter("is_current"), day_s),
            "dep_mart_casa_customer")
        put(gd.build_mart_deposit_new_accounts(spark.table(S("core_accounts")))
            .withColumn("as_of_date", day), "dep_mart_new_accounts")

        s.rows_out = fact_loan_day.count()
        ensure_held(spark, cfg.catalog)   # mất khoá giữa chừng -> KHÔNG kích hoạt (rollback)
        # KÍCH HOẠT batch: từ commit này người đọc gold_serving thấy toàn bộ batch mới.
        # Là câu lệnh CUỐI của bước publish; mọi lỗi sau đây không được rollback Gold.
        (spark.createDataFrame([(day_s, rec.run_id, json.dumps(checks, default=str))],
                               "as_of_date string, run_id string, checks string")
         .select(F.col("as_of_date").cast("date"), "run_id",
                 F.current_timestamp().alias("published_at"), "checks")
         .write.format("delta").mode("append").saveAsTable(ops("published_batches")))
        state.activated = True
    return checks


def refresh_serving_views(spark: SparkSession, cfg: GoldConfig) -> None:
    """Tạo lại view gold_serving rồi kiểm tra từng view truy vấn được (LIMIT 0)."""
    expected = [t for t in PUBLISHED_TABLES if spark.catalog.tableExists(cfg.t("gold", t))]
    created = sv.create_serving_views(spark, cfg.catalog)
    broken = {}
    for t in created:
        try:
            spark.sql(f"SELECT * FROM {cfg.t('gold_serving', t)} LIMIT 0").collect()
        except Exception as e:  # noqa: BLE001
            broken[t] = f"{type(e).__name__}: {e}"
    errors = serving_view_errors(expected, created, broken)
    if errors:
        raise ServingViewsInvalid("; ".join(errors))


def run(spark: SparkSession, cfg: GoldConfig) -> list[str]:
    with writer_lock(spark, cfg.catalog, "gold"):
        return _run(spark, cfg)


def _run(spark: SparkSession, cfg: GoldConfig) -> list[str]:
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
        state = PublishState()
        try:
            build_and_publish(spark, cfg, day, rec, state)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            if isinstance(e, LockLost):
                # job khác đang giữ khoá: RESTORE lúc này có thể xoá dữ liệu của job đó.
                # Batch này chưa kích hoạt -> vô hình; lần publish sau của ngày này dọn nó.
                msg += " | mất khoá ghi, không rollback (batch chưa kích hoạt, vô hình)"
                log("gold_lock_lost", as_of_date=day, run_id=rec.run_id, error=str(e))
                rec.flush(spark, cfg.t("ops", "task_runs"))
                record_run(spark, cfg.catalog, day, "gold", rec.run_id, "FAILED", msg)
                raise
            if failure_action(state.activated) == "keep_active":
                # con trỏ đã trỏ batch mới, dữ liệu batch đầy đủ -> KHÔNG RESTORE (sẽ làm
                # gold_serving trỏ vào batch đã bị xoá). Báo FAILED để người vận hành kiểm tra.
                msg += " | batch ĐÃ kích hoạt, không rollback (runbook mục 5)"
                log("gold_failed_after_activation", as_of_date=day, run_id=rec.run_id,
                    error=str(e))
                rec.flush(spark, cfg.t("ops", "task_runs"))
                record_run(spark, cfg.catalog, day, "gold", rec.run_id, "FAILED", msg)
                raise
            # lỗi trước kích hoạt: RESTORE về version trước publish (dọn dòng batch lỗi)
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
        # batch đã kích hoạt: dọn dòng batch cũ (người đọc không còn thấy). Lỗi dọn dẹp không
        # làm sai dữ liệu người đọc thấy -> chỉ ghi log, lần chạy sau dọn tiếp.
        try:
            for t in PUBLISHED_TABLES:
                sv.cleanup_superseded(spark, cfg.catalog, t, day, rec.run_id)
        except Exception as e:  # noqa: BLE001
            log("gold_cleanup_failed", as_of_date=day, run_id=rec.run_id, error=str(e))
        # view gold_serving lỗi / thiếu -> người đọc không thấy batch mới đúng -> FAILED + raise
        try:
            refresh_serving_views(spark, cfg)
        except Exception as e:
            msg = (f"{type(e).__name__}: {e} | batch ĐÃ kích hoạt, view gold_serving cần sửa "
                   "rồi chạy 03_gold/02_dashboard_views")
            log("gold_serving_views_failed", as_of_date=day, run_id=rec.run_id, error=str(e))
            rec.flush(spark, cfg.t("ops", "task_runs"))
            record_run(spark, cfg.catalog, day, "gold", rec.run_id, "FAILED", msg)
            raise
        rec.flush(spark, cfg.t("ops", "task_runs"))
        record_run(spark, cfg.catalog, day, "gold", rec.run_id, "SUCCESS")
        log("gold_published", as_of_date=day, run_id=rec.run_id)
        done.append(day)
    return done
