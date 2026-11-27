"""Chạy Silver -> Gold XEN KẼ từng ngày.

Vì sao không chạy hết Silver rồi mới Gold: các bảng Silver dạng snapshot (core_accounts,
card_master, ...) được MERGE theo khóa, chỉ giữ trạng thái MỚI NHẤT. Gold của ngày D phải được
dựng ngay sau Silver ngày D, trước khi Silver ngày D+1 ghi đè. Job bị gián đoạn nhiều ngày ->
lần chạy sau xử lý D, D+1, ... mỗi ngày đủ Silver + Gold.

Correction: nguồn gửi lại file sửa cho ngày đã Silver SUCCESS -> Bronze có file mới hơn lần
Silver đó. Ngày = watermark Silver: chạy lại Silver rồi Gold ngày đó (không dựng Gold từ Silver
cũ). Ngày < watermark: snapshot MERGE đã tiến qua -> dừng với CorrectionNeedsReplay (runbook).
"""

from __future__ import annotations

from dataclasses import replace

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.batch import corrected_days, pending_dates, watermark
from common.ops_log import log
from common.silver import ORDER, TABLES
from pipelines import gold, silver


class CorrectionNeedsReplay(RuntimeError):
    """File sửa cho ngày cũ hơn watermark Silver: phải replay từ ngày đó, không tự xử lý."""


def find_corrections(spark: SparkSession, cfg: silver.SilverConfig) -> list[str]:
    """Ngày Silver SUCCESS mà Bronze nhận thêm file sau thời điểm đó."""
    runs = cfg.t("ops", "pipeline_runs")
    done = {str(r[0]): r[1] for r in spark.sql(
        f"SELECT as_of_date, max(finished_at) FROM {runs} WHERE layer = 'silver' "
        "AND status = 'SUCCESS' GROUP BY as_of_date").collect()}
    if not done:
        return []
    loads = None
    for name in ORDER:
        df = (spark.table(cfg.t("bronze", TABLES[name].bronze))
              .groupBy("_as_of_date").agg(F.max("_load_ts").alias("ts")))
        loads = df if loads is None else loads.unionByName(df)
    last = {str(r[0]): r[1] for r in loads.groupBy("_as_of_date").agg(F.max("ts")).collect()}
    return corrected_days(last, done)


def run(spark: SparkSession, cfg: silver.SilverConfig) -> list[str]:
    silver.ensure_tables(spark, cfg)
    if cfg.as_of_date:
        days = [cfg.as_of_date]
    else:
        bronze_dates = [r[0] for r in spark.table(cfg.t("bronze", "loans"))
                        .select("_as_of_date").distinct().collect()]
        silver_wm = watermark(spark, cfg.catalog, "silver")
        days = pending_dates(bronze_dates, silver_wm, daily_from=cfg.daily_from or None,
                             expected_until=cfg.expected_until())
        corrections = find_corrections(spark, cfg)
        too_old = [d for d in corrections if silver_wm and d < silver_wm]
        if too_old:
            raise CorrectionNeedsReplay(
                f"Nguồn gửi file sửa cho {too_old} nhưng Silver đã ở {silver_wm}: phải replay "
                "từ ngày đó (docs/incident_runbook.md mục 6), job không tự ghi đè quá khứ")
        gold_wm = watermark(spark, cfg.catalog, "gold")
        if silver_wm in corrections:
            # file sửa cho ngày mới nhất: xử lý lại Silver trước, rồi mới dựng Gold
            log("daily_correction", catalog=cfg.catalog, as_of_date=silver_wm)
            silver.run(spark, replace(cfg, as_of_date=silver_wm))
            gold.run(spark, gold.GoldConfig(catalog=cfg.catalog, as_of_date=silver_wm))
        elif silver_wm and (gold_wm is None or gold_wm < silver_wm):
            # Gold còn nợ ngày mà Silver đã xong và không có file mới (vd: Gold fail gate do lỗi
            # logic / hạ tầng) -> dựng lại Gold từ Silver hiện có
            gold.run(spark, gold.GoldConfig(catalog=cfg.catalog, as_of_date=silver_wm))
    log("daily_pending", catalog=cfg.catalog, days=days)
    done = []
    for d in days:
        if not silver.run(spark, replace(cfg, as_of_date=d)):
            break   # WAITING: file chưa về, chưa tới hạn SLA -> dừng, lần chạy sau làm tiếp
        gold.run(spark, gold.GoldConfig(catalog=cfg.catalog, as_of_date=d))
        done.append(d)
    return done
