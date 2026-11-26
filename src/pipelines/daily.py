"""Chạy Silver -> Gold XEN KẼ từng ngày.

Vì sao không chạy hết Silver rồi mới Gold: các bảng Silver dạng snapshot (core_accounts,
card_master, ...) được MERGE theo khóa, chỉ giữ trạng thái MỚI NHẤT. Gold của ngày D phải được
dựng ngay sau Silver ngày D, trước khi Silver ngày D+1 ghi đè. Job bị gián đoạn nhiều ngày ->
lần chạy sau xử lý D, D+1, ... mỗi ngày đủ Silver + Gold.
"""

from __future__ import annotations

from dataclasses import replace

from pyspark.sql import SparkSession

from common.batch import pending_dates, watermark
from common.ops_log import log
from pipelines import gold, silver


def run(spark: SparkSession, cfg: silver.SilverConfig) -> list[str]:
    silver.ensure_tables(spark, cfg)
    if cfg.as_of_date:
        days = [cfg.as_of_date]
    else:
        bronze_dates = [r[0] for r in spark.table(cfg.t("bronze", "loans"))
                        .select("_as_of_date").distinct().collect()]
        days = pending_dates(bronze_dates, watermark(spark, cfg.catalog, "silver"))
        # Gold còn nợ ngày mà Silver đã xong (vd: lần trước Gold fail gate) -> dựng lại trước
        gold_wm = watermark(spark, cfg.catalog, "gold")
        silver_wm = watermark(spark, cfg.catalog, "silver")
        if silver_wm and (gold_wm is None or gold_wm < silver_wm):
            gold.run(spark, gold.GoldConfig(catalog=cfg.catalog, as_of_date=silver_wm))
    log("daily_pending", catalog=cfg.catalog, days=days)
    for d in days:
        silver.run(spark, replace(cfg, as_of_date=d))
        gold.run(spark, gold.GoldConfig(catalog=cfg.catalog, as_of_date=d))
    return days
