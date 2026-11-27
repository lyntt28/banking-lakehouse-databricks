"""Publish Gold nguyên tử theo batch: dữ liệu ghi THÊM kèm `batch_id`, người đọc chỉ thấy batch
đã được kích hoạt trong con trỏ `ops.published_batches`.

Vấn đề cũ: publish ghi đè lần lượt 18 bảng -> trong lúc ghi, dashboard có thể thấy bảng mới /
bảng cũ lẫn lộn; khi nguồn gửi file sửa cho ngày ĐÃ publish, lọc theo as_of_date không phân
biệt được bản cũ / bản mới.

Cách làm:
    1. Mỗi lần publish có batch_id (= run_id Gold). Mọi bảng Gold ghi THÊM dòng của batch đó
       (append / MERGE insert-only) -> không đụng tới dòng đang được đọc.
    2. Kích hoạt = 1 dòng append vào `ops.published_batches` (1 commit Delta = nguyên tử).
       Batch đang dùng của ngày D = dòng `published_at` mới nhất của D.
    3. Người đọc (dashboard, view, ML) đọc schema `gold_serving`: view lọc đúng batch đang dùng.
    4. Sau khi kích hoạt mới xoá dòng của batch cũ (người đọc đã không còn thấy chúng).

Batch lỗi giữa chừng: chưa kích hoạt -> vô hình với người đọc; rollback (RESTORE) vẫn dọn nó.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.names import fq

BATCH_COL = "batch_id"
LEGACY_BATCH = "legacy"

# daily   : 1 snapshot / ngày (as_of_date). Batch đang dùng của ngày = batch mới nhất ngày đó.
# snapshot: cả bảng tính lại mỗi batch. Batch đang dùng = batch mới nhất toàn bảng.
# append  : fact bất biến (MERGE insert-only). Thấy mọi dòng thuộc batch đã kích hoạt.
TABLE_KIND = {
    "dim_date": "snapshot", "dim_branch": "snapshot", "dim_product": "snapshot",
    "dim_customer": "snapshot", "loan_fact_daily": "daily", "core_fact_transaction": "append",
    "loan_mart_overdue_daily": "daily", "loan_mart_npl_daily": "snapshot",
    "dep_mart_daily": "daily", "cust_mart_360": "snapshot", "card_mart_kpi_daily": "daily",
    "card_mart_spend_mix": "daily", "card_mart_kpi_monthly": "daily", "dep_mart_trend": "daily",
    "dep_mart_by_tenor": "daily", "dep_mart_casa_customer": "daily",
    "dep_mart_new_accounts": "daily", "card_mart_overdue_daily": "daily",
}


def _pointer(catalog: str) -> str:
    return fq(catalog, "ops", "published_batches")


def active_by_date_sql(catalog: str) -> str:
    """(as_of_date, batch_id) đang dùng: dòng kích hoạt mới nhất của mỗi ngày."""
    return (f"SELECT as_of_date, max_by(run_id, published_at) AS batch_id "
            f"FROM {_pointer(catalog)} GROUP BY as_of_date")


def latest_batch_sql(catalog: str) -> str:
    return f"(SELECT max_by(run_id, published_at) FROM {_pointer(catalog)})"


def serving_view_sql(catalog: str, table: str, columns: list[str]) -> str:
    """View đọc batch đang dùng của bảng Gold (bỏ cột batch_id)."""
    src = fq(catalog, "gold", table)
    view = fq(catalog, "gold_serving", table)
    cols = ", ".join(f"s.`{c}`" for c in columns if c != BATCH_COL)
    kind = TABLE_KIND[table]
    if kind == "daily":
        body = (f"SELECT {cols} FROM {src} s JOIN ({active_by_date_sql(catalog)}) a "
                f"ON s.as_of_date = a.as_of_date AND s.{BATCH_COL} = a.batch_id")
    elif kind == "snapshot":
        body = f"SELECT {cols} FROM {src} s WHERE s.{BATCH_COL} = {latest_batch_sql(catalog)}"
    else:
        body = (f"SELECT {cols} FROM {src} s WHERE s.{BATCH_COL} IN "
                f"(SELECT run_id FROM {_pointer(catalog)})")
    return f"CREATE OR REPLACE VIEW {view} AS {body}"


def create_serving_views(spark, catalog: str) -> list[str]:
    """Tạo / cập nhật mọi view gold_serving.* (chạy sau publish: bảng mới có thể thêm cột)."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq(catalog, 'gold_serving')}")
    done = []
    for t in TABLE_KIND:
        src = fq(catalog, "gold", t)
        if spark.catalog.tableExists(src):
            spark.sql(serving_view_sql(catalog, t, spark.table(src).columns))
            done.append(t)
    return done


def ensure_batch_column(spark, catalog: str, table: str) -> None:
    """Bảng cũ (trước nâng cấp) chưa có batch_id: thêm cột và gán batch đang dùng hiện tại
    (theo ngày với bảng daily, batch mới nhất với bảng còn lại) để view không mất dữ liệu."""
    target = fq(catalog, "gold", table)
    if not spark.catalog.tableExists(target) or BATCH_COL in spark.table(target).columns:
        return
    spark.sql(f"ALTER TABLE {target} ADD COLUMNS ({BATCH_COL} STRING)")
    ptr = _pointer(catalog)
    has_ptr = spark.catalog.tableExists(ptr) and spark.table(ptr).limit(1).count() > 0
    if not has_ptr:   # chưa từng publish: tạo dòng kích hoạt 'legacy' cho dữ liệu sẵn có
        spark.sql(f"UPDATE {target} SET {BATCH_COL} = '{LEGACY_BATCH}'")
        return
    if TABLE_KIND[table] == "daily":
        spark.sql(f"MERGE INTO {target} t USING ({active_by_date_sql(catalog)}) a "
                  f"ON t.as_of_date = a.as_of_date WHEN MATCHED THEN UPDATE SET "
                  f"t.{BATCH_COL} = a.batch_id")
    else:
        latest = spark.sql(latest_batch_sql(catalog)[1:-1]).first()[0]
        spark.sql(f"UPDATE {target} SET {BATCH_COL} = '{latest}'")


def write_batch(df: DataFrame, catalog: str, table: str, batch_id: str,
                key: list[str] | None = None) -> None:
    """Ghi THÊM dòng của batch (không xoá / ghi đè dòng đang được đọc)."""
    spark = df.sparkSession
    target = fq(catalog, "gold", table)
    ensure_batch_column(spark, catalog, table)
    df = df.withColumn(BATCH_COL, F.lit(batch_id))
    if not spark.catalog.tableExists(target):
        df.write.format("delta").saveAsTable(target)
    elif TABLE_KIND[table] == "append":
        from common.delta_io import merge_upsert

        merge_upsert(df, target, key or [], insert_only=True)
    else:
        df.write.format("delta").mode("append").option("mergeSchema", "true") \
            .saveAsTable(target)


def candidate(spark, catalog: str, table: str, day_s: str, batch_id: str) -> DataFrame:
    """Bảng như SAU khi batch này được kích hoạt (dùng để dựng mart luỹ kế trước khi kích hoạt):
    dòng của batch này cho ngày `day_s` + dòng đang dùng của các ngày khác."""
    target = fq(catalog, "gold", table)
    t = spark.table(target)
    kind = TABLE_KIND[table]
    if kind == "snapshot":
        return t.filter(F.col(BATCH_COL) == batch_id).drop(BATCH_COL)
    if kind == "append":
        active = spark.sql(f"SELECT run_id FROM {_pointer(catalog)}")
        return (t.join(active, t[BATCH_COL] == active.run_id, "left")
                .filter((F.col(BATCH_COL) == batch_id) | F.col("run_id").isNotNull())
                .drop("run_id", BATCH_COL))
    day = F.lit(day_s).cast("date")
    active = spark.sql(active_by_date_sql(catalog)).withColumnRenamed("as_of_date", "_d") \
        .withColumnRenamed("batch_id", "_b")
    j = t.join(active, (t.as_of_date == active._d) & (t[BATCH_COL] == active._b), "left")
    keep = ((F.col("as_of_date") == day) & (F.col(BATCH_COL) == batch_id)) | \
        ((F.col("as_of_date") != day) & F.col("_b").isNotNull())
    return j.filter(keep).drop("_d", "_b", BATCH_COL)


def cleanup_superseded(spark, catalog: str, table: str, day_s: str, batch_id: str) -> None:
    """Sau khi kích hoạt: xoá dòng của batch cũ / batch lỗi (người đọc đã không thấy chúng)."""
    target = fq(catalog, "gold", table)
    if not spark.catalog.tableExists(target):
        return
    kind = TABLE_KIND[table]
    if kind == "daily":
        spark.sql(f"DELETE FROM {target} WHERE as_of_date = DATE'{day_s}' "
                  f"AND {BATCH_COL} <> '{batch_id}'")
    elif kind == "snapshot":
        spark.sql(f"DELETE FROM {target} WHERE {BATCH_COL} <> '{batch_id}'")
    # append: fact bất biến, dòng của batch cũ vẫn là dữ liệu đúng -> giữ
