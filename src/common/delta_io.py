"""Ghi Delta dùng chung cho Silver/Gold (chỉ chạy trên Databricks / có Delta Lake).

Mọi hàm đều idempotent: chạy lại cùng ngày không nhân đôi dữ liệu.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from common.scd2 import apply_scd2, build_scd2_staging
from common.silver import merge_condition


def table_exists(spark, name: str) -> bool:
    return spark.catalog.tableExists(name)


def merge_upsert(df: DataFrame, target: str, key: list[str], insert_only: bool = False) -> None:
    """MERGE theo khóa: khớp -> cập nhật (trừ khi insert_only), chưa có -> thêm mới."""
    from delta.tables import DeltaTable

    spark = df.sparkSession
    if not table_exists(spark, target):
        df.write.format("delta").saveAsTable(target)
        return
    m = DeltaTable.forName(spark, target).alias("t").merge(df.alias("s"), merge_condition(key))
    if not insert_only:
        m = m.whenMatchedUpdateAll()
    m.whenNotMatchedInsertAll().execute()


def overwrite_date(df: DataFrame, target: str, date_col: str, as_of_date: str) -> None:
    """Ghi đè đúng 1 ngày (replaceWhere) - dùng cho mart tính lại toàn bộ ngày đó."""
    spark = df.sparkSession
    if not table_exists(spark, target):
        df.write.format("delta").saveAsTable(target)
        return
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"{date_col} = DATE'{as_of_date}'")
     .saveAsTable(target))


def overwrite(df: DataFrame, target: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(target)


def scd2_upsert(df: DataFrame, target: str, key: str, tracked: list[str],
                effective_date: str) -> None:
    """SCD2 vào target; lần đầu tạo bảng rỗng có cột kỹ thuật rồi MERGE như mọi lần."""
    from pyspark.sql import functions as F

    spark = df.sparkSession
    if not table_exists(spark, target):
        from common.scd2 import add_row_hash

        empty = (add_row_hash(df, tracked).limit(0)
                 .withColumn("valid_from", F.lit(None).cast("date"))
                 .withColumn("valid_to", F.lit(None).cast("date"))
                 .withColumn("is_current", F.lit(None).cast("boolean")))
        empty.write.format("delta").saveAsTable(target)
    current = spark.table(target).filter("is_current")
    staging = build_scd2_staging(df, current, key, tracked)
    apply_scd2(staging, target, key, effective_date)
