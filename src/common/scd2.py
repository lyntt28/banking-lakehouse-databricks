"""SCD Type 2 - lưu lịch sử thay đổi của dimension (vd: khách hàng đổi địa chỉ).

Cột kỹ thuật trong bảng đích:
    row_hash     : hash các cột theo dõi, để phát hiện thay đổi
    valid_from   : ngày bắt đầu hiệu lực
    valid_to     : ngày hết hiệu lực (NULL = đang hiệu lực)
    is_current   : TRUE nếu là bản ghi hiện hành

Cách làm (1 lệnh MERGE, chạy lại an toàn):
    1. build_scd2_staging: so sánh dữ liệu mới với bản hiện hành, tìm bản ghi mới/đổi.
       Bản ghi đổi được nhân đôi: 1 dòng để ĐÓNG bản cũ (merge_key = business key),
       1 dòng để CHÈN bản mới (merge_key = NULL nên không khớp, đi vào nhánh INSERT).
    2. apply_scd2: chạy MERGE vào bảng Delta.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.dates import check_date


def add_row_hash(df: DataFrame, tracked_cols: list[str]) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in tracked_cols]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def build_scd2_staging(
    incoming: DataFrame, current: DataFrame, key: str, tracked_cols: list[str]
) -> DataFrame:
    """incoming: dữ liệu mới (mỗi key 1 dòng). current: bản ghi is_current = TRUE của bảng đích."""
    inc = add_row_hash(incoming, tracked_cols)
    cur = current.select(F.col(key).alias("_cur_key"), F.col("row_hash").alias("_cur_hash"))

    joined = inc.join(cur, inc[key] == cur["_cur_key"], "left")
    new_rows = joined.filter(F.col("_cur_key").isNull())
    changed = joined.filter(
        F.col("_cur_key").isNotNull() & (F.col("row_hash") != F.col("_cur_hash"))
    )

    cols = inc.columns
    close_old = changed.select(F.col(key).alias("merge_key"), *cols)
    insert_new = changed.unionByName(new_rows).select(
        F.lit(None).cast(inc.schema[key].dataType).alias("merge_key"), *cols
    )
    return close_old.unionByName(insert_new)


def apply_scd2(staging: DataFrame, target_table: str, key: str, effective_date: str) -> None:
    """MERGE staging vào bảng Delta đích. Chỉ chạy trên Databricks (cần Delta Lake)."""
    from delta.tables import DeltaTable

    effective_date = check_date(effective_date)

    spark = staging.sparkSession
    target = DeltaTable.forName(spark, target_table)
    data_cols = [c for c in staging.columns if c != "merge_key"]
    insert_values = {c: f"s.{c}" for c in data_cols}
    insert_values.update(
        {"valid_from": f"DATE'{effective_date}'", "valid_to": "NULL", "is_current": "TRUE"}
    )

    (
        target.alias("t")
        .merge(staging.alias("s"), f"t.{key} = s.merge_key AND t.is_current = TRUE")
        .whenMatchedUpdate(
            condition="t.row_hash <> s.row_hash",
            # greatest(): bản ghi đổi 2 lần trong cùng ngày không bị valid_to < valid_from
            set={
                "valid_to": f"greatest(DATE'{effective_date}' - INTERVAL 1 DAY, t.valid_from)",
                "is_current": "FALSE",
            },
        )
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
