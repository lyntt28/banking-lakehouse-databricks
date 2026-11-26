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

Ngữ nghĩa "daily snapshot": mỗi khách hàng tối đa 1 phiên bản / ngày. Nếu bản hiện hành đã
bắt đầu đúng ngày hiệu lực (file sửa lại / chạy lại cùng ngày có thay đổi) thì CẬP NHẬT TẠI
CHỖ phiên bản đó (_action = 'correct'), không đóng + chèn bản mới. Nhờ vậy khoảng hiệu lực
[valid_from, valid_to] (đóng 2 đầu, theo ngày) không bao giờ chồng lấn và
(customer_id, valid_from) - nguồn của customer_sk - luôn duy nhất.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.dates import check_date


def add_row_hash(df: DataFrame, tracked_cols: list[str]) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in tracked_cols]
    return df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))


def build_scd2_staging(
    incoming: DataFrame, current: DataFrame, key: str, tracked_cols: list[str],
    effective_date: str | None = None,
) -> DataFrame:
    """incoming: dữ liệu mới (mỗi key 1 dòng). current: bản ghi is_current = TRUE của bảng đích.

    Cột _action: 'close' (đóng bản cũ), 'insert' (chèn bản mới), 'correct' (sửa tại chỗ bản
    đã bắt đầu đúng effective_date)."""
    inc = add_row_hash(incoming, tracked_cols)
    vf = (F.col("valid_from") if "valid_from" in current.columns
          else F.lit(None).cast("date"))
    cur = current.select(F.col(key).alias("_cur_key"), F.col("row_hash").alias("_cur_hash"),
                         vf.alias("_cur_from"))

    joined = inc.join(cur, inc[key] == cur["_cur_key"], "left")
    new_rows = joined.filter(F.col("_cur_key").isNull())
    changed = joined.filter(
        F.col("_cur_key").isNotNull() & (F.col("row_hash") != F.col("_cur_hash"))
    )

    if effective_date:
        same_day = F.coalesce(
            F.col("_cur_from") == F.lit(check_date(effective_date)).cast("date"), F.lit(False))
    else:
        same_day = F.lit(False)
    correct = changed.filter(same_day)
    changed = changed.filter(~same_day)

    cols = inc.columns
    key_type = inc.schema[key].dataType
    fix = correct.select(F.col(key).alias("merge_key"), F.lit("correct").alias("_action"), *cols)
    close_old = changed.select(F.col(key).alias("merge_key"), F.lit("close").alias("_action"),
                               *cols)
    insert_new = changed.unionByName(new_rows).select(
        F.lit(None).cast(key_type).alias("merge_key"), F.lit("insert").alias("_action"), *cols
    )
    return fix.unionByName(close_old).unionByName(insert_new)


def apply_scd2(staging: DataFrame, target_table: str, key: str, effective_date: str) -> None:
    """MERGE staging vào bảng Delta đích. Chỉ chạy trên Databricks (cần Delta Lake)."""
    from delta.tables import DeltaTable

    effective_date = check_date(effective_date)

    spark = staging.sparkSession
    target = DeltaTable.forName(spark, target_table)
    data_cols = [c for c in staging.columns if c not in ("merge_key", "_action")]
    insert_values = {c: f"s.{c}" for c in data_cols}
    insert_values.update(
        {"valid_from": f"DATE'{effective_date}'", "valid_to": "NULL", "is_current": "TRUE"}
    )

    (
        target.alias("t")
        .merge(staging.alias("s"), f"t.{key} = s.merge_key AND t.is_current = TRUE")
        # sửa tại chỗ phiên bản cùng ngày (giữ valid_from) -> không sinh phiên bản trùng ngày
        .whenMatchedUpdate(
            condition="s._action = 'correct' AND t.row_hash <> s.row_hash",
            set={c: f"s.{c}" for c in data_cols},
        )
        # bản cũ bắt đầu TRƯỚC effective_date nên valid_to = ngày trước >= valid_from
        .whenMatchedUpdate(
            condition="s._action = 'close' AND t.row_hash <> s.row_hash",
            set={
                "valid_to": f"DATE'{effective_date}' - INTERVAL 1 DAY",
                "is_current": "FALSE",
            },
        )
        .whenNotMatchedInsert(condition="s._action = 'insert'", values=insert_values)
        .execute()
    )
