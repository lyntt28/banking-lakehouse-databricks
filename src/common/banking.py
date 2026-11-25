"""Quy tắc nghiệp vụ ngân hàng dùng chung.

Phân nhóm nợ theo số ngày quá hạn (DPD - days past due), phương pháp định lượng
theo Thông tư 11/2021/TT-NHNN:
    Nhóm 1 (đủ tiêu chuẩn):   DPD < 10
    Nhóm 2 (cần chú ý):       10 <= DPD < 90
    Nhóm 3 (dưới tiêu chuẩn): 90 <= DPD < 180
    Nhóm 4 (nghi ngờ):        180 <= DPD < 360
    Nhóm 5 (có khả năng mất vốn): DPD >= 360
Nợ xấu (NPL) = nhóm 3, 4, 5.
"""

from pyspark.sql import Column
from pyspark.sql import functions as F

DEBT_GROUP_BOUNDS = [(10, 1), (90, 2), (180, 3), (360, 4)]


def debt_group_from_dpd(dpd: int | None) -> int | None:
    """Bản Python thuần, dùng để test và kiểm tra nhanh."""
    if dpd is None:
        return None
    if dpd < 0:
        raise ValueError("DPD must be >= 0")
    for upper, group in DEBT_GROUP_BOUNDS:
        if dpd < upper:
            return group
    return 5


def debt_group_col(dpd: Column) -> Column:
    """Bản Spark, dùng trong pipeline: df.withColumn('debt_group', debt_group_col(F.col('dpd')))."""
    expr = F.when(dpd.isNull(), F.lit(None))
    for upper, group in DEBT_GROUP_BOUNDS:
        expr = expr.when(dpd < upper, F.lit(group))
    return expr.otherwise(F.lit(5)).cast("int")


def is_npl_col(debt_group: Column) -> Column:
    """Nợ xấu: nhóm 3 trở lên."""
    return debt_group >= 3
