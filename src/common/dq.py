"""Data Quality gate.

Ý tưởng:
- Mỗi rule là 1 điều kiện SQL mô tả bản ghi HỢP LỆ.
- severity = "FAIL": bản ghi vi phạm bị đưa vào quarantine, không đi tiếp.
- severity = "WARN": bản ghi vẫn đi tiếp, chỉ ghi log để theo dõi.
- Bản ghi lỗi không bị xóa: giữ lại kèm danh sách rule vi phạm.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    name: str
    condition: str          # SQL boolean expression, TRUE = hợp lệ
    severity: str = "FAIL"  # FAIL | WARN


def _violation_flags(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Thêm cột _failed_rules / _warned_rules (mảng tên rule bị vi phạm).

    NULL trong điều kiện được coi là vi phạm (coalesce về False).
    """
    def collect(sev: str):
        flags = [
            F.when(~F.coalesce(F.expr(r.condition), F.lit(False)), F.lit(r.name))
            for r in rules
            if r.severity == sev
        ]
        if not flags:
            return F.array().cast("array<string>")
        return F.filter(F.array(*flags), lambda x: x.isNotNull())

    return df.withColumn("_failed_rules", collect("FAIL")).withColumn(
        "_warned_rules", collect("WARN")
    )


def apply_dq(df: DataFrame, rules: list[Rule]) -> tuple[DataFrame, DataFrame]:
    """Tách df thành (valid_df, quarantine_df).

    valid_df: giữ nguyên các cột gốc (bản ghi WARN vẫn nằm ở đây).
    quarantine_df: cột gốc + _failed_rules.
    """
    flagged = _violation_flags(df, rules)
    is_bad = F.size("_failed_rules") > 0
    valid = flagged.filter(~is_bad).drop("_failed_rules", "_warned_rules")
    quarantine = flagged.filter(is_bad).drop("_warned_rules")
    return valid, quarantine


def dq_summary(df: DataFrame, rules: list[Rule], table_name: str) -> DataFrame:
    """1 dòng cho mỗi rule: tổng số dòng, số dòng lỗi, % lỗi, trạng thái."""
    total = df.count()
    flagged = _violation_flags(df, rules).cache()
    spark = df.sparkSession
    rows = []
    for r in rules:
        col = "_failed_rules" if r.severity == "FAIL" else "_warned_rules"
        failed = flagged.filter(F.array_contains(col, r.name)).count()
        pct = (failed / total * 100) if total else 0.0
        status = "PASS" if failed == 0 else r.severity
        rows.append((table_name, r.name, r.severity, total, failed, round(pct, 4), status))
    flagged.unpersist()
    return spark.createDataFrame(
        rows,
        "table_name string, rule_name string, severity string, total_rows long, "
        "failed_rows long, fail_pct double, status string",
    )


def should_stop(summary: DataFrame, max_fail_pct: float = 5.0) -> bool:
    """Dừng pipeline nếu có rule FAIL vượt ngưỡng % cho phép."""
    return (
        summary.filter((F.col("severity") == "FAIL") & (F.col("fail_pct") > max_fail_pct))
        .limit(1)
        .count()
        > 0
    )
