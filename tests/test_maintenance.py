import pytest

from common.governance import BRANCH_FILTERED
from common.maintenance import (
    CLUSTERING,
    benchmark_sql,
    cluster_statements,
    vacuum_statements,
)


def test_cluster_then_optimize_each_table():
    sql = cluster_statements("bank_dev")
    assert len(sql) == 2 * len(CLUSTERING)
    for i, (t, cols) in enumerate(CLUSTERING.items()):
        assert sql[2 * i] == f"ALTER TABLE bank_dev.{t} CLUSTER BY ({', '.join(cols)})"
        assert sql[2 * i + 1] == f"OPTIMIZE bank_dev.{t}"


def test_skips_missing_tables():
    assert cluster_statements("c", existing={"gold.dim_customer"}) == [
        "ALTER TABLE c.gold.dim_customer CLUSTER BY (customer_id)",
        "OPTIMIZE c.gold.dim_customer"]


def test_vacuum_keeps_at_least_7_days():
    assert all(s.endswith("RETAIN 168 HOURS") for s in vacuum_statements("c"))
    with pytest.raises(ValueError):
        vacuum_statements("c", retain_hours=24)


def test_branch_filtered_fact_tables_cluster_on_branch_or_key():
    # bảng có row filter theo chi nhánh nên cluster theo branchnbr để filter bỏ được file
    for t in ("gold.loan_fact_daily", "gold.loan_mart_overdue_daily"):
        assert t in BRANCH_FILTERED and "branchnbr" in CLUSTERING[t]


def test_benchmarks_parse_on_spark(spark):
    """Câu benchmark phải là SQL hợp lệ: parse bằng Spark (không cần bảng thật)."""
    for name, sql in benchmark_sql("cat", "2026-11-01").items():
        assert "{" not in sql, name
        spark._jsparkSession.sessionState().sqlParser().parsePlan(sql)
