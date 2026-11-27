"""Governance: nội dung câu lệnh SQL + logic che cột / lọc dòng chạy trên Spark local."""

import re
from datetime import date

import pytest

from common.governance import (
    BRANCH_FILTERED,
    GROUPS,
    MASKS,
    PII_COLUMNS,
    branch_filter_body,
    mask_body,
    statements,
)

SQL = statements("bank_test")


def _eval(spark, expr: str, val, dtype: str):
    return spark.sql(f"SELECT {expr} AS r FROM (SELECT CAST(? AS {dtype}) AS val)",
                     args=[val]).first().r


@pytest.mark.parametrize("kind,val,expected", [
    ("phone", "0912345678", "*******678"),
    ("id_no", "012345678901", "********8901"),
    ("email", "user12@example.com", "u***@example.com"),
    ("name", "NGUYEN VAN AN", "N*** V*** A***"),
    ("address", "12 DUONG SO 3", "***"),
    ("phone", None, None),
])
def test_mask_hides_pii_for_other_users(spark, kind, val, expected):
    assert _eval(spark, mask_body(kind, privileged="false"), val, MASKS[kind].dtype) == expected


def test_mask_dob_keeps_year_and_income_hidden(spark):
    assert _eval(spark, mask_body("dob", "false"), "1990-07-15", "DATE") == date(1990, 1, 1)
    assert _eval(spark, mask_body("income", "false"), "25000000", "DECIMAL(22,2)") is None


@pytest.mark.parametrize("kind", list(MASKS))
def test_privileged_users_see_raw_value(spark, kind):
    raw = {"DATE": "1990-07-15", "STRING": "0912345678"}.get(MASKS[kind].dtype, "123.45")
    if kind == "email":
        raw = "user12@example.com"
    masked = _eval(spark, mask_body(kind, "false"), raw, MASKS[kind].dtype)
    plain = _eval(spark, mask_body(kind, "true"), raw, MASKS[kind].dtype)
    assert str(plain) == raw and plain != masked


def test_branch_filter(spark):
    spark.createDataFrame([("a@bank.vn", "101"), ("a@bank.vn", "102"), ("b@bank.vn", "1")],
                          "user_email string, branchnbr string").createOrReplaceTempView("ub_map")

    def sees(user, branch, all_branches="false"):
        body = branch_filter_body("x", all_branches=all_branches, user_expr=f"'{user}'",
                                  mapping="ub_map")
        return spark.sql(f"SELECT {body} AS r FROM (SELECT '{branch}' AS branchnbr_val)").first().r

    assert sees("a@bank.vn", "101") and sees("a@bank.vn", "102")
    assert not sees("a@bank.vn", "1")
    assert not sees("nobody@bank.vn", "101")
    assert sees("nobody@bank.vn", "1", all_branches="true")


def test_every_pii_column_gets_mask_and_tag():
    for table, cols in PII_COLUMNS.items():
        for col, kind in cols.items():
            fn = MASKS[kind].function
            assert (f"ALTER TABLE bank_test.{table} ALTER COLUMN {col} "
                    f"SET MASK bank_test.ops.{fn}") in SQL
            assert f"ALTER TABLE bank_test.{table} ALTER COLUMN {col} SET TAGS ('pii' = '{kind}')" \
                in SQL


def test_row_filter_on_branch_tables():
    for t in BRANCH_FILTERED:
        assert (f"ALTER TABLE bank_test.{t} SET ROW FILTER bank_test.ops.branch_filter "
                "ON (branchnbr)") in SQL


def test_least_privilege_grants():
    joined = "\n".join(SQL)
    assert "TO `branch_users`" in joined
    # branch_users chỉ đọc gold_serving, không đụng gold (bảng gốc) / silver / bronze / ops data
    bu = [s for s in SQL if s.endswith("TO `branch_users`") and "ON SCHEMA" in s]
    assert {s.split("ON SCHEMA ")[1].split(" ")[0] for s in bu} == {"bank_test.gold_serving",
                                                                    "bank_test.ops"}
    # người đọc không có quyền gì trên schema gold (dòng batch chưa kích hoạt / batch cũ)
    for g in ["risk_analysts", "branch_users"]:
        assert not [s for s in SQL if s.startswith("GRANT") and s.endswith(f"`{g}`")
                    and re.search(r"bank_test\.gold[. ]", s)], g
    assert all("MODIFY" not in s for s in SQL if "risk_analysts" in s or "branch_users" in s)
    # nhóm đọc giới hạn không có SELECT cả schema gold (branch_users) / ops (cả hai)
    for g, schema in [("branch_users", "gold"), ("branch_users", "gold_serving"),
                      ("branch_users", "ops"),
                      ("risk_analysts", "ops")]:
        assert not any(f"ON SCHEMA bank_test.{schema} TO `{g}`" in s and "SELECT" in s
                       for s in SQL), (g, schema)
    assert all(f"GRANT USE CATALOG ON CATALOG bank_test TO `{g}`" in SQL for g in GROUPS)


def test_functions_created_before_use():
    first_mask_use = next(i for i, s in enumerate(SQL) if "SET MASK" in s)
    last_fn = max(i for i, s in enumerate(SQL) if s.startswith("CREATE OR REPLACE FUNCTION"))
    assert last_fn < first_mask_use


def test_restricted_tables_never_granted_to_limited_groups():
    from common.governance import RESTRICTED_TABLES

    for t in RESTRICTED_TABLES:
        assert not any(f"ON TABLE bank_test.{t} " in s + " " for s in SQL if "GRANT SELECT" in s)


def test_branch_users_only_get_row_filtered_or_dimension_tables():
    from common.governance import BRANCH_FILTERED

    granted = {s.split("ON TABLE bank_test.")[1].split(" ")[0] for s in SQL
               if s.startswith("GRANT SELECT ON TABLE") and s.endswith("`branch_users`")}
    base = set(BRANCH_FILTERED) | {"gold.dim_date", "gold.dim_branch", "gold.dim_product"}
    # chỉ view gold_serving.* của đúng các bảng đó (lọc dòng vẫn áp dụng qua bảng gốc)
    allowed = {t.replace("gold.", "gold_serving.", 1) for t in base}
    assert granted == allowed


def test_table_grants_after_row_filters():
    last_filter = max(i for i, s in enumerate(SQL) if "SET ROW FILTER" in s)
    first_grant = min(i for i, s in enumerate(SQL) if s.startswith("GRANT SELECT ON TABLE"))
    assert last_filter < first_grant
