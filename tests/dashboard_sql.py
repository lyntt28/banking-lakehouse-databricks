"""Chạy SQL dataset dashboard trên temp view local (dùng chung cho mọi test dashboard).

Bảng `schema.table` -> temp view `schema_table`; tham số `:name` -> literal SQL."""

from __future__ import annotations

import re

SCHEMAS = ("gold_serving", "gold", "silver", "ops")


def localize(sql: str, params: dict[str, str]) -> str:
    """params: tên tham số -> literal SQL đã quote (vd "DATE'2026-11-01'", "'all'")."""
    for schema in SCHEMAS:
        prefix = "gold" if schema == "gold_serving" else schema
        sql = re.sub(rf"\b{schema}\.", f"{prefix}_", sql)
    for k, v in params.items():
        sql = re.sub(rf":{k}\b", v, sql)
    return sql


def literals(report_date: str, **params: str) -> dict[str, str]:
    """report_date + tham số chuỗi -> dict literal cho `localize`."""
    return {"report_date": f"DATE'{report_date}'", **{k: f"'{v}'" for k, v in params.items()}}
