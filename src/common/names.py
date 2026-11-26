"""Tên bảng đầy đủ. catalog rỗng -> `schema.table` (Spark local trong test), có catalog ->
`catalog.schema.table` (Unity Catalog). Nhờ vậy pipeline Silver/Gold chạy được end-to-end
trên Spark + Delta local (tests/integration) với đúng code chạy trên Databricks."""

from __future__ import annotations


def fq(catalog: str, schema: str, table: str | None = None) -> str:
    base = f"{catalog}.{schema}" if catalog else schema
    return f"{base}.{table}" if table else base
