"""Thay thế psycopg2 cho code dashboard gốc: chuyển kết nối sang Spark / Databricks SQL.
Đặt thư mục apps/dashboards lên đầu sys.path trước khi import api_server.py gốc."""

from pgcompat import Connection, Error, connect  # noqa: F401

from . import extras, pool  # noqa: F401

OperationalError = DatabaseError = InterfaceError = Error
