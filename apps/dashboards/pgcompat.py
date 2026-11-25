"""Chạy code dashboard gốc (viết cho PostgreSQL / psycopg2) trên Spark hoặc Databricks SQL.

`translate()` đổi những cú pháp PostgreSQL mà code gốc dùng sang Spark SQL:
    %s                      -> ?            (tham số vị trí)
    %%                      -> %
    x::date / ::text / ...  -> CAST(x AS ...)
    "ident"                 -> `ident`
    INTERVAL '5 months'     -> INTERVAL 5 months
    to_char(x, 'YYYY-MM')   -> date_format(x, 'yyyy-MM')
Truy vấn information_schema.tables (kiểm tra bảng tồn tại) được trả lời trực tiếp.

Backend chọn bằng biến môi trường:
    PDB_BACKEND=databricks  databricks-sql-connector (DATABRICKS_SERVER_HOSTNAME,
                            DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN, LEGACY_CATALOG,
                            LEGACY_SCHEMA=legacy)
    PDB_BACKEND=spark       SparkSession đang chạy (test / chạy local) - set_spark()
"""

from __future__ import annotations

import os
import re

_SPARK = None


def set_spark(spark) -> None:
    global _SPARK
    _SPARK = spark


# ---------------------------------------------------------------- dịch SQL
_CAST_TYPES = {"date": "DATE", "text": "STRING", "varchar": "STRING", "numeric": "DECIMAL(38,6)",
               "int": "INT", "integer": "INT", "bigint": "BIGINT", "float": "DOUBLE",
               "double": "DOUBLE", "timestamp": "TIMESTAMP"}
_PG_FMT = [("YYYY", "yyyy"), ("HH24", "HH"), ("MI", "mm"), ("SS", "ss"), ("Mon", "MMM"),
           ("DD", "dd")]


def _split_strings(sql: str) -> list[tuple[bool, str]]:
    """Tách thành các đoạn (là_chuỗi_trong_nháy_đơn, nội_dung)."""
    out, i, buf = [], 0, ""
    while i < len(sql):
        ch = sql[i]
        if ch == "'":
            if buf:
                out.append((False, buf))
                buf = ""
            j = i + 1
            while j < len(sql):
                if sql[j] == "'" and j + 1 < len(sql) and sql[j + 1] == "'":
                    j += 2
                    continue
                if sql[j] == "'":
                    break
                j += 1
            out.append((True, sql[i:j + 1]))
            i = j + 1
            continue
        buf += ch
        i += 1
    if buf:
        out.append((False, buf))
    return out


def _find_call(sql: str, name: str):
    """Vị trí (start, args_start, end) của lời gọi hàm name(...) đầu tiên, end = sau ')'."""
    m = re.search(r"\b" + name + r"\s*\(", sql, re.I)
    if not m:
        return None
    depth, i = 1, m.end()
    in_str = False
    while i < len(sql) and depth:
        c = sql[i]
        if c == "'":
            in_str = not in_str
        elif not in_str:
            depth += c == "("
            depth -= c == ")"
        i += 1
    return m.start(), m.end(), i


def _to_char(sql: str) -> str:
    while True:
        f = _find_call(sql, "to_char")
        if not f:
            return sql
        start, a0, end = f
        inner = sql[a0:end - 1]
        expr, _, fmt = inner.rpartition(",")
        fmt = fmt.strip().strip("'")
        for pg, sp in _PG_FMT:
            fmt = fmt.replace(pg, sp)
        sql = f"{sql[:start]}date_format({expr.strip()}, '{fmt}'){sql[end:]}"


def translate(sql: str) -> str:
    sql = sql.replace("%s", "?").replace("%%", "%")
    sql = re.sub(r"INTERVAL\s+'(\d+)\s+(\w+)'", r"INTERVAL \1 \2", sql, flags=re.I)
    parts = []
    for is_str, text in _split_strings(sql):
        if not is_str:
            text = re.sub(r'"([A-Za-z_]\w*)"', r"`\1`", text)
        parts.append(text)
    sql = "".join(parts)
    operand = r"(\?|'[^']*'|\([^()]*\)|[A-Za-z_][\w.]*)"
    sql = re.sub(operand + r"\s*::\s*(\w+)",
                 lambda m: f"CAST({m.group(1)} AS {_CAST_TYPES.get(m.group(2).lower(), m.group(2))})",
                 sql)
    return _to_char(sql)


# ---------------------------------------------------------------- DB-API
class Error(Exception):
    pass


class Cursor:
    def __init__(self, conn, dict_rows: bool = False):
        self.conn, self.dict_rows = conn, dict_rows
        self._rows, self.description, self.rowcount = [], None, -1

    def execute(self, sql, params=None):
        params = list(params) if params else []
        m = re.search(r"information_schema\.tables.*?table_name\s*=\s*'(\w+)'", sql, re.S | re.I)
        if m:
            ok = self.conn.table_exists(m.group(1))
            self._set([(1,)] if ok else [], ["?column?"])
            return
        try:
            cols, rows = self.conn.run(translate(sql), params)
        except Exception as e:  # giữ hành vi psycopg2: lỗi là psycopg2.Error
            raise Error(f"{e}\n--- SQL ---\n{translate(sql)}") from e
        self._set(rows, cols)

    def _set(self, rows, cols):
        self.description = [(c, None, None, None, None, None, None) for c in cols]
        if self.dict_rows:
            rows = [dict(zip(cols, r)) for r in rows]
        self._rows, self.rowcount = list(rows), len(rows)

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def fetchmany(self, n=1):
        rows, self._rows = self._rows[:n], self._rows[n:]
        return rows

    def __iter__(self):
        return iter(self.fetchall())

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


class Connection:
    autocommit = True

    def __init__(self):
        self.backend = os.environ.get("PDB_BACKEND", "spark")
        self._db = None
        if self.backend == "databricks":
            from databricks import sql as dbsql

            self._db = dbsql.connect(
                server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
                http_path=os.environ["DATABRICKS_HTTP_PATH"],
                access_token=os.environ.get("DATABRICKS_TOKEN"),
                catalog=os.environ.get("LEGACY_CATALOG", "bank_dev"),
                schema=os.environ.get("LEGACY_SCHEMA", "legacy"))

    def run(self, sql, params):
        if self._db is not None:
            with self._db.cursor() as c:
                c.execute(sql, params)
                cols = [d[0] for d in c.description or []]
                return cols, [tuple(r) for r in c.fetchall()]
        if _SPARK is None:
            raise Error("pgcompat: chưa set_spark() và PDB_BACKEND không phải databricks")
        df = _SPARK.sql(sql, args=params) if params else _SPARK.sql(sql)
        return df.columns, [tuple(r) for r in df.collect()]

    def table_exists(self, name: str) -> bool:
        if self._db is not None:
            cols, rows = self.run(f"SHOW TABLES LIKE '{name}'", [])
            return bool(rows)
        return _SPARK.catalog.tableExists(name)

    def cursor(self, cursor_factory=None):
        return Cursor(self, dict_rows=cursor_factory is not None)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        if self._db is not None:
            self._db.close()

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()


def connect(*args, **kwargs) -> Connection:
    return Connection()
