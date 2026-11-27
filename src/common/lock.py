"""Khoá ghi (writer lock) dùng chung cho MỌI job ghi Silver / Gold / bảo trì.

`max_concurrent_runs: 1` chỉ chặn 2 lần chạy của CÙNG một job. Job daily, backfill, bảo trì
(OPTIMIZE / VACUUM) và chạy tay notebook Gold vẫn có thể ghi đồng thời -> RESTORE của rollback
có thể xoá dữ liệu job khác vừa ghi, VACUUM có thể xoá file mà RESTORE cần.

Cách làm: bảng `ops.writer_lock`, 1 dòng / tên khoá. Lấy khoá = 1 MERGE (1 commit Delta):
    - chưa có dòng            -> INSERT (mình giữ khoá)
    - có dòng nhưng hết hạn   -> UPDATE sang mình (job trước chết không nhả khoá)
    - có dòng còn hạn          -> không đổi -> đọc lại thấy chủ khác -> WriterBusy
Hai job MERGE cùng lúc: Delta optimistic concurrency cho 1 commit thắng, commit kia lỗi xung
đột -> cũng là WriterBusy. Nhả khoá = DELETE dòng của chính mình.

Re-entrant trong cùng process: daily -> silver.run / gold.run không tự khoá lại chính mình.
"""

from __future__ import annotations

import contextlib
import uuid

from common.names import fq

LOCK_NAME = "lakehouse_writer"
DEFAULT_TTL_HOURS = 6     # > timeout dài nhất (backfill 4h); job chết -> khoá tự hết hạn

LOCK_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  lock_name STRING,
  owner STRING,
  job STRING,
  acquired_at TIMESTAMP,
  expires_at TIMESTAMP
) USING DELTA
"""

_held: dict[tuple[str, str], int] = {}   # (catalog, lock_name) -> độ sâu re-entrant


class WriterBusy(RuntimeError):
    """Job ghi khác đang giữ khoá: dừng, không ghi chồng (runbook: chờ / kiểm tra khoá treo)."""


def _table(catalog: str) -> str:
    return fq(catalog, "ops", "writer_lock")


def acquire(spark, catalog: str, job: str, ttl_hours: int = DEFAULT_TTL_HOURS,
            lock_name: str = LOCK_NAME) -> str:
    t = _table(catalog)
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq(catalog, 'ops')}")
    spark.sql(LOCK_DDL.format(table=t))
    me = f"{job}:{uuid.uuid4()}"
    src = (f"SELECT '{lock_name}' AS lock_name, '{me}' AS owner, '{job}' AS job, "
           f"current_timestamp() AS acquired_at, "
           f"current_timestamp() + INTERVAL {int(ttl_hours)} HOURS AS expires_at")
    try:
        spark.sql(f"MERGE INTO {t} t USING ({src}) s ON t.lock_name = s.lock_name "
                  "WHEN MATCHED AND t.expires_at < current_timestamp() THEN UPDATE SET * "
                  "WHEN NOT MATCHED THEN INSERT *")
    except Exception as e:  # xung đột commit Delta: job khác vừa lấy khoá
        if "Concurrent" in type(e).__name__ or "Concurrent" in str(e):
            raise WriterBusy(f"Job khác vừa lấy khoá {lock_name}: {e}") from e
        raise
    row = spark.sql(f"SELECT owner, job, expires_at FROM {t} "
                    f"WHERE lock_name = '{lock_name}'").first()
    if row is None or row.owner != me:
        raise WriterBusy(f"Khoá {lock_name} đang do job '{row.job if row else '?'}' giữ tới "
                         f"{row.expires_at if row else '?'} (ops.writer_lock). Chờ job đó xong; "
                         "khoá treo do job chết -> xem docs/incident_runbook.md")
    return me


def release(spark, catalog: str, owner: str, lock_name: str = LOCK_NAME) -> None:
    spark.sql(f"DELETE FROM {_table(catalog)} WHERE lock_name = '{lock_name}' "
              f"AND owner = '{owner}'")


@contextlib.contextmanager
def writer_lock(spark, catalog: str, job: str, ttl_hours: int = DEFAULT_TTL_HOURS,
                lock_name: str = LOCK_NAME):
    key = (catalog, lock_name)
    if _held.get(key):
        _held[key] += 1
        try:
            yield
        finally:
            _held[key] -= 1
        return
    owner = acquire(spark, catalog, job, ttl_hours, lock_name)
    _held[key] = 1
    try:
        yield
    finally:
        _held.pop(key, None)
        release(spark, catalog, owner, lock_name)

