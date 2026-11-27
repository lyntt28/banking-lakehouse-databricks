"""Khóa ghi dùng chung cho MỌI job ghi Silver / Gold / bảo trì (một writer tại một thời điểm).

`max_concurrent_runs: 1` chỉ chặn 2 lần chạy của CÙNG một job. Không chặn daily chạy cùng lúc
với maintenance (OPTIMIZE / VACUUM / CLUSTER BY), backfill, hay notebook Silver / Gold chạy tay
-> 2 writer cùng ghi Gold, VACUUM xoá file mà rollback (RESTORE) cần, watermark đọc lệch nhau.

Cách làm: 1 dòng trong `ops.writer_lock` (name = 'lakehouse'). Lấy khóa = MERGE chỉ chèn khi
chưa có ai giữ (hoặc khóa đã hết hạn). Delta optimistic concurrency: 2 job MERGE cùng lúc thì
1 commit thắng, job kia bị conflict -> đọc lại thấy người khác giữ -> WriterBusy (dừng, không
ghi gì). Khóa có hạn (`ttl_hours`) để job chết giữa chừng không khóa vĩnh viễn.

Re-entrant theo `owner`: backfill giữ khóa rồi gọi notebook daily với cùng owner.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager

from common.names import fq

LOCK_NAME = "lakehouse"
DEFAULT_TTL_HOURS = 8      # > timeout dài nhất (backfill 4h) để khóa không hết hạn khi đang chạy

LOCK_DDL = ("CREATE TABLE IF NOT EXISTS {table} (name STRING, owner STRING, job STRING, "
            "acquired_at TIMESTAMP, expires_at TIMESTAMP) USING DELTA")


class WriterBusy(RuntimeError):
    """Một job khác đang ghi: dừng, không ghi gì (lần chạy sau / chạy lại sau)."""


def _table(catalog: str) -> str:
    return fq(catalog, "ops", "writer_lock")


def holder(spark, catalog: str):
    """Dòng khóa hiện tại (None nếu không ai giữ)."""
    rows = spark.table(_table(catalog)).filter(f"name = '{LOCK_NAME}'").collect()
    return rows[0] if rows else None


def acquire(spark, catalog: str, owner: str, job: str,
            ttl_hours: float = DEFAULT_TTL_HOURS) -> bool:
    """Lấy khóa. True = vừa lấy (phải release), False = owner này đã giữ sẵn (re-entrant).
    Người khác đang giữ (chưa hết hạn) -> WriterBusy."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq(catalog, 'ops')}")
    table = _table(catalog)
    spark.sql(LOCK_DDL.format(table=table))
    cur = holder(spark, catalog)
    if cur is not None and cur.owner == owner:
        return False
    secs = int(ttl_hours * 3600)
    try:
        spark.sql(
            f"MERGE INTO {table} t USING (SELECT '{LOCK_NAME}' AS name, '{owner}' AS owner, "
            f"'{job}' AS job, current_timestamp() AS acquired_at, "
            f"current_timestamp() + make_interval(0, 0, 0, 0, 0, 0, {secs}) AS expires_at) s "
            "ON t.name = s.name "
            "WHEN MATCHED AND t.expires_at < current_timestamp() THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *")
        conflict = None
    except Exception as e:  # noqa: BLE001 - commit đồng thời bị Delta từ chối
        conflict = e
    cur = holder(spark, catalog)
    if cur is None or cur.owner != owner:
        who = f"{cur.job} (owner {cur.owner}, tới {cur.expires_at})" if cur else "?"
        raise WriterBusy(f"{job}: job khác đang ghi lakehouse: {who}") from conflict
    return True


def release(spark, catalog: str, owner: str) -> None:
    spark.sql(f"DELETE FROM {_table(catalog)} WHERE name = '{LOCK_NAME}' AND owner = '{owner}'")


@contextmanager
def writer_lock(spark, catalog: str, job: str, owner: str | None = None,
                ttl_hours: float = DEFAULT_TTL_HOURS):
    """with writer_lock(spark, catalog, "daily") as owner: ...  (owner truyền cho notebook con)"""
    owner = owner or str(uuid.uuid4())
    new = acquire(spark, catalog, owner, job, ttl_hours)
    try:
        yield owner
    finally:
        if new:
            release(spark, catalog, owner)
