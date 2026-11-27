"""Khoá ghi dùng chung: job thứ 2 (bảo trì / backfill / chạy tay) không ghi chồng job đang chạy."""

import pytest

from common import lock


@pytest.fixture
def ops(spark):
    spark.sql("DROP SCHEMA IF EXISTS ops CASCADE")
    spark.sql("CREATE SCHEMA ops")
    yield
    spark.sql("DROP SCHEMA IF EXISTS ops CASCADE")


def test_second_writer_is_rejected_until_release(spark, ops):
    owner = lock.acquire(spark, "", "daily")
    with pytest.raises(lock.WriterBusy, match="daily"):
        lock.acquire(spark, "", "maintenance")
    lock.release(spark, "", owner)
    lock.release(spark, "", lock.acquire(spark, "", "maintenance"))


def test_expired_lock_is_taken_over(spark, ops):
    lock.acquire(spark, "", "daily")      # job chết, không nhả khoá
    spark.sql("UPDATE ops.writer_lock SET expires_at = current_timestamp() - INTERVAL 1 HOURS")
    owner = lock.acquire(spark, "", "maintenance")
    assert spark.table("ops.writer_lock").first().owner == owner


def test_context_manager_is_reentrant_and_releases_on_error(spark, ops):
    with pytest.raises(ValueError):
        with lock.writer_lock(spark, "", "daily"):
            with lock.writer_lock(spark, "", "gold"):     # daily -> gold.run: không tự chặn
                assert spark.table("ops.writer_lock").count() == 1
            raise ValueError("lỗi trong job")
    assert spark.table("ops.writer_lock").count() == 0
