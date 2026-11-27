"""Khóa ghi dùng chung: chỉ 1 job ghi lakehouse tại một thời điểm."""

import pytest

from common.writer_lock import WriterBusy, acquire, holder, release, writer_lock


@pytest.fixture
def clean(spark):
    spark.sql("DROP SCHEMA IF EXISTS ops CASCADE")
    yield
    spark.sql("DROP SCHEMA IF EXISTS ops CASCADE")


def test_second_writer_is_rejected_until_release(spark, clean):
    with writer_lock(spark, "", "daily") as owner:
        assert holder(spark, "").owner == owner
        with pytest.raises(WriterBusy, match="daily"):
            acquire(spark, "", "other", "maintenance")
        assert holder(spark, "").owner == owner          # khóa của daily không bị cướp
    assert holder(spark, "") is None                     # thoát with -> nhả khóa
    assert acquire(spark, "", "other", "maintenance")


def test_reentrant_for_same_owner(spark, clean):
    # backfill giữ khóa, notebook daily con dùng cùng owner: không tự chặn, không nhả hộ
    with writer_lock(spark, "", "backfill") as owner:
        with writer_lock(spark, "", "daily", owner=owner):
            pass
        assert holder(spark, "").owner == owner
    assert holder(spark, "") is None


def test_lock_released_when_job_fails(spark, clean):
    with pytest.raises(ValueError):
        with writer_lock(spark, "", "daily"):
            raise ValueError("job lỗi")
    assert holder(spark, "") is None


def test_expired_lock_can_be_taken_over(spark, clean):
    assert acquire(spark, "", "dead-job", "daily", ttl_hours=-1)   # job chết, khóa đã hết hạn
    assert acquire(spark, "", "new-job", "daily")
    assert holder(spark, "").owner == "new-job"
    release(spark, "", "dead-job")                                  # không nhả khóa người khác
    assert holder(spark, "").owner == "new-job"
