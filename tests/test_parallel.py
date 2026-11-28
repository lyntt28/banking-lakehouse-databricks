"""run_branches: chạy song song thật, đợi mọi nhánh xong rồi mới báo lỗi."""

import threading
import time

import pytest

from common.parallel import BranchFailed, run_branches


def test_branches_run_concurrently():
    barrier = threading.Barrier(3, timeout=5)   # chỉ qua được nếu 3 nhánh chạy cùng lúc
    done = []

    def branch(name):
        def fn():
            barrier.wait()
            done.append(name)
        return fn

    run_branches({n: branch(n) for n in "abc"})
    assert sorted(done) == ["a", "b", "c"]


def test_failure_waits_for_other_branches_then_raises():
    finished = []

    def slow():
        time.sleep(0.3)
        finished.append("slow")

    def bad():
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):        # 1 nhánh lỗi: giữ nguyên loại lỗi
        run_branches({"slow": slow, "bad": bad})
    assert finished == ["slow"]                  # nhánh còn lại đã chạy xong trước khi raise

    def bad2():
        raise OSError("disk")

    with pytest.raises(BranchFailed) as e:              # nhiều nhánh lỗi: gom lại
        run_branches({"bad": bad, "bad2": bad2})
    assert set(e.value.errors) == {"bad", "bad2"}


def test_sequential_mode_stops_at_first_error():
    ran = []

    def ok():
        ran.append("ok")

    def bad():
        raise RuntimeError("x")

    with pytest.raises(RuntimeError):
        run_branches({"bad": bad, "ok": ok}, parallel=False)
    assert ran == []
