"""Chạy song song các nhánh độc lập trên cùng SparkSession (luồng Python).

Dùng cho Gold publish: các nhánh (loan, deposit, card) ghi các bảng KHÁC NHAU nên không tranh
commit Delta. Đợi MỌI nhánh xong rồi mới báo lỗi, để không còn luồng nào ghi tiếp trong lúc
pipeline đã chuyển sang rollback.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor


class BranchFailed(RuntimeError):
    def __init__(self, errors: dict[str, BaseException]):
        self.errors = errors
        super().__init__("; ".join(f"{k}: {type(e).__name__}: {e}" for k, e in errors.items()))


def run_branches(branches: dict[str, Callable[[], None]], parallel: bool = True,
                 max_workers: int | None = None) -> None:
    """Chạy từng nhánh; parallel=False chạy tuần tự (debug / máy nhỏ).
    Sau khi MỌI nhánh dừng: 1 nhánh lỗi -> raise đúng exception gốc (giữ loại lỗi cho xử lý
    phía trên, vd LockLost); nhiều nhánh lỗi -> BranchFailed (`.errors` = lỗi từng nhánh)."""
    errors: dict[str, BaseException] = {}
    if not parallel or len(branches) < 2:
        for name, fn in branches.items():
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                errors[name] = e
                break       # tuần tự: dừng ở nhánh lỗi đầu tiên như trước
    else:
        with ThreadPoolExecutor(max_workers=max_workers or len(branches),
                                thread_name_prefix="gold") as pool:
            futures = {name: pool.submit(fn) for name, fn in branches.items()}
        for name, f in futures.items():      # pool đã đóng = mọi nhánh đã xong
            if f.exception() is not None:
                errors[name] = f.exception()
    if len(errors) == 1:
        raise next(iter(errors.values()))
    if errors:
        raise BranchFailed(errors) from next(iter(errors.values()))
