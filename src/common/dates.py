"""Kiểm tra tham số ngày trước khi chèn vào câu SQL (widget / tham số job là chuỗi tự do)."""

from __future__ import annotations

import datetime as dt


def check_date(value: str) -> str:
    """Trả về 'YYYY-MM-DD' nếu hợp lệ, ngược lại ValueError. Chặn chuỗi lạ lọt vào SQL."""
    s = str(value).strip()
    try:
        d = dt.date.fromisoformat(s)
    except ValueError as e:
        raise ValueError(f"Ngày không hợp lệ (cần YYYY-MM-DD): {value!r}") from e
    if d.isoformat() != s:
        raise ValueError(f"Ngày không hợp lệ (cần YYYY-MM-DD): {value!r}")
    return s
