import pytest

from common.dates import check_date


def test_valid_date():
    assert check_date("2026-11-01") == "2026-11-01"
    assert check_date(" 2026-11-01 ") == "2026-11-01"


@pytest.mark.parametrize("bad", ["", "2026-13-01", "01/11/2026", "20261101",
                                 "2026-11-01' OR 1=1 --"])
def test_invalid_date(bad):
    with pytest.raises(ValueError):
        check_date(bad)
