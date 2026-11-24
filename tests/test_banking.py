import pytest
from pyspark.sql import functions as F

from common.banking import debt_group_col, debt_group_from_dpd


@pytest.mark.parametrize(
    "dpd, expected",
    [(0, 1), (9, 1), (10, 2), (89, 2), (90, 3), (179, 3), (180, 4), (359, 4), (360, 5), (1000, 5)],
)
def test_debt_group_boundaries(dpd, expected):
    assert debt_group_from_dpd(dpd) == expected


def test_debt_group_none_and_negative():
    assert debt_group_from_dpd(None) is None
    with pytest.raises(ValueError):
        debt_group_from_dpd(-1)


def test_debt_group_spark_matches_python(spark):
    values = [0, 9, 10, 89, 90, 179, 180, 359, 360, None]
    df = spark.createDataFrame([(v,) for v in values], "dpd int")
    result = df.withColumn("g", debt_group_col(F.col("dpd"))).collect()
    for row in result:
        assert row.g == debt_group_from_dpd(row.dpd)
