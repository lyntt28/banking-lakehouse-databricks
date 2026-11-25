import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")  # nhiều fixture + plan lớn khi chạy cả bộ test
        # Delta Lake local: test MERGE / SCD2 / replaceWhere như trên Databricks
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


# ---- dữ liệu mẫu 1 ngày đi qua Bronze -> Silver (dùng chung cho test_silver / test_gold)
SMALL_SIZES = dict(customers=600, accounts=800, loans=400, txns_per_day=1500,
                   card_txns_per_day=50)


@pytest.fixture(scope="session")
def bronze(spark, tmp_path_factory):
    """dict table -> DataFrame giống bảng bronze (string + cột kỹ thuật)."""
    from datetime import date

    from pyspark.sql import functions as F

    from common.bronze import SOURCES, add_bronze_metadata
    from common.silver import ORDER
    from data_generator.generate import Sizes, generate

    out = tmp_path_factory.mktemp("landing")
    generate(out, date(2026, 11, 1), 1, seed=7, sizes=Sizes(**SMALL_SIZES))
    srcs = {s.table: s for s in SOURCES}
    res = {}
    for t in ORDER:
        s = srcs[t]
        path = str(out / t)
        if s.fmt == "json":
            df = spark.read.option("primitivesAsString", "true").json(path)
        else:
            df = spark.read.options(**s.options).csv(path)  # không inferSchema -> string
        res[t] = add_bronze_metadata(df, F.input_file_name())
    return res


@pytest.fixture(scope="session")
def results(bronze):
    from common.silver import ORDER, TABLES, prepare_silver

    out = {}
    for t in ORDER:   # bảng tham chiếu chạy trước bảng có khóa ngoại
        spec = TABLES[t]
        ref = None
        if spec.fk:
            parent = spec.fk[1]
            ref = out[parent].known_keys(TABLES[parent].key[0])
        out[t] = prepare_silver(bronze[t], spec, ref)
    return out
