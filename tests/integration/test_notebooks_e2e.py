"""End-to-end: chạy CHÍNH các notebook Silver + Gold qua nhiều ngày liên tiếp, như 05_backfill.

Bắt các lỗi chỉ lộ ra từ ngày thứ 2 trở đi (MERGE / SCD2 có lịch sử, đối soát với file nguồn).
Notebook chạy thật trên Spark + Delta local; chỉ giả lập `dbutils` và `display`.
Bronze được nạp thẳng từ file (Auto Loader không chạy local).
"""

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import functions as F

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
CATALOG = "spark_catalog"
DAYS = ["2026-10-31", "2026-11-02", "2026-11-03"]   # cuối tháng + 2 ngày liên tiếp


class _Widgets:
    def __init__(self, args):
        self.args = args

    def text(self, name, default, *_):
        self.args.setdefault(name, default)

    def dropdown(self, name, default, *_):
        self.args.setdefault(name, default)

    def get(self, name):
        return self.args[name]


class _TaskValues:
    def get(self, taskKey, key, default=None, debugValue=None):  # noqa: N803 (API Databricks)
        return debugValue

    def set(self, key, value):
        raise RuntimeError("chỉ set được trong Job task")   # giống dbutils.notebook.run


class FakeDbutils:
    def __init__(self, args):
        self.widgets = _Widgets(dict(args))
        self.jobs = type("Jobs", (), {"taskValues": _TaskValues()})()


def run_notebook(spark, rel_path: str, **args):
    src = (ROOT / rel_path).read_text(encoding="utf-8")
    env = {"spark": spark, "dbutils": FakeDbutils(args), "display": lambda *a, **k: None,
           "__name__": "__notebook__"}
    exec(compile(src, rel_path, "exec"), env)   # noqa: S102 - chạy notebook nguồn của repo


class _SetupSpark:
    """Chạy notebook setup local: bỏ lệnh chỉ có trên Unity Catalog (CATALOG / VOLUME);
    CREATE TABLE không ghi USING -> thêm USING DELTA (mặc định của Databricks)."""

    def __init__(self, spark):
        self._spark = spark

    def sql(self, q):
        if "CATALOG" in q or "VOLUME" in q:
            return self._spark.sql("SELECT 1")
        if q.lstrip().upper().startswith("CREATE TABLE") and "USING" not in q.upper():
            q = q.rstrip() + " USING DELTA"
        return self._spark.sql(q)

    def __getattr__(self, name):
        return getattr(self._spark, name)


@pytest.fixture(scope="module", autouse=True)
def _delta_three_part_names():
    """delta-spark local chưa parse `spark_catalog.schema.table` trong DeltaTable.forName."""
    from delta.tables import DeltaTable

    orig = DeltaTable.forName.__func__

    def for_name(cls, spark, name):
        return orig(cls, spark, name.removeprefix(f"{CATALOG}."))

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(DeltaTable, "forName", classmethod(for_name))
        yield


@pytest.fixture(scope="module")
def pipeline(spark, tmp_path_factory):
    from common.bronze import SOURCES, add_bronze_metadata
    from data_generator.generate import Sizes, generate

    spark.conf.set("spark.sql.sources.default", "delta")   # như Databricks
    spark.conf.set("spark.databricks.delta.snapshotPartitions", "1")   # nhanh hơn khi local
    for schema in ["bronze", "silver", "silver_stage", "gold", "gold_stage", "ops"]:
        spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")
    run_notebook(_SetupSpark(spark), "src/00_setup/01_create_catalog_schema.py", catalog=CATALOG)

    out = tmp_path_factory.mktemp("e2e")
    generate(out, date(2026, 11, 2), 2, seed=21,   # -> 2026-10-31, 11-02, 11-03
             sizes=Sizes(800, 1000, 300, 800, 300), history_months=1)
    for s in SOURCES:
        path = str(out / s.table)
        df = (spark.read.option("primitivesAsString", "true").json(path) if s.fmt == "json"
              else spark.read.options(**s.options).csv(path))
        add_bronze_metadata(df, F.input_file_name()) \
            .write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.{s.table}")

    days = [str(r[0]) for r in spark.table(f"{CATALOG}.bronze.loans")
            .select("_as_of_date").distinct().orderBy("_as_of_date").collect()]
    assert days == DAYS
    for d in days:   # giống vòng lặp trong 05_backfill
        run_notebook(spark, "src/02_silver/01_silver_transform.py", catalog=CATALOG, as_of_date=d)
        run_notebook(spark, "src/03_gold/01_gold_build.py", catalog=CATALOG, as_of_date=d)
    return spark


def test_backfill_runs_every_day(pipeline):
    # Gold tự assert đối soát (dư nợ, KPI thẻ, tiền gửi) -> tới đây là mọi ngày đều qua
    got = {str(r[0]) for r in pipeline.table(f"{CATALOG}.gold.loan_mart_npl_daily")
           .select("as_of_date").collect()}
    assert got == set(DAYS)


@pytest.mark.parametrize("table", ["recon_deposit", "recon_card_kpi"])
def test_recon_matches_every_day(pipeline, table):
    r = pipeline.table(f"{CATALOG}.ops.{table}")
    assert {str(x[0]) for x in r.select("as_of_date").distinct().collect()} == set(DAYS)
    bad = r.filter("status NOT IN ('MATCH', 'WITHIN_TOLERANCE')")
    assert bad.count() == 0, bad.limit(5).collect()


def test_rerun_last_day_is_idempotent(pipeline):
    def counts():
        return {t: pipeline.table(f"{CATALOG}.{t}").count()
                for t in ["silver.core_customers", "silver.core_accounts",
                          "gold_serving.loan_fact_daily", "gold_serving.dep_mart_daily"]}

    before = counts()
    d = DAYS[-1]
    run_notebook(pipeline, "src/02_silver/01_silver_transform.py", catalog=CATALOG, as_of_date=d)
    run_notebook(pipeline, "src/03_gold/01_gold_build.py", catalog=CATALOG, as_of_date=d)
    assert counts() == before
