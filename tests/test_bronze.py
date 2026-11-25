from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from common.bronze import SOURCES, Source, add_bronze_metadata, autoloader_options


def test_sources_cover_all_generated_tables(tmp_path):
    from data_generator.generate import Sizes, generate

    generate(tmp_path, date(2026, 11, 1), 1, seed=1, sizes=Sizes(50, 50, 20, 20, 20))
    produced = {p.name for p in Path(tmp_path).iterdir() if p.is_dir()}
    assert produced == {s.table for s in SOURCES}


def test_autoloader_options_keep_strings_and_rescue():
    opts = autoloader_options(Source("card_kpi", "csv", {"header": "true", "sep": "\t"}),
                              "/Volumes/c/ops/checkpoints/card_kpi/schema")
    assert opts["cloudFiles.format"] == "csv"
    assert opts["cloudFiles.inferColumnTypes"] == "false"
    assert opts["cloudFiles.schemaEvolutionMode"] == "rescue"
    assert opts["sep"] == "\t"


def test_add_bronze_metadata(spark):
    df = spark.createDataFrame(
        [("1", "/Volumes/c/landing/raw_files/loans/as_of_date=2026-11-02/loans.csv"),
         ("2", "/tmp/no_partition/file.csv")],
        "id string, p string",
    )
    out = add_bronze_metadata(df, F.col("p")).collect()
    assert out[0]["_as_of_date"] == date(2026, 11, 2)
    assert out[1]["_as_of_date"] is None
    assert out[0]["_source_file"].endswith("loans.csv")
    assert out[0]["_load_ts"] is not None
