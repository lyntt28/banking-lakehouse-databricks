"""ML cross-sell: feature table đúng grain / không rò rỉ nhãn, model học được tín hiệu."""

from datetime import date

import numpy as np
import pytest
from pyspark.sql import functions as F

from common.bronze import SOURCES, add_bronze_metadata
from common.ml_features import (
    NUMERIC_FEATURES,
    build_customer_features,
    build_labels,
    lift_at,
    train_model,
    training_set,
)
from common.silver import TABLES, prepare_silver
from data_generator.generate import Sizes, generate

AS_OF = "2026-11-01"
NEED = ["core_customers", "core_accounts", "loan_snapshots", "card_master",
        "card_transactions_hist", "ins_policies", "ref_fx_rates"]


@pytest.fixture(scope="module")
def silver(spark, tmp_path_factory):
    out = tmp_path_factory.mktemp("ml")
    generate(out, date(2026, 11, 1), 1, seed=11, sizes=Sizes(4000, 3000, 300, 300, 8000))
    srcs = {s.table: s for s in SOURCES}
    res = {}
    for t in NEED:
        s = srcs[TABLES[t].bronze]
        p = str(out / TABLES[t].bronze)
        df = (spark.read.option("primitivesAsString", "true").json(p)
              if s.fmt == "json" else spark.read.options(**s.options).csv(p))
        df = add_bronze_metadata(df, F.input_file_name())
        spec = TABLES[t]
        ref = None
        if spec.fk:
            ref = res[spec.fk[1]].known_keys(TABLES[spec.fk[1]].key[0])
        res[t] = prepare_silver(df, spec, ref)
    return {t: r.valid for t, r in res.items()}


@pytest.fixture(scope="module")
def features(silver):
    s = silver
    return build_customer_features(AS_OF, s["core_customers"], s["core_accounts"],
                                   s["loan_snapshots"], s["card_master"],
                                   s["card_transactions_hist"], s["ref_fx_rates"]).cache()


def test_feature_grain_and_columns(features, silver):
    assert features.groupBy("customer_id").count().filter("count > 1").count() == 0
    n_cn = silver["core_customers"].filter("cust_type = 'CN'").count()
    assert features.count() == n_cn
    assert set(NUMERIC_FEATURES) <= set(features.columns)
    # không có cột lấy từ bảng bảo hiểm (tránh rò rỉ nhãn)
    assert not [c for c in features.columns if "polic" in c or "premium" in c]


def test_features_have_no_nulls_except_profile(features):
    nullable = {"age", "log_income", "tenure_years"}
    for c in NUMERIC_FEATURES:
        if c not in nullable:
            assert features.filter(F.col(c).isNull()).count() == 0, c


def test_labels_modes(spark):
    p = spark.createDataFrame(
        [("P1", "A", date(2026, 1, 1), "INFORCE"), ("P2", "B", date(2026, 12, 1), "INFORCE"),
         ("P3", "C", date(2026, 1, 1), "LAPSED"), ("P4", None, date(2026, 1, 1), "INFORCE")],
        "policy_id string, customer_id string, start_date date, status string")
    holder = {r.customer_id for r in build_labels(p, AS_OF, "holder").collect()}
    future = {r.customer_id for r in build_labels(p, AS_OF, "future", 60).collect()}
    assert holder == {"A"} and future == {"B"}
    f = spark.createDataFrame([("A",), ("B",), ("C",)], "customer_id string")
    ts = training_set(f, build_labels(p, AS_OF, "future", 60), p, AS_OF, "future")
    assert {(r.customer_id, r.label) for r in ts.collect()} == {("B", 1)}   # A, C đã có HĐ


def test_model_learns_signal(features, silver):
    ts = training_set(features, build_labels(silver["ins_policies"], AS_OF))
    pdf = ts.toPandas()
    assert 0.02 < pdf["label"].mean() < 0.5
    _, m = train_model(pdf)
    assert m["roc_auc"] > 0.65, m
    assert m["lift_top10"] > 1.5, m


def test_lift_at():
    y = np.array([1, 1, 0, 0, 0, 0, 0, 0, 0, 0])
    s = np.arange(10, 0, -1).astype(float)
    assert lift_at(y, s, 0.2) == pytest.approx(5.0)
