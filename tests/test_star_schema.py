"""Star schema Gold: grain duy nhất, khoá ngoại khớp dimension, mart đọc từ fact = đọc Silver."""

import pytest
from pyspark.sql import functions as F

from common import gold as g
from common import gold_cards as gc
from common import gold_deposit as gd

AS_OF = "2026-11-01"


@pytest.fixture(scope="module")
def star(results):
    r = {t: x.valid for t, x in results.items()}
    cust = r["core_customers"].withColumn("valid_from", F.lit(AS_OF).cast("date")) \
        .withColumn("valid_to", F.lit(None).cast("date")).withColumn("is_current", F.lit(True))
    dim_customer = g.build_dim_customer(cust)
    cards, stm, txns = r["card_master"], r["card_statements_hist"], r["card_transactions_hist"]
    out = {
        "r": r,
        "dim_customer": dim_customer, "dim_branch": g.build_dim_branch(r["ref_branches"]),
        "dim_product": g.build_dim_product(r["ref_products"]), "dim_card": g.build_dim_card(cards),
        "dim_date": None,
        "fact_deposit_daily": g.build_fact_deposit_daily(r["core_accounts"], r["ref_fx_rates"],
                                                         dim_customer, AS_OF),
        "fact_card_daily": g.build_fact_card_daily(cards, stm, dim_customer, AS_OF),
        "fact_card_transaction": g.build_fact_card_transaction(txns, cards, dim_customer),
    }
    return {k: (v.localCheckpoint() if hasattr(v, "localCheckpoint") else v)
            for k, v in out.items()}


@pytest.mark.parametrize("fact", ["fact_deposit_daily", "fact_card_daily",
                                  "fact_card_transaction", "dim_card"])
def test_grain_unique(star, fact):
    key = g.GOLD_KEYS[fact]
    df = star[fact]
    assert df.count() > 0
    assert df.groupBy(*key).count().filter("count > 1").count() == 0


@pytest.mark.parametrize("fact", ["fact_deposit_daily", "fact_card_daily",
                                  "fact_card_transaction"])
def test_foreign_keys_resolve(star, fact):
    f = star[fact]
    # customer_sk NULL chỉ khi KH KHÔNG có trong dim_customer (KH bị quarantine ở Silver vì lỗi
    # thuộc tính nhưng vẫn là khoá FK hợp lệ của TK / thẻ) - không được NULL vì join sai
    miss = f.filter("customer_id IS NOT NULL AND customer_sk IS NULL").select("customer_id")
    known = star["dim_customer"].select("customer_id")
    assert miss.join(known, "customer_id").count() == 0
    sk = star["dim_customer"].select("customer_sk")
    assert f.filter("customer_sk IS NOT NULL").join(sk, "customer_sk", "left_anti").count() == 0
    br = star["dim_branch"].select("branchnbr")
    assert f.filter("branchnbr IS NOT NULL").join(br, "branchnbr", "left_anti").count() == 0
    assert f.filter("date_key IS NULL").count() == 0
    if "card_no" in f.columns and fact != "dim_card":
        assert f.join(star["dim_card"].select("card_no"), "card_no", "left_anti").count() == 0
    if fact == "fact_deposit_daily":
        p = star["dim_product"].select(F.col("prodcode").alias("product_type"))
        assert f.join(p, "product_type", "left_anti").count() == 0
        assert f.filter("product_type = 'LOAN'").count() == 0


def _same(a, b, keys):
    cols = sorted(set(a.columns) & set(b.columns))
    x, y = a.select(*cols), b.select(*cols)
    assert x.count() == y.count()
    assert x.exceptAll(y).count() == 0 and y.exceptAll(x).count() == 0, keys


def test_card_kpi_mart_from_facts_equals_from_silver(star):
    r = star["r"]
    old = gc.build_mart_card_kpi_daily(r["card_master"], r["card_transactions_hist"],
                                       r["card_statements_hist"], AS_OF)
    fc, ft = star["fact_card_daily"], star["fact_card_transaction"]
    new = gc.build_mart_card_kpi_daily(
        fc, ft.drop("branchnbr", "customer_id", "customer_sk"),
        fc.filter(F.col("outstanding_balance").isNotNull())
        .select("card_no", "outstanding_balance", "revolving_balance", "dpd"), AS_OF)
    _same(old, new, gc.DIMS)


def test_deposit_marts_from_fact_equal_from_silver(star):
    r = star["r"]
    acc, fd = r["core_accounts"], star["fact_deposit_daily"]
    _same(g.build_mart_deposit_daily(acc, AS_OF), g.build_mart_deposit_daily(fd, AS_OF),
          ["branchnbr", "product_type"])
    p, fx, b = star["dim_product"], r["ref_fx_rates"], r["ref_branches"]
    _same(gd.build_mart_deposit_by_tenor(acc, p, fx, AS_OF, b),
          gd.build_mart_deposit_by_tenor(fd, p, fx, AS_OF, b), ["tenor"])
    _same(gd.build_mart_deposit_trend(acc, p, fx, b, AS_OF),
          gd.build_mart_deposit_trend(fd, p, fx, b, AS_OF), gd.DIMS)


def test_balance_vnd_reconciles_with_fx(star):
    fd = star["fact_deposit_daily"]
    usd = fd.filter("currencycd = 'USD'").first()
    rate = star["r"]["ref_fx_rates"].filter("currencycd = 'USD'").first().rate_to_vnd
    assert float(usd.balance_vnd) == pytest.approx(float(usd.balance) * float(rate))
