"""Chạy 1 ngày của pipeline: `run_silver` (bronze -> silver) và `run_gold` (silver -> gold).

Notebook Databricks, `05_backfill` và test end-to-end đều gọi CHÍNH các hàm này, nên code chạy
trong test cũng là code chạy trên Databricks. Mỗi bước là 1 hàm nhỏ, đọc từ trên xuống.

Chạy lại cùng ngày an toàn: silver MERGE theo khóa / SCD2 so hash, quarantine xoá lô cũ;
gold fact MERGE theo khóa, mart ghi đè đúng ngày.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from common import gold as g
from common import gold_cards as gc
from common import gold_deposit as gd
from common.dates import check_date
from common.delta_io import merge_upsert, overwrite, overwrite_date, scd2_upsert
from common.dq import dq_summary, should_stop
from common.governance import assert_pipeline_identity
from common.silver import CUSTOMER_TRACKED, ORDER, TABLES, prepare_silver, quarantine_records


class ReconciliationError(RuntimeError):
    """Số liệu không khớp (bronze vs silver, silver vs gold, gold vs file tổng hợp nguồn)."""


def latest_date(spark: SparkSession, table: str, col: str) -> str:
    return str(spark.table(table).agg(F.max(col)).first()[0])


# ============================================================== SILVER
@dataclass
class SilverResult:
    as_of_date: str
    report: DataFrame   # mỗi bảng: bronze, silver, quarantine, exact_dupes, reconciled


def run_silver(spark: SparkSession, catalog: str, as_of_date: str = "",
               max_fail_pct: float = 5.0) -> SilverResult:
    """Bronze ngày `as_of_date` (trống = ngày mới nhất) -> silver, rồi đối soát số dòng."""
    as_of_date = check_date(as_of_date or latest_date(spark, f"{catalog}.bronze.loans",
                                                      "_as_of_date"))
    assert_pipeline_identity(spark, catalog)   # governance bật -> phải thấy PII
    run_id = str(uuid.uuid4())
    print(f"silver catalog={catalog} as_of_date={as_of_date} run_id={run_id}")

    known: dict[str, DataFrame] = {}   # bảng -> khóa trong lô hôm nay (kể cả bị quarantine)
    report = []
    for name in ORDER:
        spec = TABLES[name]
        bronze = spark.table(f"{catalog}.bronze.{spec.bronze}").filter(
            F.col("_as_of_date") == F.lit(as_of_date).cast("date"))
        n_bronze = bronze.count()
        if n_bronze == 0:
            print(f"{name}: không có dữ liệu ngày {as_of_date}, bỏ qua")
            continue

        ref = _ref_keys(spark, catalog, spec.fk[1], known) if spec.fk else None
        res = prepare_silver(bronze, spec, ref)   # không .cache(): serverless không hỗ trợ
        known[name] = res.known_keys(spec.key[0])

        summary = dq_summary(res.checked, spec.rules, name)
        _write_dq(spark, catalog, name, run_id, summary, bronze, res.quarantine)
        if should_stop(summary, max_fail_pct):
            raise RuntimeError(f"DQ gate: {name} có rule FAIL > {max_fail_pct}% (run_id={run_id})")

        target = f"{catalog}.silver.{name}"
        if spec.write_mode == "scd2":
            scd2_upsert(res.valid, target, spec.key[0], CUSTOMER_TRACKED, as_of_date)
        else:
            merge_upsert(res.valid, target, spec.key, insert_only=spec.write_mode == "append")

        n_valid, n_quar = res.valid.count(), res.quarantine.count()
        report.append((name, n_bronze, n_valid, n_quar, res.exact_dupes,
                       n_bronze == n_valid + n_quar + res.exact_dupes))

    rep = spark.createDataFrame(report, "table string, bronze long, silver long, "
                                        "quarantine long, exact_dupes long, reconciled boolean")
    bad = [r.table for r in rep.collect() if not r.reconciled]
    if bad:
        raise ReconciliationError(f"Đối soát bronze = silver + quarantine lệch: {bad}")
    return SilverResult(as_of_date, rep)


def _ref_keys(spark, catalog: str, table: str, known: dict) -> DataFrame:
    """Khóa cho FK = khóa đã có ở silver (các ngày trước) + khóa trong lô hôm nay."""
    col = TABLES[table].key[0]
    keys = spark.table(f"{catalog}.silver.{table}").select(col)
    if table in known:
        keys = keys.unionByName(known[table])
    return keys.distinct()


def _write_dq(spark, catalog, name, run_id, summary, bronze, quarantine) -> None:
    """Ghi kết quả rule -> ops.dq_results; bản ghi lỗi -> ops.dq_quarantine (xoá lô cũ trước)."""
    summary.select(F.lit(run_id).alias("run_id"), F.current_timestamp().alias("run_ts"), "*") \
        .write.mode("append").saveAsTable(f"{catalog}.ops.dq_results")
    files = [r[0] for r in bronze.select("_source_file").distinct().collect()]
    if files:
        in_list = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
        spark.sql(f"DELETE FROM {catalog}.ops.dq_quarantine "
                  f"WHERE table_name = '{name}' AND _source_file IN ({in_list})")
    quarantine_records(quarantine, name, run_id) \
        .write.mode("append").saveAsTable(f"{catalog}.ops.dq_quarantine")


# ============================================================== GOLD
@dataclass
class GoldResult:
    as_of_date: str
    silver_loan_balance: float
    gold_loan_balance: float
    recon_card: DataFrame
    recon_deposit: DataFrame


def run_gold(spark: SparkSession, catalog: str, as_of_date: str = "") -> GoldResult:
    """Silver ngày `as_of_date` (trống = ngày mới nhất) -> gold, rồi đối soát.

    Có lệch thì raise ReconciliationError (job đỏ) sau khi đã ghi bảng ops.recon_* để tra cứu.
    """
    S = f"{catalog}.silver"
    as_of_date = check_date(as_of_date or latest_date(spark, f"{S}.loan_snapshots", "as_of_date"))
    assert_pipeline_identity(spark, catalog)
    print(f"gold catalog={catalog} as_of_date={as_of_date}")

    dim_product, dim_customer = _build_dimensions(spark, catalog, as_of_date)
    _build_loans(spark, catalog, as_of_date, dim_product, dim_customer)
    card_daily = _build_cards(spark, catalog, as_of_date)
    _build_deposits(spark, catalog, as_of_date, dim_product, dim_customer)
    result = _reconcile(spark, catalog, as_of_date, dim_product, card_daily)

    errors = []
    if result.silver_loan_balance != result.gold_loan_balance:
        errors.append(f"dư nợ silver={result.silver_loan_balance:,.0f} "
                      f"gold={result.gold_loan_balance:,.0f}")
    bad_card = result.recon_card.filter("status NOT IN ('MATCH', 'WITHIN_TOLERANCE')").count()
    if bad_card:
        errors.append(f"KPI thẻ lệch {bad_card} chỉ tiêu (xem ops.recon_card_kpi)")
    bad_dep = result.recon_deposit.filter("status <> 'MATCH'").count()
    if bad_dep:
        errors.append(f"tiền gửi lệch {bad_dep} dòng (xem ops.recon_deposit)")
    if errors:
        raise ReconciliationError(f"Đối soát Gold ngày {as_of_date}: " + "; ".join(errors))
    return result


def _day(as_of_date: str):
    return F.lit(as_of_date).cast("date")


def _build_dimensions(spark, catalog, as_of_date):
    """Dimension nhỏ -> ghi đè toàn bộ."""
    S, G = f"{catalog}.silver", f"{catalog}.gold"
    first = spark.table(f"{S}.loan_snapshots").agg(F.min("as_of_date")).first()[0]
    year = int(as_of_date[:4])
    start = min(str(first), f"{year - 1}-01-01")
    overwrite(g.build_dim_date(spark, start, f"{year + 1}-12-31"), f"{G}.dim_date")
    overwrite(g.build_dim_branch(spark.table(f"{S}.ref_branches")), f"{G}.dim_branch")
    overwrite(g.build_dim_product(spark.table(f"{S}.ref_products")), f"{G}.dim_product")
    overwrite(g.build_dim_customer(spark.table(f"{S}.core_customers")), f"{G}.dim_customer")
    return spark.table(f"{G}.dim_product"), spark.table(f"{G}.dim_customer")


def _accounts_day(spark, catalog, as_of_date) -> DataFrame:
    """Snapshot TK đúng ngày. TK bị quarantine hôm nay vẫn còn dòng cũ trong silver
    -> phải lọc theo _as_of_date, không thì Gold cộng thừa so với file bang1247."""
    return spark.table(f"{catalog}.silver.core_accounts") \
        .filter(F.col("_as_of_date") == _day(as_of_date))


def _build_loans(spark, catalog, as_of_date, dim_product, dim_customer) -> None:
    """Fact khoản vay + giao dịch, mart nợ quá hạn / NPL, customer 360."""
    S, G, day = f"{catalog}.silver", f"{catalog}.gold", _day(as_of_date)
    loans_day = spark.table(f"{S}.loan_snapshots").filter(F.col("as_of_date") == day)
    merge_upsert(g.build_fact_loan_daily(loans_day, dim_customer), f"{G}.loan_fact_daily",
                 g.GOLD_KEYS["loan_fact_daily"])
    txns_day = spark.table(f"{S}.core_transactions").filter(F.col("_as_of_date") == day)
    merge_upsert(g.build_fact_transaction(txns_day, spark.table(f"{S}.core_accounts"),
                                          dim_customer),
                 f"{G}.core_fact_transaction", g.GOLD_KEYS["core_fact_transaction"],
                 insert_only=True)

    fact_loan = spark.table(f"{G}.loan_fact_daily")
    overdue = g.build_mart_loan_overdue_daily(fact_loan.filter(F.col("as_of_date") == day),
                                              dim_product)
    overwrite_date(overdue, f"{G}.loan_mart_overdue_daily", "as_of_date", as_of_date)
    overwrite(g.build_mart_npl_daily(spark.table(f"{G}.loan_mart_overdue_daily")),
              f"{G}.loan_mart_npl_daily")

    acc_day = _accounts_day(spark, catalog, as_of_date)
    overwrite_date(g.build_mart_deposit_daily(acc_day, as_of_date),
                   f"{G}.dep_mart_daily", "as_of_date", as_of_date)
    overwrite(g.build_mart_customer_360(dim_customer, fact_loan, acc_day,
                                        spark.table(f"{G}.core_fact_transaction"), as_of_date),
              f"{G}.cust_mart_360")


def _build_cards(spark, catalog, as_of_date) -> DataFrame:
    """Mart thẻ tín dụng theo ngày / tháng. Trả về KPI ngày để đối soát."""
    S, G, day = f"{catalog}.silver", f"{catalog}.gold", _day(as_of_date)
    cards = spark.table(f"{S}.card_master").filter(F.col("_as_of_date") == day)
    txns = spark.table(f"{S}.card_transactions_hist").filter(F.col("_as_of_date") == day)
    stmts = spark.table(f"{S}.card_statements_hist").filter(F.col("stnd_dt") == day)
    card_daily = gc.build_mart_card_kpi_daily(cards, txns, stmts, as_of_date)
    overwrite_date(card_daily, f"{G}.card_mart_kpi_daily", "as_of_date", as_of_date)
    overwrite_date(gc.build_mart_card_spend_mix(txns, as_of_date), f"{G}.card_mart_spend_mix",
                   "as_of_date", as_of_date)
    overwrite(gc.build_mart_card_kpi_monthly(spark.table(f"{G}.card_mart_kpi_daily"),
                                             spark.table(f"{S}.ref_targets")),
              f"{G}.card_mart_kpi_monthly")
    return card_daily


def _build_deposits(spark, catalog, as_of_date, dim_product, dim_customer) -> None:
    """Mart tiền gửi: xu hướng, kỳ hạn, CASA, TK mới."""
    S, G, day = f"{catalog}.silver", f"{catalog}.gold", _day(as_of_date)
    acc = _accounts_day(spark, catalog, as_of_date)
    fx = spark.table(f"{S}.ref_fx_rates").filter(F.col("rate_date") == day)
    history = (spark.table(f"{G}.dep_mart_trend")
               if spark.catalog.tableExists(f"{G}.dep_mart_trend") else None)
    branches = spark.table(f"{S}.ref_branches")
    overwrite_date(gd.build_mart_deposit_trend(acc, dim_product, fx, branches, as_of_date,
                                               history=history),
                   f"{G}.dep_mart_trend", "as_of_date", as_of_date)
    overwrite_date(gd.build_mart_deposit_by_tenor(acc, dim_product, fx, as_of_date),
                   f"{G}.dep_mart_by_tenor", "as_of_date", as_of_date)
    overwrite_date(gd.build_mart_casa_customer(acc, dim_product, dim_customer.filter("is_current"),
                                               as_of_date),
                   f"{G}.dep_mart_casa_customer", "as_of_date", as_of_date)
    overwrite(gd.build_mart_deposit_new_accounts(spark.table(f"{S}.core_accounts")),
              f"{G}.dep_mart_new_accounts")


def _reconcile(spark, catalog, as_of_date, dim_product, card_daily) -> GoldResult:
    """Gold vs silver (dư nợ) và Gold vs file tổng hợp nguồn -> ops.recon_*."""
    S, G, day = f"{catalog}.silver", f"{catalog}.gold", _day(as_of_date)
    recon_card = gc.reconcile(gc.card_kpi_metrics(card_daily),
                              spark.table(f"{S}.card_kpi_source").filter(F.col("trx_date") == day))
    overwrite_date(recon_card, f"{catalog}.ops.recon_card_kpi", "as_of_date", as_of_date)

    groups = {r.prodcode: r.product_name
              for r in dim_product.filter("product_family = 'DEPOSIT'").collect()}
    recon_dep = gc.deposit_reconciliation(
        spark.table(f"{G}.dep_mart_daily").filter(F.col("as_of_date") == day),
        spark.table(f"{S}.dep_summary_source").filter(F.col("stnd_dt") == day),
        spark.table(f"{S}.ref_fx_rates").filter(F.col("rate_date") == day), groups)
    overwrite_date(recon_dep, f"{catalog}.ops.recon_deposit", "as_of_date", as_of_date)

    silver_bal = spark.table(f"{S}.loan_snapshots").filter(F.col("as_of_date") == day) \
        .agg(F.sum(F.coalesce("ducuoiqd", "ducuoi"))).first()[0] or 0
    gold_bal = spark.table(f"{G}.loan_mart_overdue_daily").filter(F.col("as_of_date") == day) \
        .agg(F.sum("portfolio_balance")).first()[0] or 0
    ops = f"{catalog}.ops"
    return GoldResult(as_of_date, silver_bal, gold_bal,
                      spark.table(f"{ops}.recon_card_kpi").filter(F.col("as_of_date") == day),
                      spark.table(f"{ops}.recon_deposit").filter(F.col("as_of_date") == day))
