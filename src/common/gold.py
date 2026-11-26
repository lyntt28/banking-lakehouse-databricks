"""Gold: star schema (Kimball) + mart cho dashboard, tính từ bảng Silver.

Dimension
    dim_date        1 dòng / ngày (date_key = yyyyMMdd)
    dim_branch      chi nhánh
    dim_product     sản phẩm vay / tiền gửi
    dim_customer    lịch sử SCD2 từ silver.core_customers, khóa thay thế customer_sk
Fact
    loan_fact_daily     snapshot dư nợ theo ngày x khoản vay (grain: as_of_date, acctnbr)
    core_fact_transaction    giao dịch tài khoản (grain: txn_id)
Mart (mỗi visual = 1 SELECT đơn giản, xem docs/gold_dashboards.md)
    loan_mart_overdue_daily     dư nợ / nợ quá hạn theo ngày x chi nhánh x sản phẩm x nhóm nợ
    loan_mart_npl_daily              tỷ lệ nợ xấu toàn hàng, kèm so sánh D-1 / M-1 / Y-1
    dep_mart_daily          số dư tiền gửi theo ngày x chi nhánh x sản phẩm x tiền tệ
    cust_mart_360           1 dòng / khách hàng hiện hành: vay, tiền gửi, giao dịch

Hàm ở đây thuần Spark (test local); notebook src/03_gold ghi ra Delta.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def date_key(c) -> F.Column:
    return F.date_format(c, "yyyyMMdd").cast("int")


# ---------------------------------------------------------------- dimension
def build_dim_date(spark: SparkSession, start: str, end: str) -> DataFrame:
    d = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}')) AS date")
    return d.select(
        date_key("date").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.date_format("date", "yyyy-MM").alias("year_month"),
        F.dayofweek("date").alias("day_of_week"),          # 1 = Chủ nhật
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        (F.col("date") == F.last_day("date")).alias("is_month_end"),
        (F.date_format("date", "MM-dd") == "12-31").alias("is_year_end"),
    )


def build_dim_branch(branches: DataFrame) -> DataFrame:
    return branches.select("branchnbr", "br_name", "br_area", "region", "province", "open_date")


def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select("prodcode", "product_name", "product_family", "product_group",
                           "scope", "mjacctcd", "miacctcd", "term_months")


CUSTOMER_COLS = ["customer_id", "cust_type", "full_name", "id_no", "dob", "gender", "phone",
                 "email", "address", "province", "branchnbr", "segment", "monthly_income",
                 "open_date", "nationality"]


def build_dim_customer(customers_scd2: DataFrame) -> DataFrame:
    """silver.core_customers (SCD2) -> dim_customer có khóa thay thế ổn định giữa các lần chạy."""
    return customers_scd2.select(
        F.xxhash64("customer_id", "valid_from").alias("customer_sk"),
        *CUSTOMER_COLS, "valid_from", "valid_to", "is_current",
    )


def _as_of_join(fact: DataFrame, dim_customer: DataFrame, date_col: str) -> DataFrame:
    """Gắn customer_sk của phiên bản khách hàng có hiệu lực tại ngày date_col."""
    d = dim_customer.select(
        F.col("customer_id").alias("_cid"), "customer_sk", "valid_from", "valid_to"
    )
    cond = (
        (fact["customer_id"] == d["_cid"])
        & (d["valid_from"] <= fact[date_col])
        & (d["valid_to"].isNull() | (d["valid_to"] >= fact[date_col]))
    )
    return fact.join(d, cond, "left").drop("_cid", "valid_from", "valid_to")


# ---------------------------------------------------------------- fact
def build_fact_loan_daily(loans: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """Grain: as_of_date x acctnbr. balance_vnd = ducuoiqd (đã quy đổi VND)."""
    f = loans.select(
        "as_of_date", "acctnbr", "customer_id", "branchnbr", "prodcode", "scope", "currencycd",
        "int_rate", "datestart", "datemat", "loan_dur",
        F.col("ducuoi").alias("balance"),
        F.coalesce("ducuoiqd", "ducuoi").alias("balance_vnd"),
        F.col("chovay").alias("disbursed"),
        F.col("thuno").alias("principal_repaid"),
        F.col("songay_nqh").alias("dpd"),
        "debt_group", "is_npl",
        (F.col("songay_nqh") > 0).alias("is_overdue"),
    )
    f = _as_of_join(f, dim_customer, "as_of_date")
    return f.withColumn("date_key", date_key("as_of_date"))


def build_fact_transaction(txns: DataFrame, accounts: DataFrame,
                           dim_customer: DataFrame) -> DataFrame:
    """Grain: txn_id. Số tiền có dấu: Có (C) dương, Nợ (D) âm."""
    acc = accounts.select(F.col("account_id").alias("_aid"), "customer_id",
                          F.col("product_type").alias("account_product"))
    f = txns.join(acc, txns["account_id"] == acc["_aid"], "left").drop("_aid")
    f = f.select(
        "txn_id", "account_id", "customer_id", "account_product", "txn_ts", "value_date",
        "amount", F.when(F.col("dr_cr") == "D", -F.col("amount")).otherwise(F.col("amount"))
        .alias("signed_amount"),
        "dr_cr", "currencycd", "channel", "txn_type", "branchnbr", "teller_user",
    )
    f = _as_of_join(f, dim_customer, "value_date")
    return f.withColumn("date_key", date_key("value_date"))


# ---------------------------------------------------------------- mart
def build_mart_loan_overdue_daily(fact_loan: DataFrame, dim_product: DataFrame) -> DataFrame:
    p = dim_product.select("prodcode", "product_group")
    return (
        fact_loan.join(p, "prodcode", "left")
        .groupBy("as_of_date", "branchnbr", "product_group", "scope", "debt_group")
        .agg(
            F.count("*").alias("loan_count"),
            F.sum(F.col("is_overdue").cast("int")).alias("overdue_count"),
            F.sum("balance_vnd").alias("portfolio_balance"),
            F.sum(F.when(F.col("is_overdue"), F.col("balance_vnd")).otherwise(0))
            .alias("overdue_balance"),
            F.sum(F.when(F.col("is_npl"), F.col("balance_vnd")).otherwise(0))
            .alias("npl_balance"),
        )
    )


def _ratio(num: str, den: str) -> F.Column:
    return F.when(F.col(den) > 0, F.col(num) / F.col(den))


def build_mart_npl_daily(mart_overdue: DataFrame) -> DataFrame:
    """Toàn hàng theo ngày + so sánh với D-1, cuối tháng trước (M-1), cuối năm trước (Y-1).

    Ngày so sánh lấy theo ngày có dữ liệu gần nhất <= mốc (bỏ qua ngày nghỉ không có file).
    """
    daily = (
        mart_overdue.groupBy("as_of_date")
        .agg(F.sum("portfolio_balance").alias("portfolio_balance"),
             F.sum("overdue_balance").alias("overdue_balance"),
             F.sum("npl_balance").alias("npl_balance"))
        .withColumn("overdue_ratio", _ratio("overdue_balance", "portfolio_balance"))
        .withColumn("npl_ratio", _ratio("npl_balance", "portfolio_balance"))
    )
    hist = daily.select(F.col("as_of_date").alias("_d"),
                        F.col("overdue_ratio").alias("_or"), F.col("npl_ratio").alias("_nr"),
                        F.col("portfolio_balance").alias("_pb"))
    out = daily.withColumn("_d1", F.date_sub("as_of_date", 1)) \
        .withColumn("_m1", F.last_day(F.add_months("as_of_date", -1))) \
        .withColumn("_y1", F.make_date(F.year("as_of_date") - 1, F.lit(12), F.lit(31)))
    for tag in ["d1", "m1", "y1"]:
        h = hist.alias(f"h_{tag}")
        j = out.join(h, F.col(f"h_{tag}._d") <= F.col(f"_{tag}"), "left")
        w = Window.partitionBy("as_of_date").orderBy(F.col(f"h_{tag}._d").desc_nulls_last())
        out = (
            j.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
            .withColumn(f"npl_ratio_{tag}", F.col(f"h_{tag}._nr"))
            .withColumn(f"overdue_ratio_{tag}", F.col(f"h_{tag}._or"))
            .withColumn(f"portfolio_balance_{tag}", F.col(f"h_{tag}._pb"))
            .drop("_rn", "_d", "_or", "_nr", "_pb")
        )
    return out.drop("_d1", "_m1", "_y1")


def build_mart_deposit_daily(accounts: DataFrame, as_of_date: str) -> DataFrame:
    """Snapshot số dư tiền gửi (không tính TK vay) tại ngày chạy."""
    return (
        accounts.filter(F.col("product_type") != "LOAN")
        .groupBy("branchnbr", "product_type", "currencycd")
        .agg(
            F.count("*").alias("account_count"),
            F.sum(F.col("status").isin("ACTIVE").cast("int")).alias("active_count"),
            F.sum(F.col("status").isin("DORMANT").cast("int")).alias("dormant_count"),
            F.sum("balance").alias("balance"),
            F.avg("int_rate").alias("avg_int_rate"),
        )
        .withColumn("as_of_date", F.lit(as_of_date).cast("date"))
    )


def build_mart_customer_360(dim_customer: DataFrame, fact_loan: DataFrame,
                            accounts: DataFrame, fact_txn: DataFrame,
                            as_of_date: str) -> DataFrame:
    cur = dim_customer.filter("is_current")
    loans = (
        fact_loan.filter(F.col("as_of_date") == F.lit(as_of_date).cast("date"))
        .groupBy("customer_id")
        .agg(F.count("*").alias("loan_count"), F.sum("balance_vnd").alias("loan_balance_vnd"),
             F.max("dpd").alias("max_dpd"), F.max("debt_group").alias("worst_debt_group"))
    )
    deps = (
        accounts.filter((F.col("product_type") != "LOAN") & (F.col("status") != "CLOSED"))
        .groupBy("customer_id")
        .agg(F.count("*").alias("deposit_account_count"),
             F.sum(F.when(F.col("currencycd") == "VND", F.col("balance")).otherwise(0))
             .alias("deposit_balance_vnd"))
    )
    since = F.date_sub(F.lit(as_of_date).cast("date"), 29)
    txns = (
        fact_txn.filter(F.col("value_date").between(since, F.lit(as_of_date).cast("date")))
        .groupBy("customer_id")
        .agg(F.count("*").alias("txn_count_30d"),
             F.sum("amount").alias("txn_amount_30d"),
             F.max("value_date").alias("last_txn_date"))
    )
    out = cur.select("customer_id", "customer_sk", "full_name", "cust_type", "segment",
                     "province", "branchnbr", "monthly_income")
    for df in [loans, deps, txns]:
        out = out.join(df, "customer_id", "left")
    return (
        out.fillna(0, subset=["loan_count", "loan_balance_vnd", "deposit_account_count",
                              "deposit_balance_vnd", "txn_count_30d", "txn_amount_30d"])
        .withColumn("has_npl", F.coalesce(F.col("worst_debt_group") >= 3, F.lit(False)))
        .withColumn("as_of_date", F.lit(as_of_date).cast("date"))
    )


# Khóa MERGE của dim/fact (notebook dùng). Mart không MERGE mà ghi đè đúng ngày chạy
# (replaceWhere as_of_date = ...) vì cột nhóm có thể NULL và mart luôn tính lại cả ngày.
GOLD_KEYS = {
    "dim_date": ["date_key"],
    "dim_branch": ["branchnbr"],
    "dim_product": ["prodcode"],
    "loan_fact_daily": ["as_of_date", "acctnbr"],
    "core_fact_transaction": ["txn_id"],
}
