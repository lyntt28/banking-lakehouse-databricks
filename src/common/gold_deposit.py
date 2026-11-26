"""Gold cho dashboard Tiền gửi bán lẻ (RCD) - xem docs/gold_dashboards.md mục 3.

    dep_mart_trend          ngày x nhóm TK x lớp (CASA/TD/ISA) x tiền tệ x vùng: số dư VND,
                                so với D-1, cuối tháng trước (M-1), cuối năm trước (Y-1)
    dep_mart_by_tenor       TD / ISA theo kỳ hạn: số dư, số TK, lãi suất min/max/bình quân
    dep_mart_casa_customer          chi nhánh: KH cá nhân, KH có CASA, CASA hoạt động / ngủ đông
    dep_mart_new_accounts   tháng mở x chi nhánh x sản phẩm: TK mở mới + luỹ kế trong năm
                                (dashboard lọc product_type = 'KSTUDY')
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def _open() -> F.Column:
    return ~F.col("status").isin("CLOSED")


def _deposits_vnd(accounts: DataFrame, products: DataFrame, fx: DataFrame) -> DataFrame:
    """TK tiền gửi + nhóm TK / lớp (từ dim_product) + số dư quy đổi VND (fx cùng ngày)."""
    p = products.filter("product_family = 'DEPOSIT'").select(
        F.col("prodcode").alias("product_type"),
        F.col("product_name").alias("account_group"),
        F.col("product_group").alias("group_class"))
    rate = fx.select("currencycd", "rate_to_vnd")
    return (accounts.filter(F.col("product_type") != "LOAN")
            .join(p, "product_type", "left")
            .join(rate, "currencycd", "left")
            .withColumn("balance_vnd", F.col("balance") * F.col("rate_to_vnd")))


def compare_with_history(df: DataFrame, keys: list[str], values: list[str],
                         date_col: str = "as_of_date") -> DataFrame:
    """Thêm <v>_d1 / _m1 / _y1 cho từng cột value: giá trị cùng khóa tại ngày gần nhất
    <= hôm qua / cuối tháng trước / cuối năm trước (bỏ qua ngày không có file)."""
    hist = df.select(F.col(date_col).alias("_hd"), *[F.col(k).alias(f"_h_{k}") for k in keys],
                     *[F.col(v).alias(f"_h_{v}") for v in values])
    marks = {"d1": F.date_sub(date_col, 1),
             "m1": F.last_day(F.add_months(date_col, -1)),
             "y1": F.make_date(F.year(date_col) - 1, F.lit(12), F.lit(31))}
    out = df
    for tag, mark in marks.items():
        cond = F.col("_hd") <= mark
        for k in keys:
            cond = cond & F.col(f"_h_{k}").eqNullSafe(F.col(k))
        w = Window.partitionBy(date_col, *keys).orderBy(F.col("_hd").desc_nulls_last())
        out = (out.join(hist, cond, "left")
               .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
               .select(*out.columns, *[F.col(f"_h_{v}").alias(f"{v}_{tag}") for v in values]))
    return out


def build_mart_deposit_trend(accounts: DataFrame, products: DataFrame, fx: DataFrame,
                             branches: DataFrame, as_of_date: str,
                             history: DataFrame | None = None) -> DataFrame:
    """Mart ngày as_of; `history` = các ngày trước của chính mart (để tính D-1/M-1/Y-1)."""
    dims = ["account_group", "group_class", "currencycd", "region"]
    b = branches.select("branchnbr", "region")
    day = (_deposits_vnd(accounts, products, fx).join(b, "branchnbr", "left")
           .groupBy(*dims)
           .agg(F.sum("balance_vnd").alias("balance_vnd"),
                F.sum(_open().cast("int")).alias("open_accounts"))
           .withColumn("as_of_date", F.lit(as_of_date).cast("date")))
    total = day.agg(F.sum("balance_vnd")).first()[0] or 0
    day = day.withColumn("share_pct", F.col("balance_vnd").cast("double") / float(total) * 100
                         if total else F.lit(None).cast("double"))
    base = ["as_of_date", *dims, "balance_vnd", "open_accounts", "share_pct"]
    allrows = day.select(*base)
    if history is not None:
        allrows = history.select(*base).filter(F.col("as_of_date") != F.lit(as_of_date)
                                               .cast("date")).unionByName(allrows)
    cmp = compare_with_history(allrows, dims, ["balance_vnd"])
    out = cmp.filter(F.col("as_of_date") == F.lit(as_of_date).cast("date"))
    for tag in ("d1", "m1", "y1"):
        out = out.withColumn(f"diff_{tag}", F.col("balance_vnd") - F.col(f"balance_vnd_{tag}"))
    return out


def build_mart_deposit_by_tenor(accounts: DataFrame, products: DataFrame, fx: DataFrame,
                                as_of_date: str) -> DataFrame:
    d = _deposits_vnd(accounts, products, fx).filter(_open() & (F.col("tenor_months") > 0))
    return (d.groupBy("group_class", "product_type", "tenor_months", "currencycd")
            .agg(F.count("*").alias("accounts"),
                 F.sum("balance_vnd").alias("balance_vnd"),
                 F.min("int_rate").alias("min_rate"), F.max("int_rate").alias("max_rate"),
                 F.avg("int_rate").alias("avg_rate"),
                 (F.sum(F.col("int_rate") * F.col("balance_vnd"))
                  / F.sum("balance_vnd")).alias("weighted_rate"))
            .withColumn("as_of_date", F.lit(as_of_date).cast("date")))


def build_mart_casa_customer(accounts: DataFrame, products: DataFrame,
                             customers: DataFrame, as_of_date: str) -> DataFrame:
    """customers: bản hiện hành (is_current). KH 'CASA hoạt động' = có ít nhất 1 TK CASA ACTIVE;
    'ngủ đông' = có CASA nhưng tất cả đều DORMANT."""
    p = products.filter("product_group = '1-CASA'").select(F.col("prodcode").alias("product_type"))
    casa = accounts.join(p, "product_type").filter(_open()).groupBy("customer_id").agg(
        F.max((F.col("status") == "ACTIVE").cast("int")).alias("has_active"))
    retail = customers.filter("cust_type = 'CN'").select("customer_id", "branchnbr")
    j = retail.join(casa, "customer_id", "left")
    return (j.groupBy("branchnbr")
            .agg(F.count("*").alias("retail_customers"),
                 F.count("has_active").alias("casa_customers"),
                 F.sum(F.coalesce("has_active", F.lit(0))).alias("casa_active_customers"))
            .withColumn("casa_dormant_customers",
                        F.col("casa_customers") - F.col("casa_active_customers"))
            .withColumn("casa_penetration_pct",
                        F.col("casa_customers") / F.col("retail_customers") * 100)
            .withColumn("as_of_date", F.lit(as_of_date).cast("date")))


def build_mart_deposit_new_accounts(accounts: DataFrame) -> DataFrame:
    """Số TK mở mới theo tháng mở + luỹ kế trong năm (tính lại toàn bộ từ snapshot TK)."""
    m = (accounts.filter(F.col("product_type") != "LOAN")
         .withColumn("open_month", F.date_format("open_date", "yyyy-MM"))
         .groupBy("open_month", "branchnbr", "product_type")
         .agg(F.count("*").alias("new_accounts")))
    w = Window.partitionBy("branchnbr", "product_type", F.substring("open_month", 1, 4)) \
        .orderBy("open_month").rowsBetween(Window.unboundedPreceding, 0)
    return m.withColumn("new_accounts_ytd", F.sum("new_accounts").over(w))
