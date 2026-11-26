"""Gold cho dashboard Thẻ (Card Business) + đối soát với file tổng hợp của hệ thống nguồn.

Mart
    card_mart_kpi_daily     ngày x chi nhánh x loại KH x kênh phát hành: thẻ hiện hữu, mở mới,
                            đóng, đang dùng, dư nợ, dư nợ quay vòng, chi tiêu, DPD 1+/10+/30+/90+
    card_mart_spend_mix     ngày x loại giao dịch x trong nước/nước ngoài x kênh x MCC
    card_mart_kpi_monthly   tháng x chi nhánh: tồn cuối kỳ + luỹ kế trong kỳ, MoM / YoY,
                            % hoàn thành chỉ tiêu tháng / năm (bảng targets)
Đối soát
    card_kpi_metrics        mart ngày -> mã chỉ tiêu giống file card_kpi nguồn
    reconcile               so sánh Gold vs nguồn theo từng metric, có dung sai riêng
    deposit_reconciliation  dep_mart_daily vs deposit_summary (bang1247)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DPD_BUCKETS = [("DPD_1P", 1), ("DPD_10P", 10), ("DPD_30P", 30), ("NPL_90P", 90)]
SPEND_TYPES = {"PURCHASE": "purchase_amt", "CASH_ADVANCE": "cash_advance_amt",
               "INSTALLMENT": "installment_amt", "FLEXI": "flexi_amt"}
DIMS = ["branchnbr", "cust_type", "acquisition_channel"]


def _sum_if(cond, value) -> F.Column:
    return F.sum(F.when(cond, value).otherwise(F.lit(0)))


# ---------------------------------------------------------------- mart ngày
def build_mart_card_kpi_daily(cards: DataFrame, card_txns: DataFrame, statements: DataFrame,
                              as_of_date: str) -> DataFrame:
    """cards: snapshot thẻ ngày as_of; card_txns: giao dịch hợp lệ trong ngày;
    statements: sao kê cuối ngày as_of (chỉ thẻ ACTIVE)."""
    day = F.lit(as_of_date).cast("date")
    c = cards.select("card_no", *DIMS, "status", "open_date", "close_date", "credit_limit")

    members = c.groupBy(*DIMS).agg(
        _sum_if(F.col("status") == "ACTIVE", F.lit(1)).alias("eop_members"),
        _sum_if(F.col("open_date") == day, F.lit(1)).alias("new_members"),
        _sum_if(F.col("close_date") == day, F.lit(1)).alias("closed_members"),
        _sum_if(F.col("status") == "ACTIVE", F.col("credit_limit")).alias("credit_limit"),
    )

    t = card_txns.join(c.select("card_no", *DIMS), "card_no", "inner")
    spend_aggs = [_sum_if(F.col("txn_type") == k, F.col("amount")).alias(v)
                  for k, v in SPEND_TYPES.items()]
    spend = t.groupBy(*DIMS).agg(
        F.countDistinct("card_no").alias("using_members"),
        F.count("*").alias("txn_count"),
        F.sum("amount").alias("spend_amt"),
        *spend_aggs,
        _sum_if(F.col("location") == "DOMESTIC", F.col("amount")).alias("domestic_amt"),
        _sum_if(F.col("location") == "XBORDER", F.col("amount")).alias("xborder_amt"),
        _sum_if(F.col("channel") == "POS", F.col("amount")).alias("pos_amt"),
        _sum_if(F.col("channel") == "ONLINE", F.col("amount")).alias("online_amt"),
    )

    s = statements.join(c.select("card_no", *DIMS), "card_no", "inner")
    bucket_aggs = []
    for code, lo in DPD_BUCKETS:
        name = code.lower()
        bucket_aggs += [
            _sum_if(F.col("dpd") >= lo, F.lit(1)).alias(f"{name}_members"),
            _sum_if(F.col("dpd") >= lo, F.col("outstanding_balance")).alias(f"{name}_balance"),
        ]
    risk = s.groupBy(*DIMS).agg(
        F.sum("outstanding_balance").alias("balance"),
        F.sum("revolving_balance").alias("revolving_balance"),
        _sum_if(F.col("revolving_balance") > 0, F.lit(1)).alias("revolving_members"),
        *bucket_aggs,
    )

    out = members.join(spend, DIMS, "full").join(risk, DIMS, "full")
    num_cols = [x for x in out.columns if x not in DIMS]
    return out.fillna(0, subset=num_cols).withColumn("as_of_date", day) \
        .select("as_of_date", *DIMS, *num_cols)


def build_mart_card_spend_mix(card_txns: DataFrame, as_of_date: str,
                              mcc_names: dict[str, str] | None = None) -> DataFrame:
    """Cho 3 biểu đồ donut: theo loại giao dịch, trong nước / nước ngoài, POS / Online."""
    df = card_txns
    if mcc_names:
        m = F.create_map(*[x for kv in mcc_names.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
        df = df.withColumn("mcc_category", F.coalesce(m[F.col("mcc")], F.lit("Other")))
    else:
        df = df.withColumn("mcc_category", F.col("mcc"))
    return (
        df.groupBy("txn_type", "location", "channel", "mcc_category")
        .agg(F.count("*").alias("txn_count"), F.sum("amount").alias("amount"))
        .withColumn("as_of_date", F.lit(as_of_date).cast("date"))
    )


# ---------------------------------------------------------------- mart tháng + chỉ tiêu
STOCK = ["eop_members", "balance", "revolving_balance", "revolving_members", "credit_limit"] + \
    [f"{c.lower()}_{k}" for c, _ in DPD_BUCKETS for k in ("members", "balance")]
FLOW = ["new_members", "closed_members", "spend_amt", "txn_count"] + list(SPEND_TYPES.values()) \
    + ["domestic_amt", "xborder_amt", "pos_amt", "online_amt"]


def build_mart_card_kpi_monthly(daily: DataFrame, targets: DataFrame | None = None) -> DataFrame:
    """Gộp theo tháng x chi nhánh. Chỉ số tồn (stock) lấy ngày cuối cùng có dữ liệu của tháng,
    chỉ số luỹ kế (flow) cộng cả tháng. `using_members` tháng = max theo ngày (xấp xỉ, vì
    số thẻ dùng riêng biệt cả tháng cần giao dịch chi tiết)."""
    d = daily.groupBy("as_of_date", "branchnbr").agg(
        *[F.sum(c).alias(c) for c in STOCK + FLOW + ["using_members"]])
    d = d.withColumn("year_month", F.date_format("as_of_date", "yyyy-MM"))
    w_last = Window.partitionBy("year_month", "branchnbr").orderBy(F.desc("as_of_date"))
    stock = d.withColumn("_rn", F.row_number().over(w_last)).filter("_rn = 1") \
        .select("year_month", "branchnbr", F.col("as_of_date").alias("last_date"), *STOCK)
    flow = d.groupBy("year_month", "branchnbr").agg(
        *[F.sum(c).alias(c) for c in FLOW], F.max("using_members").alias("using_members"),
        F.countDistinct("as_of_date").alias("days_with_data"))
    m = stock.join(flow, ["year_month", "branchnbr"])

    # MoM / YoY: so với cùng chi nhánh, tháng trước / cùng tháng năm trước
    prev = m.select("year_month", "branchnbr", "eop_members", "balance", "spend_amt",
                    "new_members")
    for tag, months in (("mom", 1), ("yoy", 12)):
        p = prev.select(
            F.date_format(F.add_months(F.to_date(F.concat("year_month", F.lit("-01"))), months),
                          "yyyy-MM").alias("year_month"),
            "branchnbr",
            *[F.col(c).alias(f"{c}_{tag}_base") for c in
              ("eop_members", "balance", "spend_amt", "new_members")])
        m = m.join(p, ["year_month", "branchnbr"], "left")
        for c in ("eop_members", "balance", "spend_amt", "new_members"):
            base = F.col(f"{c}_{tag}_base")
            m = m.withColumn(f"{c}_{tag}_pct",
                             F.when(base > 0, (F.col(c) - base) / base * 100)).drop(base)

    if targets is not None:
        m = _attach_targets(m, targets)
    return m


def _attach_targets(m: DataFrame, targets: DataFrame) -> DataFrame:
    """% hoàn thành chỉ tiêu: mở mới & doanh số theo tháng và luỹ kế năm."""
    t = targets.filter(F.col("metric_code").isin("CARD_NEW_ACQ", "CARD_SALES_VOLUME"))
    month_t = t.filter("period_type = 'MONTH'").groupBy(
        F.col("period").alias("year_month"), "branchnbr").pivot(
        "metric_code", ["CARD_NEW_ACQ", "CARD_SALES_VOLUME"]).agg(F.sum("target_value"))
    month_t = month_t.withColumnRenamed("CARD_NEW_ACQ", "target_new_month") \
        .withColumnRenamed("CARD_SALES_VOLUME", "target_sales_month")
    year_t = t.filter("period_type = 'YEAR'").groupBy(
        F.col("period").alias("_year"), "branchnbr").pivot(
        "metric_code", ["CARD_NEW_ACQ", "CARD_SALES_VOLUME"]).agg(F.sum("target_value"))
    year_t = year_t.withColumnRenamed("CARD_NEW_ACQ", "target_new_year") \
        .withColumnRenamed("CARD_SALES_VOLUME", "target_sales_year")

    w_ytd = Window.partitionBy("branchnbr", F.substring("year_month", 1, 4)) \
        .orderBy("year_month").rowsBetween(Window.unboundedPreceding, 0)
    out = (
        m.withColumn("_year", F.substring("year_month", 1, 4))
        .withColumn("new_members_ytd", F.sum("new_members").over(w_ytd))
        .withColumn("spend_amt_ytd", F.sum("spend_amt").over(w_ytd))
        .join(month_t, ["year_month", "branchnbr"], "left")
        .join(year_t, ["_year", "branchnbr"], "left")
        .drop("_year")
    )

    def pct(a, b):
        return F.when(F.col(b) > 0, F.col(a) / F.col(b) * 100)

    return (out.withColumn("new_vs_month_target_pct", pct("new_members", "target_new_month"))
            .withColumn("sales_vs_month_target_pct", pct("spend_amt", "target_sales_month"))
            .withColumn("new_vs_year_target_pct", pct("new_members_ytd", "target_new_year"))
            .withColumn("sales_vs_year_target_pct", pct("spend_amt_ytd", "target_sales_year")))


# ---------------------------------------------------------------- đối soát
def card_kpi_metrics(daily: DataFrame) -> DataFrame:
    """Mart ngày -> (as_of_date, metric_code, gold_value) với mã giống file card_kpi nguồn."""
    sums = [F.sum(c).alias(c) for c in
            STOCK + FLOW + ["using_members"]]
    by_type = daily.groupBy("as_of_date", "cust_type").agg(*sums)
    tot = daily.groupBy("as_of_date").agg(*sums)

    def typ(t: str, col: str) -> F.Column:
        return F.first(F.when(F.col("cust_type") == t, F.col(col)), ignorenulls=True)

    per_type = by_type.groupBy("as_of_date").agg(
        *[typ(t, c).alias(f"{c}_{t}") for t in ("CN", "TC")
          for c in ("eop_members", "using_members", "new_members", "closed_members")])
    x = tot.join(per_type, "as_of_date", "left").fillna(0)

    def rate(a, b):
        return F.when(F.col(b) > 0, F.round(F.col(a) / F.col(b) * 100, 2)).otherwise(0)

    def ratio(a, b):
        return F.when(b > 0, a / b).otherwise(0)

    closed = F.col("closed_members_CN") + F.col("closed_members_TC")
    users = F.col("using_members_CN") + F.col("using_members_TC")
    metrics = {
        "EOP_Consumer": F.col("eop_members_CN"), "EOP_Corporate": F.col("eop_members_TC"),
        "EOP_Total": F.col("eop_members"),
        "Using_Consumer": F.col("using_members_CN"),
        "Usage_Consumer_Rate": rate("using_members_CN", "eop_members_CN"),
        "Using_Corporate": F.col("using_members_TC"),
        "Usage_Corporate_Rate": rate("using_members_TC", "eop_members_TC"),
        "New_Consumer": F.col("new_members_CN"), "New_Corporate": F.col("new_members_TC"),
        "New_Total": F.col("new_members"),
        "Consumer_Closed": F.col("closed_members_CN"),
        "Corporate_Closed": F.col("closed_members_TC"), "Member_Closed": closed,
        "Attrition_Rate": F.round(ratio(closed, F.col("eop_members")) * 100, 2),
        "Total_Outstanding_Balance": F.col("balance"),
        "Avg_Balance_Per_Member": ratio(F.col("balance"), F.col("eop_members")),
        "Purchasing_Amt": F.col("purchase_amt"), "Cash_Advance_Amt": F.col("cash_advance_amt"),
        "Installment_Amt": F.col("installment_amt"), "Flexi_Amt": F.col("flexi_amt"),
        "Total_Spending_Amt": F.col("spend_amt"),
        "Avg_Spending_Per_Member": ratio(F.col("spend_amt"), F.greatest(users, F.lit(1))),
        "Domestic_Spending_Amt": F.col("domestic_amt"),
        "XBorder_Spending_Amt": F.col("xborder_amt"),
        "Domestic_Rate": rate("domestic_amt", "spend_amt"),
        "XBorder_Rate": rate("xborder_amt", "spend_amt"),
        "POS_Spending_Amt": F.col("pos_amt"), "Online_Spending_Amt": F.col("online_amt"),
        "Offline_Rate": rate("pos_amt", "spend_amt"),
        "Online_Rate": rate("online_amt", "spend_amt"),
        "Revolving_Balance": F.col("revolving_balance"),
        "Revolving_Member": F.col("revolving_members"),
        "Revolving_Rate": rate("revolving_members", "eop_members"),
    }
    for code, _ in DPD_BUCKETS:
        n = code.lower()
        metrics[f"{code}_Member"] = F.col(f"{n}_members")
        metrics[f"{code}_Balance"] = F.col(f"{n}_balance")
        metrics[f"{code}_Rate"] = rate(f"{n}_balance", "balance")
    stack = F.array(*[F.struct(F.lit(k).alias("metric_code"),
                               v.cast("decimal(24,4)").alias("gold_value"))
                      for k, v in metrics.items()])
    return x.select("as_of_date", F.explode(stack).alias("m")).select("as_of_date", "m.*")


# Dung sai mặc định: chỉ tiêu tồn / số thẻ phải khớp tuyệt đối (sai lệch do làm tròn file nguồn
# <= 1 đơn vị). Chỉ tiêu dựa trên giao dịch cho lệch <= 1%: khi 2 giao dịch trùng txn_id,
# Silver đưa CẢ HAI vào quarantine (không biết bản nào đúng) còn hệ thống nguồn giữ bản đầu.
TXN_BASED = ("Using_", "Usage_", "_Spending_Amt", "Purchasing_Amt", "Cash_Advance_Amt",
             "Installment_Amt", "Flexi_Amt", "Avg_Spending", "Domestic_Rate", "XBorder_Rate",
             "Offline_Rate", "Online_Rate")


def reconcile(gold: DataFrame, source: DataFrame, abs_tol: float = 1.0,
              txn_rel_tol_pct: float = 1.0) -> DataFrame:
    """gold: (as_of_date, metric_code, gold_value); source: silver.card_kpi_source.
    Trả về từng metric: giá trị 2 bên, chênh lệch, MATCH / WITHIN_TOLERANCE / MISMATCH /
    MISSING_IN_GOLD / MISSING_IN_SOURCE."""
    src = source.select(F.col("trx_date").alias("as_of_date"), "metric_code",
                        F.col("metric_value").alias("source_value"), "is_rate")
    j = gold.join(src, ["as_of_date", "metric_code"], "full")
    is_txn = F.lit(False)
    for p in TXN_BASED:
        is_txn = is_txn | F.col("metric_code").contains(p)
    diff = F.col("gold_value") - F.col("source_value")
    rel = F.when(F.col("source_value") != 0, F.abs(diff) / F.abs(F.col("source_value")) * 100)
    # tỷ lệ % làm tròn 2 số lẻ ở nguồn -> cho lệch 0.01 điểm %
    tol = F.when(F.coalesce(F.col("is_rate"), F.lit(False)), F.lit(0.011)).otherwise(abs_tol)
    status = (
        F.when(F.col("gold_value").isNull(), "MISSING_IN_GOLD")
        .when(F.col("source_value").isNull(), "MISSING_IN_SOURCE")
        .when(F.abs(diff) <= tol, "MATCH")
        .when(is_txn & (F.coalesce(rel, F.lit(0)) <= txn_rel_tol_pct), "WITHIN_TOLERANCE")
        .otherwise("MISMATCH")
    )
    return j.select("as_of_date", "metric_code", "gold_value", "source_value",
                    diff.alias("diff"), F.round(rel, 4).alias("diff_pct"),
                    status.alias("status"))


def deposit_reconciliation(mart_deposit: DataFrame, deposit_summary: DataFrame,
                           fx: DataFrame, group_of: dict[str, str]) -> DataFrame:
    """Số dư tiền gửi Gold (quy đổi triệu VND) vs file bang1247, theo ngày x nhóm TK x tiền tệ
    x chi nhánh. group_of: product_type -> ACCOUNT_GROUP."""
    gmap = F.create_map(*[x for kv in group_of.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
    rate = fx.select(F.col("rate_date").alias("as_of_date"), "currencycd", "rate_to_vnd")
    gold = (
        mart_deposit.join(rate, ["as_of_date", "currencycd"], "left")
        .withColumn("account_group", gmap[F.col("product_type")])
        .groupBy("as_of_date", "account_group", "currencycd", "branchnbr")
        .agg(F.sum(F.col("balance") * F.col("rate_to_vnd") / 1e6).alias("gold_mil_vnd"))
    )
    src = deposit_summary.select(F.col("stnd_dt").alias("as_of_date"), "account_group",
                                 "currencycd", "branchnbr",
                                 F.col("balance_mil_vnd").alias("source_mil_vnd"))
    j = gold.join(src, ["as_of_date", "account_group", "currencycd", "branchnbr"], "full")
    diff = F.col("gold_mil_vnd") - F.col("source_mil_vnd")
    return j.withColumn("diff", diff).withColumn(
        "status",
        F.when(F.col("gold_mil_vnd").isNull(), "MISSING_IN_GOLD")
        .when(F.col("source_mil_vnd").isNull(), "MISSING_IN_SOURCE")
        .when(F.abs(diff) <= 0.01, "MATCH")       # nguồn làm tròn 2 số lẻ (triệu VND)
        .otherwise("MISMATCH"))
