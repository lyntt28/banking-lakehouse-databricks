"""Gold cho dashboard Thẻ (Card Business) + đối soát với file tổng hợp của hệ thống nguồn.

Mart
    card_mart_kpi_daily     ngày x chi nhánh x loại KH x kênh phát hành: thẻ hiện hữu, mở mới,
                            đóng, đang dùng, dư nợ, dư nợ quay vòng, chi tiêu, DPD 1+/10+/30+/90+
    card_mart_spend_mix     ngày x loại giao dịch x trong nước/nước ngoài x kênh x MCC
    card_mart_kpi_monthly   tháng x chi nhánh x loại KH x kênh (tính tại as_of_date): tồn tại
                            ngày chốt, luỹ kế MTD / YTD, thẻ dùng distinct MTD, độ đầy đủ
                            (chỉ tiêu kế hoạch ghép ở dashboard SQL theo đúng loại KH)
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


# ---------------------------------------------------------------- mart tháng
STOCK = ["eop_members", "balance", "revolving_balance", "revolving_members", "credit_limit"] + \
    [f"{c.lower()}_{k}" for c, _ in DPD_BUCKETS for k in ("members", "balance")]
FLOW = ["new_members", "closed_members", "spend_amt", "txn_count"] + list(SPEND_TYPES.values()) \
    + ["domestic_amt", "xborder_amt", "pos_amt", "online_amt"]
YTD = ["new_members", "closed_members", "spend_amt"]


def using_members_by_month(card_txns: DataFrame, cards: DataFrame,
                           date_col: str = "_as_of_date") -> DataFrame:
    """Số THẺ có giao dịch hợp lệ (distinct card_no) theo tháng x DIMS, trên toàn bộ giao dịch
    của kỳ được truyền vào (MTD: truyền giao dịch từ ngày 1 tới ngày báo cáo).

    Đơn vị = thẻ (member = card_no, như file nguồn 02_member_usage), không phải KH. Thuộc
    tính DIMS của thẻ cố định -> 1 thẻ chỉ thuộc 1 tổ hợp DIMS, cộng qua DIMS không đếm trùng.
    KHÔNG xấp xỉ bằng SUM / MAX của số thẻ dùng theo ngày."""
    c = cards.select("card_no", *DIMS).dropDuplicates(["card_no"])
    # chỉ lấy khoá + ngày: fact giao dịch có thể đã mang sẵn branchnbr / customer_id
    return (card_txns.select("card_no", date_col).join(c, "card_no", "inner")
            .withColumn("year_month", F.date_format(F.col(date_col), "yyyy-MM"))
            .groupBy("year_month", *DIMS)
            .agg(F.countDistinct("card_no").alias("using_members")))


def _month_rows(daily: DataFrame, using: DataFrame | None) -> DataFrame:
    """daily (mart ngày, đã lọc kỳ) -> 1 dòng / tháng x DIMS: tồn = ngày cuối có dữ liệu của
    tháng (cùng 1 ngày chốt cho mọi DIMS), luỹ kế = cộng các ngày, kèm số ngày có dữ liệu."""
    d = daily.withColumn("year_month", F.date_format("as_of_date", "yyyy-MM"))
    snap = d.groupBy("year_month").agg(F.max("as_of_date").alias("snapshot_date"),
                                       F.countDistinct("as_of_date").alias("days_with_data"))
    last = snap.select(F.col("year_month").alias("_ym"), F.col("snapshot_date").alias("_sd"))
    stock = d.join(last, (F.col("year_month") == F.col("_ym"))
                   & (F.col("as_of_date") == F.col("_sd"))) \
        .groupBy("year_month", *DIMS).agg(*[F.sum(c).alias(c) for c in STOCK])
    flow = d.groupBy("year_month", *DIMS).agg(*[F.sum(c).alias(c) for c in FLOW])
    m = flow.join(stock, ["year_month", *DIMS], "full").join(snap, "year_month")
    if using is not None:
        m = m.join(using, ["year_month", *DIMS], "left") \
            .withColumn("using_members", F.coalesce("using_members", F.lit(0)))
    else:
        m = m.withColumn("using_members", F.lit(None).cast("long"))
    return m.fillna(0, subset=STOCK + FLOW)


def build_mart_card_kpi_monthly(daily_month: DataFrame, as_of_date: str,
                                using_mtd: DataFrame | None = None,
                                previous: DataFrame | None = None,
                                history_daily: DataFrame | None = None,
                                history_using: DataFrame | None = None) -> DataFrame:
    """card_mart_kpi_monthly tại ngày T (as_of_date = T), tháng 1 năm trước -> tháng của T.

    Grain: year_month x branchnbr x cust_type x acquisition_channel (giữ filter loại KH /
    kênh). Tháng của T: tồn tại T, luỹ kế từ ngày 1 tới T (`daily_month` = mart ngày của
    tháng, <= T), using = distinct MTD (`using_mtd`). Tháng trước đó: chép từ `previous`
    (mart tháng của ngày publish trước) -> incremental; lần đầu dựng từ `history_daily` /
    `history_using`. Không đọc dữ liệu sau T.

    flow_complete = có đủ dữ liệu mọi ngày của kỳ (tháng đóng: cả tháng; tháng hiện tại: 1..T).
    Lịch sử chỉ có snapshot cuối tháng -> tồn đúng, luỹ kế KHÔNG đủ -> dashboard hiện N/A."""
    from datetime import date

    t = date.fromisoformat(as_of_date)
    month_now, start = f"{t:%Y-%m}", f"{t.year - 1}-01"
    cur = _month_rows(daily_month.filter(
        (F.col("as_of_date") <= F.lit(as_of_date).cast("date"))
        & (F.date_format("as_of_date", "yyyy-MM") == month_now)), using_mtd)
    cols = [c for c in cur.columns]
    if previous is not None:
        past = previous.filter((F.col("year_month") < month_now)
                               & (F.col("year_month") >= start)).select(*cols)
    elif history_daily is not None:
        past = _month_rows(history_daily.filter(
            (F.date_format("as_of_date", "yyyy-MM") < month_now)
            & (F.date_format("as_of_date", "yyyy-MM") >= start)), history_using).select(*cols)
    else:
        past = None
    m = cur if past is None else past.unionByName(cur)
    month_start = F.to_date(F.concat("year_month", F.lit("-01")))
    expected = F.when(F.col("year_month") == month_now, F.lit(t.day)) \
        .otherwise(F.dayofmonth(F.last_day(month_start)))
    m = (m.withColumn("days_expected", expected)
         .withColumn("flow_complete", F.col("days_with_data") == F.col("days_expected"))
         .withColumn("is_month_closed", F.col("snapshot_date") == F.last_day("snapshot_date")))
    w = Window.partitionBy(*DIMS, F.substring("year_month", 1, 4)).orderBy("year_month") \
        .rowsBetween(Window.unboundedPreceding, 0)
    for c in YTD:
        m = m.withColumn(f"{c}_ytd", F.sum(c).over(w))
    # YTD chỉ đủ khi mọi tháng từ tháng 1 đều đủ ngày
    m = m.withColumn("ytd_complete", F.min(F.col("flow_complete").cast("int")).over(w) == 1)
    return m.withColumn("as_of_date", F.lit(as_of_date).cast("date"))


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
    metrics = {
        "EOP_Consumer": F.col("eop_members_CN"), "EOP_Corporate": F.col("eop_members_TC"),
        "EOP_Total": F.col("eop_members"),
        "Using_Consumer": F.col("using_members_CN"),
        "Usage_Consumer_Rate": rate("using_members_CN", "eop_members_CN"),
        "Using_Corporate": F.col("using_members_TC"),
        "Usage_Corporate_Rate": rate("using_members_TC", "eop_members_TC"),
        "New_Consumer": F.col("new_members_CN"), "New_Corporate": F.col("new_members_TC"),
        "New_Member": F.col("new_members"),
        "Consumer_Closed": F.col("closed_members_CN"),
        "Corporate_Closed": F.col("closed_members_TC"), "Member_Closed": closed,
        "Attrition_Rate": F.round(ratio(closed, F.col("eop_members")) * 100, 2),
        "Total_Outstanding_Balance": F.col("balance"),
        "Avg_Balance_Per_Member": ratio(F.col("balance"), F.col("eop_members")),
        "Purchasing_Amt": F.col("purchase_amt"), "Cash_Advance_Amt": F.col("cash_advance_amt"),
        "Installment_Amt": F.col("installment_amt"), "Flexi_Amt": F.col("flexi_amt"),
        "Total_Spending_Amt": F.col("spend_amt"),
        # như API gốc (calculate_avg_spending_per_member): chi tiêu / số thẻ hiện hữu (EOP)
        "Avg_Spending_Per_Member": ratio(F.col("spend_amt"), F.col("eop_members")),
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


def build_mart_card_overdue_daily(src: DataFrame) -> DataFrame:
    """silver.card_overdue_source -> gold.card_mart_overdue_daily (VND, chưa quy đổi USD).

    Grain: ngày x nhóm SP x sản phẩm x CN quản lý x CN bán x nhóm nợ. Nguồn cho dashboard
    Overdue (tỷ lệ quá hạn = sum(ovd_bal) / sum(port_bal), lọc theo CN quản lý)."""
    return (
        src.withColumnRenamed("report_dt", "as_of_date")
        .groupBy("as_of_date", "product_type", "product_group", "product_name", "mng_br",
                 "sal_br", "del_grp")
        .agg(F.sum("port_acc").alias("port_acc"), F.sum("ovd_acc").alias("ovd_acc"),
             F.sum("port_bal").alias("port_bal"), F.sum("ovd_bal").alias("ovd_bal"))
    )


OVD_MEASURES = ("port_acc", "ovd_acc", "port_bal", "ovd_bal")


def overdue_reconciliation(silver: DataFrame, gold: DataFrame) -> dict[str, dict]:
    """Đối soát Silver (card_overdue_source, 1 ngày) vs Gold (card_mart_overdue_daily, cùng
    ngày): tổng 4 chỉ tiêu phải khớp TUYỆT ĐỐI. Một lần agg mỗi bên; tiền giữ Decimal (Spark
    decimal -> Python Decimal), không qua float. Trả về {measure: {silver, gold, match}}."""
    from decimal import Decimal

    def totals(df: DataFrame) -> dict:
        row = df.agg(*[F.sum(m).alias(m) for m in OVD_MEASURES]).first()
        out = {}
        for m in OVD_MEASURES:
            v = row[m] if row is not None else None
            if isinstance(v, float):
                raise TypeError(f"{m}: số tiền / số lượng phải là DECIMAL / BIGINT, không float")
            out[m] = Decimal(v) if v is not None else Decimal(0)
        return out

    s, g = totals(silver), totals(gold)
    return {m: {"silver": s[m], "gold": g[m], "match": s[m] == g[m]} for m in OVD_MEASURES}
