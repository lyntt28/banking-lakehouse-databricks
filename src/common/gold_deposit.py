"""Gold cho dashboard Tiền gửi bán lẻ (DEPOSIT) - xem docs/deposit_dashboard.md.

    dep_mart_trend          ngày x nhóm TK x lớp (CASA/TD/ISA) x tiền tệ x vùng x chi nhánh
                                x quốc tịch: số dư (stock), bình quân từ đầu tháng (MTD avg),
                                chênh lệch D-1 / M-1 / Y-1 + ngày so sánh thực tế
    dep_mart_monthly        tháng x cùng dimension (tính tại as_of_date): số dư cuối tháng
                                (tháng hiện tại = ngày báo cáo) + bình quân tháng
    dep_mart_by_tenor       TD / ISA theo kỳ hạn x chi nhánh: số dư, tử / mẫu lãi suất bình quân
                                gia quyền, min / max
    dep_mart_casa_customer  chi nhánh (+ vùng): KH cá nhân, CASA hoạt động / ngủ đông
    dep_mart_new_accounts   tháng mở x chi nhánh x sản phẩm: TK mở mới + luỹ kế trong năm

Quy ước so sánh (thống nhất mọi mart / dashboard):
    snapshot kỳ so sánh KHÔNG tồn tại -> NULL (không tự thay bằng 0)
    snapshot tồn tại nhưng không có tổ hợp dimension đó -> số dư 0 (đã tất toán / mới mở)
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DIMS = ["account_group", "group_class", "currencycd", "region", "branchnbr", "nationality"]
COMPARE_TAGS = ("d1", "m1", "y1")
# kỳ hạn (tháng) -> nhóm như cột TENNOR của bang5; biên: [lo, hi) theo tháng
TENOR_BUCKETS = [(1, "00-01M", 0, 1), (2, "01-06M", 1, 6), (3, "06-12M", 6, 12),
                 (4, "12-24M", 12, 24), (5, "24M+", 24, None)]


def _open() -> F.Column:
    return ~F.col("status").isin("CLOSED")


def _d(s: str) -> date:
    return date.fromisoformat(str(s)[:10])


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


def customers_as_of(dim_customer: DataFrame, as_of_date: str) -> DataFrame:
    """Phiên bản KH có hiệu lực tại ngày báo cáo (SCD2 [valid_from, valid_to] đóng 2 đầu),
    thay cho is_current khi dựng lại lịch sử. Bảng không có cột SCD2 -> giữ nguyên."""
    if not {"valid_from", "valid_to"} <= set(dim_customer.columns):
        return dim_customer
    d = F.lit(as_of_date).cast("date")
    return dim_customer.filter((F.col("valid_from") <= d)
                               & (F.col("valid_to").isNull() | (F.col("valid_to") >= d)))


def resolve_compare_dates(history_dates: list, as_of_date: str) -> dict[str, date | None]:
    """Ngày so sánh thực tế từ các ngày đã có dữ liệu (không bao giờ >= ngày báo cáo):
    d1 = ngày có dữ liệu liền trước; m1 = ngày cuối có dữ liệu TRONG tháng trước;
    y1 = ngày cuối có dữ liệu TRONG năm trước. Không có -> None."""
    t = _d(as_of_date)
    ds = sorted({_d(x) for x in history_dates if _d(x) < t})
    prev_m_end = t.replace(day=1) - timedelta(days=1)
    return {
        "d1": ds[-1] if ds else None,
        "m1": max((x for x in ds if (x.year, x.month) == (prev_m_end.year, prev_m_end.month)),
                  default=None),
        "y1": max((x for x in ds if x.year == t.year - 1), default=None),
    }


def _with_dims(dep: DataFrame, branches: DataFrame, customers: DataFrame | None) -> DataFrame:
    dep = dep.join(branches.select("branchnbr", "region"), "branchnbr", "left")
    if customers is not None and "nationality" in customers.columns:
        nat = customers.select("customer_id", nation_group().alias("nationality")) \
            .dropDuplicates(["customer_id"])
        return dep.join(nat, "customer_id", "left") \
            .withColumn("nationality", F.coalesce("nationality", F.lit("OTHER")))
    return dep.withColumn("nationality", F.lit("OTHER"))


def nation_group(col: str = "nationality") -> F.Column:
    """Mã quốc gia KH -> nhóm như cột NATION của bang1247 (bộ lọc Nationality).

    VN -> VIETNAMESE; mọi mã quốc gia khác -> FOREIGN; trống / NULL -> OTHER.
    """
    c = F.upper(F.trim(F.col(col)))
    return (F.when(c.isNull() | (c == "") | (c == "OTHER"), "OTHER")
            .when(c.isin("VN", "VIETNAMESE"), "VIETNAMESE")
            .otherwise("FOREIGN"))


def _snapshot(history: DataFrame, d: date | None, cols: list[str]) -> DataFrame | None:
    if d is None:
        return None
    h = history
    for c in cols:
        if c not in h.columns:
            h = h.withColumn(c, F.lit(None).cast("decimal(38,6)"))
    return h.filter(F.col("as_of_date") == F.lit(d.isoformat()).cast("date")) \
        .select(*DIMS, *cols)


def build_mart_deposit_trend(accounts: DataFrame, products: DataFrame, fx: DataFrame,
                             branches: DataFrame, as_of_date: str,
                             history: DataFrame | None = None,
                             customers: DataFrame | None = None) -> DataFrame:
    """Mart ngày T. `history` = các ngày đã publish của chính mart (chỉ đọc đúng snapshot
    D-1 / M-1 / Y-1, không join với toàn bộ lịch sử); `customers` = KH hiệu lực tại T.

    MTD average (cộng được qua mọi dimension vì cùng mẫu số = số ngày từ đầu tháng):
        avg_T = (avg_{T-1} * (n - 1) + balance_T) / n,  n = ngày trong tháng của T
    chỉ hợp lệ khi có đủ snapshot liên tục từ ngày 1 (ngày 1: avg = balance). Thiếu ngày ->
    monthly_avg_balance_vnd = NULL, monthly_avg_complete = false (không lấy số dư cuối thay).
    """
    t = _d(as_of_date)
    for c, typ in (("branchnbr", "string"), ("nationality", "string")):
        if history is not None and c not in history.columns:
            history = history.withColumn(c, F.lit(None).cast(typ))
    dep = _with_dims(_deposits_vnd(accounts, products, fx), branches, customers)
    cur = (dep.groupBy(*DIMS)
           .agg(F.sum("balance_vnd").alias("balance_vnd"),
                F.sum(_open().cast("int")).alias("open_accounts")))

    hist = None
    dates: dict[str, date | None] = {k: None for k in COMPARE_TAGS}
    complete_at: dict[date, bool] = {}
    if history is not None:
        hist = history.filter(F.col("as_of_date") < F.lit(as_of_date).cast("date"))
        flag = (F.max(F.col("monthly_avg_complete").cast("int"))
                if "monthly_avg_complete" in hist.columns else F.lit(None))
        meta = hist.groupBy("as_of_date").agg(flag.alias("c")).collect()   # vài trăm dòng
        complete_at = {r.as_of_date: bool(r.c) for r in meta}
        dates = resolve_compare_dates(list(complete_at), as_of_date)

    # khóa = hiện tại ∪ khóa còn số dư ở D-1 / M-1 / Y-1 (khoản đã tất toán vẫn có dòng, số
    # dư 0) ∪ khóa có MTD avg trong tháng (để bình quân tháng của khóa đó vẫn cộng đủ)
    keys = cur.select(*DIMS)
    snaps = {}
    for tag in COMPARE_TAGS:
        s = _snapshot(hist, dates[tag], ["balance_vnd", "monthly_avg_balance_vnd"]) \
            if hist is not None else None
        snaps[tag] = s
        if s is not None:
            keep = F.col("balance_vnd") != 0
            if tag == "d1":
                keep = keep | (F.coalesce("monthly_avg_balance_vnd", F.lit(0)) != 0)
            keys = keys.unionByName(s.filter(keep).select(*DIMS))
    out = keys.distinct()
    c2 = cur.select(*[F.col(k).alias(f"_c_{k}") for k in DIMS], "balance_vnd", "open_accounts")
    out = out.join(c2, [out[k].eqNullSafe(c2[f"_c_{k}"]) for k in DIMS], "left") \
        .drop(*[f"_c_{k}" for k in DIMS]) \
        .withColumn("balance_vnd", F.coalesce("balance_vnd", F.lit(0)).cast("decimal(38,6)")) \
        .withColumn("open_accounts", F.coalesce("open_accounts", F.lit(0)).cast("long"))
    for tag, s in snaps.items():
        if s is None:
            out = out.withColumn(f"balance_vnd_{tag}", F.lit(None).cast("decimal(38,6)")) \
                .withColumn(f"monthly_avg_balance_{tag}", F.lit(None).cast("decimal(38,6)"))
        else:
            r = s.select(*[F.col(k).alias(f"_s_{k}") for k in DIMS],
                         F.col("balance_vnd").alias("_b"),
                         F.col("monthly_avg_balance_vnd").alias("_a"))
            ok = complete_at.get(dates[tag], False)
            out = (out.join(r, [out[k].eqNullSafe(r[f"_s_{k}"]) for k in DIMS], "left")
                   .drop(*[f"_s_{k}" for k in DIMS])
                   .withColumn(f"balance_vnd_{tag}",
                               F.coalesce("_b", F.lit(0)).cast("decimal(38,6)"))
                   .withColumn(f"monthly_avg_balance_{tag}",
                               (F.coalesce("_a", F.lit(0)) if ok else F.lit(None))
                               .cast("decimal(38,6)"))
                   .drop("_b", "_a"))
        out = out.withColumn(f"comparison_date_{tag}",
                             F.lit(dates[tag].isoformat() if dates[tag] else None).cast("date"))
        out = out.withColumn(f"diff_{tag}", F.col("balance_vnd") - F.col(f"balance_vnd_{tag}"))

    # MTD average
    n = t.day
    d1 = dates["d1"]
    if n == 1:
        complete, avg = True, F.col("balance_vnd")
    elif d1 == t - timedelta(days=1) and complete_at.get(d1, False):
        complete = True
        avg = (F.col("monthly_avg_balance_d1") * (n - 1) + F.col("balance_vnd")) / n
    else:
        complete, avg = False, F.lit(None)
    out = (out.withColumn("monthly_avg_balance_vnd", avg.cast("decimal(38,6)"))
           .withColumn("monthly_avg_complete", F.lit(complete))
           .withColumn("as_of_date", F.lit(as_of_date).cast("date")))
    # tỷ trọng: tham khảo, dashboard KHÔNG cộng cột này (tính lại từ balance_vnd)
    total = F.sum("balance_vnd").over(Window.partitionBy("as_of_date"))
    out = out.withColumn("share_pct", F.col("balance_vnd").cast("double")
                         / F.when(total != 0, total).cast("double") * 100)
    cols = ["as_of_date", *DIMS, "balance_vnd", "open_accounts", "share_pct",
            "monthly_avg_balance_vnd", "monthly_avg_complete"]
    for tag in COMPARE_TAGS:
        cols += [f"balance_vnd_{tag}", f"diff_{tag}", f"monthly_avg_balance_{tag}",
                 f"comparison_date_{tag}"]
    return out.select(*cols)


def build_mart_deposit_monthly(trend_day: DataFrame, as_of_date: str,
                               previous: DataFrame | None = None,
                               trend_history: DataFrame | None = None) -> DataFrame:
    """dep_mart_monthly tại ngày T (as_of_date = T): từ tháng 1 năm trước đến tháng của T.

    Tháng hiện tại lấy snapshot T (không đọc ngày sau T). Tháng trước đó:
      - `previous` (mart tháng của ngày publish trước, cùng tháng hoặc tháng trước): sao chép
        tháng đã có -> chỉ tính lại tháng hiện tại (incremental);
      - không có `previous` (lần đầu / sau nâng cấp): dựng từ `trend_history` (snapshot cuối
        có dữ liệu mỗi tháng).
    Bình quân tháng chỉ có khi MTD avg của snapshot đó đủ ngày; không thay bằng số dư cuối."""
    t = _d(as_of_date)
    start = date(t.year - 1, 1, 1).isoformat()
    month_now = f"{t:%Y-%m}"

    def shape(df: DataFrame) -> DataFrame:
        return df.select(
            F.date_format("as_of_date", "yyyy-MM").alias("month_id"),
            F.col("as_of_date").alias("snapshot_date"), *DIMS,
            F.col("balance_vnd").alias("ending_balance_vnd"),
            "monthly_avg_balance_vnd", "monthly_avg_complete")

    cur = shape(trend_day.filter(F.col("as_of_date") == F.lit(as_of_date).cast("date")))
    if previous is not None:
        past = previous.filter((F.col("month_id") < month_now) & (F.col("month_id") >= start[:7]))
        past = past.select(*cur.columns)
    elif trend_history is not None:
        h = trend_history.filter((F.col("as_of_date") < F.lit(f"{month_now}-01").cast("date"))
                                 & (F.col("as_of_date") >= F.lit(start).cast("date")))
        if "monthly_avg_complete" not in h.columns:
            h = h.withColumn("monthly_avg_balance_vnd", F.lit(None).cast("decimal(38,6)")) \
                .withColumn("monthly_avg_complete", F.lit(False))
        last = h.groupBy(F.date_format("as_of_date", "yyyy-MM").alias("_m")) \
            .agg(F.max("as_of_date").alias("_d"))
        past = shape(h.join(last, h.as_of_date == last._d).drop("_m", "_d"))
    else:
        past = None
    out = cur if past is None else past.unionByName(cur)
    return (out.withColumn("monthly_avg_balance_vnd",
                           F.when(F.col("monthly_avg_complete"), F.col("monthly_avg_balance_vnd")))
            .withColumn("is_month_closed", F.col("snapshot_date") == F.last_day("snapshot_date"))
            .withColumn("as_of_date", F.lit(as_of_date).cast("date")))


def tenor_bucket_cols(col: str = "tenor_months") -> tuple[F.Column, F.Column]:
    lab, order = None, None
    for sort, name, lo, hi in TENOR_BUCKETS:
        cond = F.col(col) >= lo if hi is None else (F.col(col) >= lo) & (F.col(col) < hi)
        lab = (F.when(cond, name) if lab is None else lab.when(cond, name))
        order = (F.when(cond, sort) if order is None else order.when(cond, sort))
    return lab, order


def build_mart_deposit_by_tenor(accounts: DataFrame, products: DataFrame, fx: DataFrame,
                                as_of_date: str, branches: DataFrame | None = None) -> DataFrame:
    """Chỉ sản phẩm có kỳ hạn (TD / ISA, tenor_months > 0; CASA không thuộc biểu đồ này).
    Lãi suất (int_rate, dạng thập phân 0.05 = 5%/năm) bình quân gia quyền theo số dư:
    lưu tử số / mẫu số để cộng qua chi nhánh / vùng, không AVG các tỷ lệ đã tính."""
    d = _deposits_vnd(accounts, products, fx).filter(_open() & (F.col("tenor_months") > 0))
    if branches is not None:
        d = d.join(branches.select("branchnbr", "region"), "branchnbr", "left")
    else:
        d = d.withColumn("region", F.lit(None).cast("string"))
    lab, order = tenor_bucket_cols()
    d = d.withColumn("tenor_bucket", lab).withColumn("tenor_sort_order", order)
    return (d.groupBy("group_class", "product_type", "tenor_months", "tenor_bucket",
                      "tenor_sort_order", "currencycd", "region", "branchnbr")
            .agg(F.count("*").alias("accounts"),
                 F.sum("balance_vnd").alias("balance_vnd"),
                 F.min("int_rate").alias("min_rate"), F.max("int_rate").alias("max_rate"),
                 F.avg("int_rate").alias("avg_rate"),
                 F.sum(F.col("int_rate") * F.col("balance_vnd"))
                 .alias("interest_weighted_numerator"),
                 F.sum("balance_vnd").alias("interest_weighted_denominator"))
            .withColumn("weighted_rate", F.col("interest_weighted_numerator")
                        / F.when(F.col("interest_weighted_denominator") != 0,
                                 F.col("interest_weighted_denominator")))
            .withColumn("as_of_date", F.lit(as_of_date).cast("date")))


def build_mart_casa_customer(accounts: DataFrame, products: DataFrame,
                             customers: DataFrame, as_of_date: str,
                             branches: DataFrame | None = None) -> DataFrame:
    """customers: KH hiệu lực tại as_of_date (customers_as_of). Mỗi KH đếm 1 lần ở chi nhánh
    của KH. 'CASA hoạt động' = có >= 1 TK CASA ACTIVE; 'ngủ đông' = có CASA nhưng không TK nào
    ACTIVE. active + dormant = casa; casa <= retail. Vùng lưu theo chi nhánh tại ngày đó."""
    p = products.filter("product_group = '1-CASA'").select(F.col("prodcode").alias("product_type"))
    casa = accounts.join(p, "product_type").filter(_open()).groupBy("customer_id").agg(
        F.max((F.col("status") == "ACTIVE").cast("int")).alias("has_active"))
    retail = customers_as_of(customers, as_of_date).filter("cust_type = 'CN'") \
        .select("customer_id", "branchnbr").dropDuplicates(["customer_id"])
    j = retail.join(casa, "customer_id", "left")
    out = (j.groupBy("branchnbr")
           .agg(F.count("*").alias("retail_customers"),
                F.count("has_active").alias("casa_customers"),
                F.sum(F.coalesce("has_active", F.lit(0))).alias("casa_active_customers"))
           .withColumn("casa_dormant_customers",
                       F.col("casa_customers") - F.col("casa_active_customers"))
           .withColumn("non_casa_customers",
                       F.col("retail_customers") - F.col("casa_customers"))
           .withColumn("casa_penetration_pct",
                       F.col("casa_customers") / F.col("retail_customers") * 100)
           .withColumn("as_of_date", F.lit(as_of_date).cast("date")))
    if branches is not None:
        return out.join(branches.select("branchnbr", "region"), "branchnbr", "left")
    return out.withColumn("region", F.lit(None).cast("string"))


def build_mart_deposit_new_accounts(accounts: DataFrame, as_of_date: str | None = None,
                                    branches: DataFrame | None = None) -> DataFrame:
    """TK mở mới theo tháng mở + luỹ kế trong năm (reset tháng 1), tại ngày as_of_date.

    Mỗi TK đếm 1 lần (bảng nguồn có thể giữ nhiều snapshot của cùng TK -> dedupe theo
    account_id, lấy open_date sớm nhất). Bỏ TK mở sau as_of_date. Giữ từ tháng 1 năm trước
    (tổng năm trước + luỹ kế năm nay cho K-Study)."""
    a = accounts.filter(F.col("product_type") != "LOAN")
    if as_of_date:
        a = a.filter(F.col("open_date") <= F.lit(as_of_date).cast("date"))
        start = date(_d(as_of_date).year - 1, 1, 1).isoformat()
        a = a.filter(F.col("open_date") >= F.lit(start).cast("date"))
    a = a.groupBy("account_id").agg(F.min("open_date").alias("open_date"),
                                    F.first("branchnbr", ignorenulls=True).alias("branchnbr"),
                                    F.first("product_type", ignorenulls=True)
                                    .alias("product_type"))
    m = (a.withColumn("open_month", F.date_format("open_date", "yyyy-MM"))
         .groupBy("open_month", "branchnbr", "product_type")
         .agg(F.count("*").alias("new_accounts")))
    w = Window.partitionBy("branchnbr", "product_type", F.substring("open_month", 1, 4)) \
        .orderBy("open_month").rowsBetween(Window.unboundedPreceding, 0)
    m = m.withColumn("new_accounts_ytd", F.sum("new_accounts").over(w))
    if branches is not None:
        m = m.join(branches.select("branchnbr", "region"), "branchnbr", "left")
    else:
        m = m.withColumn("region", F.lit(None).cast("string"))
    return m
