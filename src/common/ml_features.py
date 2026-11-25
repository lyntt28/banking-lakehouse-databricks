"""Feature table + model cho bài toán bán chéo bảo hiểm (insurance cross-sell propensity).

Bài toán: KH cá nhân CHƯA có hợp đồng bảo hiểm đang hiệu lực -> xác suất sẽ mua.
Nhãn (label): KH đang có hợp đồng INFORCE tại ngày as_of (học "KH giống người đã mua").
Khi có đủ lịch sử, `label_mode="future"` lấy nhãn = mua MỚI trong `horizon_days` sau as_of,
feature chỉ dùng dữ liệu <= as_of (point-in-time, không rò rỉ nhãn).

Feature (1 dòng / KH / as_of_date) - tất cả từ Silver, không dùng bảng bảo hiểm:
    hồ sơ      : tuổi, thu nhập (log), segment, số năm quan hệ, tỉnh
    tiền gửi   : số TK mở, tổng số dư VND, có TD / ISA, tỷ trọng CASA
    vay        : số khoản vay, dư nợ, DPD lớn nhất
    thẻ        : số thẻ, hạng thẻ cao nhất, hạn mức, chi tiêu 30 ngày, tỷ lệ chi du lịch /
                 y tế / giáo dục (MCC 4511, 7011, 8062, 8220), giao dịch nước ngoài
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LIFESTYLE_MCC = ["4511", "7011", "8062", "8220"]
GRADE_RANK = {"CLASSIC": 1, "GOLD": 2, "PLATINUM": 3, "PREMIUM": 4, "CORPORATE": 2}
SEGMENTS = ["MASS", "AFFLUENT", "PRIORITY", "SME"]

NUMERIC_FEATURES = [
    "age", "log_income", "tenure_years", "is_affluent_plus",
    "deposit_accounts", "deposit_balance_vnd_log", "has_term_deposit", "has_isa", "casa_share",
    "loan_count", "loan_balance_vnd_log", "max_dpd",
    "card_count", "max_card_grade", "credit_limit_log", "card_spend_30d_log",
    "lifestyle_spend_share", "xborder_spend_share", "card_txn_30d",
] + [f"segment_{s.lower()}" for s in SEGMENTS]


def _log1p(c: str) -> F.Column:
    return F.log1p(F.greatest(F.coalesce(F.col(c).cast("double"), F.lit(0.0)), F.lit(0.0)))


def build_customer_features(as_of_date: str, customers: DataFrame, accounts: DataFrame,
                            loans: DataFrame, cards: DataFrame, card_txns: DataFrame,
                            fx: DataFrame | None = None, window_days: int = 30) -> DataFrame:
    """customers: bản hiện hành tại as_of; loans: snapshot ngày as_of; card_txns: giao dịch
    hợp lệ (lọc <= as_of trong hàm). Chỉ KH cá nhân (CN)."""
    day = F.lit(as_of_date).cast("date")
    c = customers.filter("cust_type = 'CN'").select(
        "customer_id", "segment", "province",
        (F.months_between(day, "dob") / 12).alias("age"),
        F.log1p(F.coalesce(F.col("monthly_income").cast("double"), F.lit(0.0))).alias("log_income"),
        (F.months_between(day, "open_date") / 12).alias("tenure_years"),
        F.col("segment").isin("AFFLUENT", "PRIORITY").cast("int").alias("is_affluent_plus"),
        *[(F.col("segment") == s).cast("int").alias(f"segment_{s.lower()}") for s in SEGMENTS],
    )

    rate = (fx.select("currencycd", "rate_to_vnd") if fx is not None else None)
    dep = accounts.filter((F.col("product_type") != "LOAN") & (F.col("status") != "CLOSED"))
    dep = dep.join(rate, "currencycd", "left") if rate is not None else \
        dep.withColumn("rate_to_vnd", F.lit(1.0))
    dep = dep.withColumn("bal_vnd", F.col("balance") * F.coalesce("rate_to_vnd", F.lit(1.0)))
    dep_f = dep.groupBy("customer_id").agg(
        F.count("*").alias("deposit_accounts"),
        F.sum("bal_vnd").alias("deposit_balance_vnd"),
        F.max((F.col("product_type") == "TD").cast("int")).alias("has_term_deposit"),
        F.max((F.col("product_type") == "ISA").cast("int")).alias("has_isa"),
        (F.sum(F.when(F.col("product_type").isin("CASA", "PAYROLL", "EDUSAVE"), F.col("bal_vnd"))
               .otherwise(0)) / F.sum("bal_vnd")).alias("casa_share"),
    )

    loan_f = loans.filter(F.col("as_of_date") == day).groupBy("customer_id").agg(
        F.count("*").alias("loan_count"),
        F.sum(F.coalesce("ducuoiqd", "ducuoi")).alias("loan_balance_vnd"),
        F.max("songay_nqh").alias("max_dpd"),
    )

    rank = F.create_map(*[x for k, v in GRADE_RANK.items() for x in (F.lit(k), F.lit(v))])
    act_cards = cards.filter("status = 'ACTIVE'")
    card_f = act_cards.groupBy("customer_id").agg(
        F.count("*").alias("card_count"),
        F.max(rank[F.col("card_grade")]).alias("max_card_grade"),
        F.sum("credit_limit").alias("credit_limit"),
    )
    since = F.date_sub(day, window_days - 1)
    tx = (card_txns.filter(F.to_date("txn_ts").between(since, day))
          .join(cards.select("card_no", "customer_id"), "card_no"))
    spend_f = tx.groupBy("customer_id").agg(
        F.count("*").alias("card_txn_30d"),
        F.sum("amount").alias("card_spend_30d"),
        (F.sum(F.when(F.col("mcc").isin(*LIFESTYLE_MCC), F.col("amount")).otherwise(0))
         / F.sum("amount")).alias("lifestyle_spend_share"),
        (F.sum(F.when(F.col("location") == "XBORDER", F.col("amount")).otherwise(0))
         / F.sum("amount")).alias("xborder_spend_share"),
    )

    out = c
    for df in (dep_f, loan_f, card_f, spend_f):
        out = out.join(df, "customer_id", "left")
    out = (out.withColumn("deposit_balance_vnd_log", _log1p("deposit_balance_vnd"))
           .withColumn("loan_balance_vnd_log", _log1p("loan_balance_vnd"))
           .withColumn("credit_limit_log", _log1p("credit_limit"))
           .withColumn("card_spend_30d_log", _log1p("card_spend_30d"))
           .drop("deposit_balance_vnd", "loan_balance_vnd", "credit_limit", "card_spend_30d"))
    fill = [x for x in NUMERIC_FEATURES if x not in ("age", "log_income", "tenure_years")]
    return (out.fillna(0, subset=fill)
            .withColumn("as_of_date", day)
            .select("customer_id", "as_of_date", "segment", "province", *NUMERIC_FEATURES))


def build_labels(policies: DataFrame, as_of_date: str, mode: str = "holder",
                 horizon_days: int = 90) -> DataFrame:
    """(customer_id, label). holder: có HĐ INFORCE bắt đầu <= as_of.
    future: mua HĐ mới trong (as_of, as_of + horizon] - dùng khi đã có lịch sử đủ dài."""
    day = F.lit(as_of_date).cast("date")
    p = policies.filter(F.col("customer_id").isNotNull())
    if mode == "holder":
        pos = p.filter((F.col("status") == "INFORCE") & (F.col("start_date") <= day))
    elif mode == "future":
        pos = p.filter((F.col("start_date") > day)
                       & (F.col("start_date") <= F.date_add(day, horizon_days)))
    else:
        raise ValueError(mode)
    return pos.select("customer_id").distinct().withColumn("label", F.lit(1))


def training_set(features: DataFrame, labels: DataFrame, policies: DataFrame | None = None,
                 as_of_date: str | None = None, mode: str = "holder") -> DataFrame:
    """Nối feature + nhãn (thiếu = 0). Ở mode future bỏ KH đã có HĐ trước as_of
    (không còn là khách tiềm năng)."""
    df = features.join(labels, "customer_id", "left").fillna(0, subset=["label"])
    if mode == "future" and policies is not None and as_of_date:
        had = policies.filter(F.col("start_date") <= F.lit(as_of_date).cast("date")) \
            .select("customer_id").distinct()
        df = df.join(had, "customer_id", "left_anti")
    return df


# ---------------------------------------------------------------- model (pandas / sklearn)
def train_model(pdf, seed: int = 42, test_size: float = 0.25):
    """Huấn luyện HistGradientBoosting trên pandas DataFrame từ training_set().
    Trả về (model, metrics). Không phụ thuộc MLflow -> test được local."""
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.metrics import average_precision_score, roc_auc_score
    from sklearn.model_selection import train_test_split

    X = pdf[NUMERIC_FEATURES].astype(float)
    y = pdf["label"].astype(int)
    X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=test_size, random_state=seed,
                                              stratify=y)
    model = HistGradientBoostingClassifier(max_iter=200, learning_rate=0.05,
                                           class_weight="balanced", random_state=seed)
    model.fit(X_tr, y_tr)
    p = model.predict_proba(X_te)[:, 1]
    return model, {
        "roc_auc": float(roc_auc_score(y_te, p)),
        "pr_auc": float(average_precision_score(y_te, p)),
        "base_rate": float(y_te.mean()),
        "lift_top10": lift_at(y_te.to_numpy(), p, 0.10),
        "n_train": int(len(y_tr)), "n_test": int(len(y_te)),
    }


def lift_at(y, scores, top: float) -> float:
    """Tỷ lệ mua trong top x% điểm cao nhất / tỷ lệ mua chung (lift cho chiến dịch)."""
    import numpy as np

    n = max(1, int(len(y) * top))
    idx = np.argsort(-scores)[:n]
    base = y.mean()
    return float(y[idx].mean() / base) if base else 0.0
