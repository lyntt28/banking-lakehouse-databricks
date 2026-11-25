"""Silver: bronze (mọi cột STRING) -> dữ liệu đã ép kiểu, khử trùng, qua DQ gate.

Luồng cho mỗi bảng (xem `prepare_silver`):
    1. Lọc theo _as_of_date, bỏ dòng trùng HOÀN TOÀN (file gửi lặp) -> đếm `exact_dupes`.
    2. Ép kiểu an toàn bằng try_cast / try_to_timestamp: giá trị sai định dạng thành NULL
       (không làm hỏng job khi Databricks bật ANSI mode).
    3. Thêm cột hỗ trợ cho rule: `_dup_key_count` (trùng khóa nhưng khác nội dung),
       `_fk_ok` (khóa ngoại tồn tại), `_raw_*` (giá trị gốc: phân biệt "trống" / "sai định dạng").
    4. `apply_dq` tách valid / quarantine. Đối soát: bronze = silver + quarantine + exact_dupes.

Phần ghi Delta (MERGE / SCD2) nằm trong notebook src/02_silver, vì cần Delta Lake.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from common.banking import debt_group_col
from common.dq import Rule, apply_dq

TECH_COLS = ["_source_file", "_as_of_date", "_load_ts", "_rescued_data"]


# ---------------------------------------------------------------- ép kiểu an toàn
def _blank_to_null(c: str) -> Column:
    s = F.trim(F.col(c))
    return F.when(s == "", F.lit(None)).otherwise(s)


def to_date_safe(c: str, *fmts: str) -> Column:
    """Thử lần lượt các format, sai hết -> NULL (không lỗi dưới ANSI mode)."""
    tries = [F.expr(f"try_to_timestamp(`{c}`, '{f}')") for f in fmts]
    return F.coalesce(*tries).cast("date")


def to_ts_safe(c: str, *fmts: str) -> Column:
    return F.coalesce(*[F.expr(f"try_to_timestamp(`{c}`, '{f}')") for f in fmts])


def to_num(c: str, dtype: str = "decimal(22,2)") -> Column:
    return F.expr(f"try_cast(nullif(trim(`{c}`), '') AS {dtype})")


def not_blank(c: str) -> str:
    return f"_raw_{c} IS NOT NULL"


def parsed_if_present(c: str) -> str:
    """Rule: nếu nguồn có giá trị thì phải parse được (trống thì để rule khác xử lý)."""
    return f"_raw_{c} IS NULL OR {c} IS NOT NULL"


# ---------------------------------------------------------------- cấu hình từng bảng
@dataclass(frozen=True)
class SilverTable:
    name: str
    key: list[str]
    cast: dict[str, Callable[[], Column]]   # cột đích -> hàm tạo biểu thức
    rules: list[Rule]
    raw_cols: list[str] = field(default_factory=list)   # giữ _raw_<c> để viết rule
    fk: tuple[str, str] | None = None       # (cột, bảng tham chiếu) -> cột _fk_ok
    write_mode: str = "merge"               # merge | scd2 | append
    optional: list[str] = field(default_factory=list)   # cột nguồn mới, file cũ có thể thiếu


D_MDY = "MM/dd/yyyy"


def _lazy(fn: Callable[..., Column]) -> Callable[..., Callable[[], Column]]:
    """Column chỉ tạo được khi có SparkSession -> cấu hình lưu hàm, gọi lúc chạy."""
    return lambda *a: (lambda: fn(*a))


_str = _lazy(_blank_to_null)
_date = _lazy(to_date_safe)
_ts = _lazy(to_ts_safe)
_num = _lazy(to_num)



BRANCHES = SilverTable(
    name="branches",
    key=["branchnbr"],
    cast={
        "branchnbr": _str("branchnbr"), "br_area": _str("br_area"), "br_name": _str("br_name"),
        "region": _str("region"), "province": _str("province"),
        "open_date": _date("open_date", D_MDY, "yyyy-MM-dd"),
    },
    rules=[
        Rule("branchnbr_not_null", "branchnbr IS NOT NULL"),
        Rule("branch_key_unique", "_dup_key_count = 1"),
    ],
)

PRODUCTS = SilverTable(
    name="products",
    key=["prodcode"],
    cast={
        "prodcode": _str("prodcode"), "product_name": _str("product_name"),
        "product_family": _str("product_family"), "mjacctcd": _str("mjacctcd"),
        "miacctcd": _str("miacctcd"), "product_group": _str("product_group"),
        "scope": _str("scope"), "term_months": _num("term_months", "int"),
    },
    rules=[
        Rule("prodcode_not_null", "prodcode IS NOT NULL"),
        Rule("product_key_unique", "_dup_key_count = 1"),
        Rule("product_family_known", "product_family IN ('LOAN', 'DEPOSIT')", "WARN"),
    ],
)

CUSTOMERS = SilverTable(
    name="customers",
    key=["customer_id"],
    cast={
        "customer_id": _str("customer_id"), "cust_type": _str("cust_type"),
        "full_name": _str("full_name"), "id_no": _str("id_no"),
        "dob": _date("dob", D_MDY), "gender": _str("gender"), "phone": _str("phone"),
        "email": _str("email"), "address": _str("address"), "province": _str("province"),
        "branchnbr": _str("branchnbr"), "segment": _str("segment"),
        "monthly_income": _num("monthly_income"),
        "open_date": _date("open_date", D_MDY),
        "updated_at": _ts("updated_at", "yyyy-MM-dd HH:mm:ss"),
        "nationality": _str("nationality"),
    },
    raw_cols=["dob"],
    optional=["nationality"],
    rules=[
        Rule("customer_id_not_null", "customer_id IS NOT NULL"),
        Rule("customer_key_unique", "_dup_key_count = 1"),
        Rule("dob_valid_format", parsed_if_present("dob")),
        Rule("id_no_length", "length(id_no) IN (9, 12)", "WARN"),          # CMND 9 / CCCD 12
        Rule("individual_age_18", "cust_type <> 'CN' OR dob <= add_months(_as_of_date, -216)",
             "WARN"),
    ],
    write_mode="scd2",
)
CUSTOMER_TRACKED = ["cust_type", "full_name", "id_no", "dob", "gender", "phone", "email",
                    "address", "province", "branchnbr", "segment", "monthly_income"]

ACCOUNTS = SilverTable(
    name="accounts",
    key=["account_id"],
    cast={
        "account_id": _str("account_id"), "customer_id": _str("customer_id"),
        "branchnbr": _str("branchnbr"), "product_type": _str("product_type"),
        "currencycd": _str("currencycd"), "status": _str("status"),
        "open_date": _date("open_date", D_MDY),
        "close_date": _date("close_date", D_MDY),
        "balance": _num("balance"), "int_rate": _num("int_rate", "decimal(9,5)"),
        "tenor_months": _num("tenor_months", "int"),
    },
    optional=["tenor_months"],
    fk=("customer_id", "customers"),
    rules=[
        Rule("account_id_not_null", "account_id IS NOT NULL"),
        Rule("account_key_unique", "_dup_key_count = 1"),
        Rule("customer_exists", "_fk_ok"),
        Rule("status_known", "status IN ('ACTIVE', 'CLOSED', 'DORMANT')", "WARN"),
        Rule("balance_not_negative", "balance >= 0", "WARN"),
    ],
)

LOANS = SilverTable(
    name="loans",
    key=["as_of_date", "acctnbr"],          # snapshot dư nợ theo ngày
    cast={
        "as_of_date": _date("etl_dt", D_MDY),
        "branchnbr": _str("branchnbr"), "acctnbr": _str("acctnbr"),
        "customer_id": _str("customer_id"), "scope": _str("scope"),
        "mjacctcd": _str("mjacctcd"), "miacctcd": _str("miacctcd"),
        "prodcode": _str("prodcode"), "currencycd": _str("currencycd"),
        "int_rate": _num("int_rate", "decimal(9,5)"),
        "datestart": _date("datestart", f"{D_MDY} HH:mm:ss", D_MDY),
        "datemat": _date("datemat", f"{D_MDY} HH:mm:ss", D_MDY),
        "loan_dur": _num("loan_dur", "int"),
        "dudau": _num("dudau"), "chovay": _num("chovay"), "thuno": _num("thuno"),
        "thulai": _num("thulai"), "ducuoi": _num("ducuoi"), "ducuoiqd": _num("ducuoiqd"),
        "songay_nqh": _num("songay_nqh", "int"), "nhom_no": _num("nhom_no", "int"),
        "offnbr": _str("offnbr"), "ftp_rate": _num("ftp_rate", "decimal(9,5)"),
        "acctstatcd": _str("acctstatcd"), "nextdue": _date("nextdue", D_MDY),
    },
    raw_cols=["datestart", "ducuoi"],
    rules=[
        Rule("acctnbr_not_null", "acctnbr IS NOT NULL"),
        Rule("loan_key_unique", "_dup_key_count = 1"),
        Rule("as_of_date_matches_file", "as_of_date = _as_of_date"),
        Rule("datestart_valid_format", parsed_if_present("datestart")),
        Rule("balance_not_null", not_blank("ducuoi")),
        Rule("balance_not_negative", "ducuoi >= 0"),
        Rule("start_before_maturity", "datestart <= datemat"),
        Rule("debt_group_in_range", "nhom_no BETWEEN 1 AND 5"),
        Rule("debt_group_matches_dpd", "nhom_no = debt_group_calc", "WARN"),
        Rule("int_rate_below_40pct", "int_rate <= 0.4", "WARN"),
    ],
)

TRANSACTIONS = SilverTable(
    name="transactions",
    key=["txn_id"],
    cast={
        "txn_id": _str("txn_id"), "account_id": _str("account_id"),
        "txn_ts": _ts("txn_ts", "yyyy-MM-dd HH:mm:ss"),
        "value_date": _date("value_date", "yyyy-MM-dd"),
        "amount": _num("amount"), "dr_cr": _str("dr_cr"), "currencycd": _str("currencycd"),
        "channel": _str("channel"), "txn_type": _str("txn_type"),
        "description": _str("description"), "teller_user": _str("teller_user"),
        "branchnbr": _str("branchnbr"),
    },
    raw_cols=["txn_ts"],
    fk=("account_id", "accounts"),
    rules=[
        Rule("txn_id_not_null", "txn_id IS NOT NULL"),
        Rule("txn_key_unique", "_dup_key_count = 1"),
        Rule("amount_positive", "amount > 0"),
        Rule("account_exists", "_fk_ok"),
        Rule("txn_ts_valid_format", parsed_if_present("txn_ts")),
        Rule("txn_ts_near_value_date", "abs(datediff(to_date(txn_ts), value_date)) <= 1", "WARN"),
        Rule("dr_cr_known", "dr_cr IN ('D', 'C')", "WARN"),
    ],
    write_mode="append",
)

# ---- tham chiếu: tỷ giá, chỉ tiêu kế hoạch
FX_RATES = SilverTable(
    name="fx_rates",
    key=["rate_date", "currencycd"],
    cast={"rate_date": _date("rate_date", "yyyy-MM-dd"), "currencycd": _str("currencycd"),
          "rate_to_vnd": _num("rate_to_vnd", "decimal(18,4)")},
    rules=[
        Rule("fx_key_not_null", "rate_date IS NOT NULL AND currencycd IS NOT NULL"),
        Rule("fx_key_unique", "_dup_key_count = 1"),
        Rule("fx_rate_positive", "rate_to_vnd > 0"),
    ],
)

TARGETS = SilverTable(
    name="targets",
    key=["metric_code", "period_type", "period", "branchnbr"],
    cast={"metric_code": _str("metric_code"), "period_type": _str("period_type"),
          "period": _str("period"), "branchnbr": _str("branchnbr"),
          "target_value": _num("target_value", "decimal(22,2)")},
    rules=[
        Rule("target_key_not_null", "metric_code IS NOT NULL AND period IS NOT NULL "
                                    "AND branchnbr IS NOT NULL"),
        Rule("target_key_unique", "_dup_key_count = 1"),
        Rule("period_type_known", "period_type IN ('MONTH', 'YEAR')"),
        Rule("target_not_negative", "target_value >= 0", "WARN"),
    ],
)

# ---- thẻ tín dụng
CARDS = SilverTable(
    name="cards",
    key=["card_no"],
    cast={
        "card_no": _str("card_no"), "customer_id": _str("customer_id"),
        "cust_type": _str("cust_type"), "branchnbr": _str("branchnbr"),
        "card_product": _str("card_product"), "card_grade": _str("card_grade"),
        "credit_limit": _num("credit_limit"), "open_date": _date("open_date", "yyyy-MM-dd"),
        "status": _str("status"), "close_date": _date("close_date", "yyyy-MM-dd"),
        "snapshot_date": _date("snapshot_date", "yyyy-MM-dd"),
        "acquisition_channel": _str("acquisition_channel"),
    },
    optional=["acquisition_channel"],
    rules=[
        Rule("card_no_not_null", "card_no IS NOT NULL"),
        Rule("card_key_unique", "_dup_key_count = 1"),
        Rule("card_status_known", "status IN ('ACTIVE', 'CLOSED')", "WARN"),
        Rule("credit_limit_positive", "credit_limit > 0", "WARN"),
    ],
)

CARD_TRANSACTIONS = SilverTable(
    name="card_transactions",
    key=["txn_id"],
    cast={
        "txn_id": _str("txn_id"), "card_no": _str("card_no"),
        "txn_ts": _ts("txn_ts", "yyyy-MM-dd HH:mm:ss"), "amount": _num("amount"),
        "currencycd": _str("currencycd"), "txn_type": _str("txn_type"), "mcc": _str("mcc"),
        "merchant_name": _str("merchant_name"), "channel": _str("channel"),
        "location": _str("location"),
    },
    fk=("card_no", "cards"),
    rules=[
        Rule("card_txn_id_not_null", "txn_id IS NOT NULL"),
        Rule("card_txn_key_unique", "_dup_key_count = 1"),
        Rule("card_txn_amount_positive", "amount > 0"),
        Rule("card_exists", "_fk_ok"),
        Rule("card_txn_type_known",
             "txn_type IN ('PURCHASE', 'CASH_ADVANCE', 'INSTALLMENT', 'FLEXI')", "WARN"),
    ],
    write_mode="append",
)

CARD_STATEMENTS = SilverTable(
    name="card_statements",
    key=["stnd_dt", "card_no"],
    cast={
        "stnd_dt": _date("stnd_dt", "yyyy-MM-dd"), "card_no": _str("card_no"),
        "outstanding_balance": _num("outstanding_balance"),
        "revolving_balance": _num("revolving_balance"),
        "min_payment_due": _num("min_payment_due"), "dpd": _num("dpd", "int"),
        "credit_limit": _num("credit_limit"),
    },
    fk=("card_no", "cards"),
    rules=[
        Rule("statement_key_not_null", "stnd_dt IS NOT NULL AND card_no IS NOT NULL"),
        Rule("statement_key_unique", "_dup_key_count = 1"),
        Rule("statement_card_exists", "_fk_ok"),
        Rule("statement_balance_not_negative", "outstanding_balance >= 0"),
        Rule("statement_limit_positive", "credit_limit > 0", "WARN"),
    ],
)

# ---- file tổng hợp của hệ thống nguồn (để đối soát với Gold)
CARD_KPI = SilverTable(
    name="card_kpi",
    key=["trx_date", "metric_code"],
    cast={
        "trx_date": _date("Trx_Date", "yyyy-MM-dd"), "metric_code": _str("Metric_Code"),
        # "219,627" -> 219627 ; "56.78%" -> 56.78 (đơn vị %)
        "metric_value": lambda: F.expr(
            "try_cast(regexp_replace(trim(Metric_Value), '[,%]', '') AS decimal(24,4))"),
        "is_rate": lambda: F.trim(F.col("Metric_Value")).endswith("%"),
    },
    rules=[
        Rule("kpi_key_not_null", "trx_date IS NOT NULL AND metric_code IS NOT NULL"),
        Rule("kpi_key_unique", "_dup_key_count = 1"),
        Rule("kpi_value_numeric", "metric_value IS NOT NULL"),
    ],
)

DEPOSIT_SUMMARY = SilverTable(
    name="deposit_summary",
    key=["stnd_dt", "account_group", "currencycd", "branchnbr"],
    cast={
        "stnd_dt": _date("STND_DT", "yyyyMMdd"), "account_group": _str("ACCOUNT_GROUP"),
        "group_class": _str("GROUP_CLASS"), "currencycd": _str("CURRENCY"),
        "branchnbr": _str("MANAGE_BRANCH"), "region": _str("REGION"),
        "balance_mil_vnd": _num("BAL", "decimal(22,2)"),
        "share_pct": _num("PORT", "decimal(9,2)"),
        "diff_d1_mil_vnd": _num("DIFF_T_T1", "decimal(22,4)"),
        "month_avg_bal_mil_vnd": _num("MON_AVG_BAL_T", "decimal(22,2)"),
    },
    rules=[
        Rule("dep_sum_key_not_null", "stnd_dt IS NOT NULL AND account_group IS NOT NULL"),
        Rule("dep_sum_key_unique", "_dup_key_count = 1"),
        Rule("dep_sum_balance_numeric", "balance_mil_vnd IS NOT NULL"),
    ],
)

TABLES = {t.name: t for t in [BRANCHES, PRODUCTS, CUSTOMERS, ACCOUNTS, LOANS, TRANSACTIONS,
                               FX_RATES, TARGETS, CARDS, CARD_TRANSACTIONS, CARD_STATEMENTS,
                               CARD_KPI, DEPOSIT_SUMMARY]}
# Thứ tự chạy: bảng tham chiếu trước (FK: accounts -> customers, transactions -> accounts,
# card_transactions / card_statements -> cards)
ORDER = ["branches", "products", "customers", "accounts", "loans", "transactions",
         "fx_rates", "targets", "cards", "card_transactions", "card_statements",
         "card_kpi", "deposit_summary"]


# ---------------------------------------------------------------- các bước
def drop_exact_duplicates(df: DataFrame) -> tuple[DataFrame, int]:
    """Bỏ dòng trùng y hệt (không tính cột kỹ thuật). Trả về (df, số dòng đã bỏ)."""
    data_cols = [c for c in df.columns if c not in TECH_COLS]
    deduped = df.dropDuplicates(data_cols)
    return deduped, df.count() - deduped.count()


def cast_columns(df: DataFrame, spec: SilverTable) -> DataFrame:
    """Chọn cột đích đã ép kiểu + _raw_* + cột kỹ thuật (_source_file, _as_of_date, _load_ts)."""
    for c in spec.optional:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    sel = [make().alias(name) for name, make in spec.cast.items()]
    sel += [_blank_to_null(c).alias(f"_raw_{c}") for c in spec.raw_cols]
    sel += [F.col(c) for c in ["_source_file", "_as_of_date", "_load_ts"] if c in df.columns]
    out = df.select(*sel)
    if spec.name == "loans":
        out = out.withColumn("debt_group_calc", debt_group_col(F.col("songay_nqh")))
    return out


def add_dup_key_count(df: DataFrame, key: list[str]) -> DataFrame:
    """Số dòng cùng khóa trong lô. NULL key đếm riêng từng dòng (đã có rule not_null)."""
    w = Window.partitionBy(*key)
    any_null = F.greatest(*[F.col(k).isNull() for k in key]) if len(key) > 1 \
        else F.col(key[0]).isNull()
    return df.withColumn(
        "_dup_key_count", F.when(any_null, F.lit(1)).otherwise(F.count(F.lit(1)).over(w))
    )


def add_fk_flag(df: DataFrame, col: str, ref_keys: DataFrame) -> DataFrame:
    """_fk_ok = TRUE nếu df[col] có trong ref_keys (DataFrame 1 cột cùng tên)."""
    ref = ref_keys.select(F.col(ref_keys.columns[0]).alias("_ref_key")).distinct() \
        .withColumn("_fk_hit", F.lit(True))
    return (
        df.join(ref, df[col] == ref["_ref_key"], "left")
        .withColumn("_fk_ok", F.coalesce(F.col("_fk_hit"), F.lit(False)))
        .drop("_ref_key", "_fk_hit")
    )


HELPER_PREFIXES = ("_raw_", "_dup_key_count", "_fk_ok")


def helper_cols(df: DataFrame) -> list[str]:
    return [c for c in df.columns if c.startswith(HELPER_PREFIXES) or c == "debt_group_calc"]


@dataclass
class SilverResult:
    valid: DataFrame        # sẵn sàng MERGE (đã bỏ cột phụ, có debt_group/is_npl cho loans)
    quarantine: DataFrame   # mọi cột + _failed_rules
    checked: DataFrame      # bản trước DQ, để dq_summary

    def known_keys(self, col: str) -> DataFrame:
        """Mọi khóa có trong lô nguồn (kể cả bản ghi bị quarantine vì lỗi thuộc tính khác).

        Dùng làm tham chiếu FK: tài khoản của KH có ngày sinh sai định dạng vẫn là tài khoản
        của một KH CÓ THẬT -> không quarantine theo; chỉ quarantine khi KH không tồn tại.
        """
        return self.checked.select(col).filter(F.col(col).isNotNull()).distinct()
    exact_dupes: int


def prepare_silver(
    bronze: DataFrame, spec: SilverTable, ref_keys: DataFrame | None = None
) -> SilverResult:
    """Toàn bộ logic Silver cho 1 lô bronze (thuần Spark, test được không cần Delta)."""
    df, dupes = drop_exact_duplicates(bronze)
    df = cast_columns(df, spec)
    df = add_dup_key_count(df, spec.key)
    if spec.fk:
        if ref_keys is None:
            raise ValueError(f"{spec.name}: cần ref_keys cho khóa ngoại {spec.fk}")
        df = add_fk_flag(df, spec.fk[0], ref_keys)
    valid, quarantine = apply_dq(df, spec.rules)
    valid = valid.drop(*helper_cols(valid))
    if spec.name == "loans":
        # Silver tính lại nhóm nợ từ DPD (nguồn có thể lệch - rule WARN ghi nhận)
        valid = valid.withColumn("debt_group", debt_group_col(F.col("songay_nqh"))) \
            .withColumn("is_npl", F.col("debt_group") >= 3)
    return SilverResult(valid=valid, quarantine=quarantine, checked=df, exact_dupes=dupes)


def quarantine_records(quarantine: DataFrame, table: str, run_id: str) -> DataFrame:
    """Định dạng bảng ops.dq_quarantine (record_json giữ nguyên bản ghi lỗi)."""
    data_cols = [c for c in quarantine.columns if c != "_failed_rules"]
    return quarantine.select(
        F.lit(run_id).alias("run_id"),
        F.lit(table).alias("table_name"),
        F.col("_failed_rules").alias("failed_rules"),
        F.to_json(F.struct(*data_cols)).alias("record_json"),
        F.col("_source_file") if "_source_file" in data_cols
        else F.lit(None).cast("string").alias("_source_file"),
        F.current_timestamp().alias("quarantined_at"),
    )


def merge_condition(key: list[str]) -> str:
    return " AND ".join(f"t.`{k}` = s.`{k}`" for k in key)
