"""Hiệu năng & bảo trì Delta: liquid clustering, OPTIMIZE, VACUUM, benchmark trước/sau.

Chọn cột clustering = cột hay lọc / join nhất trên dashboard và pipeline:
- fact theo ngày lọc as_of_date + chi nhánh; giao dịch lọc theo ngày + tài khoản.
- dim_customer join theo customer_id; quarantine tra theo bảng + file.
Liquid clustering thay cho partition + ZORDER: đổi cột được, không lo partition quá nhỏ.
"""

from __future__ import annotations

CLUSTERING: dict[str, list[str]] = {
    "silver.loans": ["as_of_date", "acctnbr"],
    "silver.transactions": ["value_date", "account_id"],
    "silver.card_transactions": ["_as_of_date", "card_no"],
    "silver.card_statements": ["stnd_dt", "card_no"],
    "silver.customers": ["customer_id"],
    "gold.fact_loan_daily": ["as_of_date", "branchnbr"],
    "gold.fact_transaction": ["value_date", "account_id"],
    "gold.dim_customer": ["customer_id"],
    "gold.mart_loan_overdue_daily": ["as_of_date", "branchnbr"],
    "gold.mart_card_kpi_daily": ["as_of_date", "branchnbr"],
    "ops.dq_quarantine": ["table_name", "_source_file"],
}

# Truy vấn đại diện cho dashboard để đo trước / sau (thay {catalog}, {day})
BENCHMARKS: dict[str, str] = {
    "overdue_one_branch_30d": (
        "SELECT as_of_date, sum(overdue_balance) / sum(portfolio_balance) AS ratio "
        "FROM {catalog}.gold.mart_loan_overdue_daily WHERE branchnbr = '101' "
        "AND as_of_date BETWEEN DATE'{day}' - INTERVAL 30 DAYS AND DATE'{day}' GROUP BY 1"),
    "loan_book_one_day": (
        "SELECT debt_group, count(*), sum(balance_vnd) FROM {catalog}.gold.fact_loan_daily "
        "WHERE as_of_date = DATE'{day}' GROUP BY 1"),
    "account_history": (
        "SELECT * FROM {catalog}.gold.fact_transaction WHERE account_id = "
        "(SELECT min(account_id) FROM {catalog}.gold.fact_transaction "
        "WHERE value_date = DATE'{day}')"),
    "customer_lookup": (
        "SELECT * FROM {catalog}.gold.dim_customer WHERE customer_id = '1000042'"),
}


def cluster_statements(catalog: str, existing: set[str] | None = None) -> list[str]:
    """ALTER TABLE ... CLUSTER BY (idempotent) rồi OPTIMIZE để gom file theo cụm.
    `existing`: bảng đã tồn tại (bỏ qua bảng chưa tạo)."""
    out = []
    for t, cols in CLUSTERING.items():
        if existing is not None and t not in existing:
            continue
        out.append(f"ALTER TABLE {catalog}.{t} CLUSTER BY ({', '.join(cols)})")
        out.append(f"OPTIMIZE {catalog}.{t}")
    return out


def vacuum_statements(catalog: str, retain_hours: int = 168,
                      existing: set[str] | None = None) -> list[str]:
    if retain_hours < 168:
        raise ValueError("Giữ tối thiểu 7 ngày (168h) để time travel / đọc đồng thời an toàn")
    return [f"VACUUM {catalog}.{t} RETAIN {retain_hours} HOURS" for t in CLUSTERING
            if existing is None or t in existing]


def benchmark_sql(catalog: str, day: str) -> dict[str, str]:
    return {k: v.format(catalog=catalog, day=day) for k, v in BENCHMARKS.items()}
