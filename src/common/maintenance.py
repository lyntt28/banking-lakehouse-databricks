"""Hiệu năng & bảo trì Delta: liquid clustering, OPTIMIZE, VACUUM, benchmark trước/sau.

Chọn cột clustering = cột hay lọc / join nhất trên dashboard và pipeline:
- fact theo ngày lọc as_of_date + chi nhánh; giao dịch lọc theo ngày + tài khoản.
- dim_customer join theo customer_id; quarantine tra theo bảng + file.
Liquid clustering thay cho partition + ZORDER: đổi cột được, không lo partition quá nhỏ.
"""

from __future__ import annotations

CLUSTERING: dict[str, list[str]] = {
    "silver.loan_snapshots": ["as_of_date", "acctnbr"],
    "silver.core_transactions": ["value_date", "account_id"],
    "silver.card_transactions_hist": ["_as_of_date", "card_no"],
    "silver.card_statements_hist": ["stnd_dt", "card_no"],
    "silver.core_customers": ["customer_id"],
    "gold.loan_fact_daily": ["as_of_date", "branchnbr"],
    "gold.core_fact_transaction": ["value_date", "account_id"],
    "gold.dim_customer": ["customer_id"],
    "gold.loan_mart_overdue_daily": ["as_of_date", "branchnbr"],
    "gold.card_mart_kpi_daily": ["as_of_date", "branchnbr"],
    "ops.dq_quarantine": ["table_name", "_source_file"],
    # gold.card_mart_overdue_daily: CHƯA thêm clustering - bảng nhỏ (vài nghìn dòng / ngày).
    # Đo trước (overdue_dashboard_* bên dưới + DESCRIBE DETAIL: numFiles / sizeInBytes), chỉ
    # thêm ["as_of_date", "mng_br"] khi Query Profile cho thấy quét thừa file đáng kể.
}

# Truy vấn đại diện cho dashboard để đo trước / sau (thay {catalog}, {day})
BENCHMARKS: dict[str, str] = {
    "overdue_one_branch_30d": (
        "SELECT as_of_date, sum(overdue_balance) / sum(portfolio_balance) AS ratio "
        "FROM {catalog}.gold.loan_mart_overdue_daily WHERE branchnbr = '101' "
        "AND as_of_date BETWEEN DATE'{day}' - INTERVAL 30 DAYS AND DATE'{day}' GROUP BY 1"),
    "loan_book_one_day": (
        "SELECT debt_group, count(*), sum(balance_vnd) FROM {catalog}.gold.loan_fact_daily "
        "WHERE as_of_date = DATE'{day}' GROUP BY 1"),
    "account_history": (
        "SELECT * FROM {catalog}.gold.core_fact_transaction WHERE account_id = "
        "(SELECT min(account_id) FROM {catalog}.gold.core_fact_transaction "
        "WHERE value_date = DATE'{day}')"),
    # Overdue dashboard: cách cũ (tổng hợp toàn lịch sử rồi mới chọn ngày) vs cách mới (chọn
    # ngày từ lịch báo cáo rồi mới lọc mart). So sánh thời gian, bytes / files scanned.
    "overdue_dashboard_old_history_scan": (
        "WITH day AS (SELECT as_of_date, sum(ovd_bal) ovd, sum(port_bal) port "
        "FROM {catalog}.gold_serving.card_mart_overdue_daily GROUP BY as_of_date) "
        "SELECT * FROM day WHERE as_of_date IN (DATE'{day}', "
        "(SELECT max(as_of_date) FROM day WHERE as_of_date < DATE'{day}'))"),
    "overdue_dashboard_new_calendar_first": (
        "WITH cur AS (SELECT report_date, previous_date "
        "FROM {catalog}.gold_serving.overdue_report_calendar WHERE report_date = "
        "(SELECT max(report_date) FROM {catalog}.gold_serving.overdue_report_calendar "
        "WHERE report_date <= DATE'{day}')), "
        "d AS (SELECT report_date AS d FROM cur UNION SELECT previous_date FROM cur) "
        "SELECT f.as_of_date, sum(f.ovd_bal) ovd, sum(f.port_bal) port "
        "FROM {catalog}.gold_serving.card_mart_overdue_daily f JOIN d ON f.as_of_date = d.d "
        "GROUP BY f.as_of_date"),
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


def table_detail_sql(catalog: str, table: str) -> str:
    """Kiểm tra small files trước khi quyết định OPTIMIZE: numFiles, sizeInBytes."""
    return f"DESCRIBE DETAIL {catalog}.{table}"
