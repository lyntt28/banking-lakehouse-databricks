"""Unity Catalog governance as code: nhóm quyền, GRANT, che cột PII, lọc dòng theo chi nhánh.

Mọi thứ sinh ra dưới dạng câu SQL (idempotent) -> notebook src/00_setup/03_governance.py chạy;
unit test kiểm tra nội dung câu lệnh và logic che/lọc (chạy biểu thức trên Spark local).

Nhóm (tạo ở Account Console / SCIM, code chỉ GRANT):
    bank_admins       toàn quyền catalog
    data_engineers    đọc/ghi bronze, silver, gold, ops; thấy PII
    risk_analysts     đọc silver, gold; PII bị che; thấy mọi chi nhánh; ở ops chỉ đọc bảng
                      kết quả DQ / đối soát / batch (KHÔNG đọc dq_quarantine - record_json
                      chứa PII gốc chưa che)
    branch_users      chỉ đọc các bảng gold CÓ row filter chi nhánh + dim không nhạy cảm
                      (cấp theo bảng, không cấp SELECT cả schema gold)

Least privilege: SELECT cấp theo BẢNG ở TABLE_GRANTS, schema chỉ cấp USE SCHEMA cho nhóm
đọc giới hạn. Bảng mới thêm vào gold/ops mặc định KHÔNG lộ cho branch_users / risk_analysts.
"""

from __future__ import annotations

from dataclasses import dataclass

GROUPS = ["bank_admins", "data_engineers", "risk_analysts", "branch_users"]
PII_READERS = ["bank_admins", "data_engineers"]      # thấy dữ liệu gốc
ALL_BRANCHES = ["bank_admins", "data_engineers", "risk_analysts"]

# schema -> {group: [privileges]}
SCHEMA_GRANTS: dict[str, dict[str, list[str]]] = {
    "landing": {"data_engineers": ["USE SCHEMA", "READ VOLUME", "WRITE VOLUME"]},
    "bronze": {"data_engineers": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]},
    "silver": {"data_engineers": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"],
               "risk_analysts": ["USE SCHEMA", "SELECT"]},
    "gold": {"data_engineers": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"],
             "risk_analysts": ["USE SCHEMA", "SELECT"],
             "branch_users": ["USE SCHEMA"]},
    "silver_stage": {"data_engineers": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]},
    "gold_stage": {"data_engineers": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE"]},
    "ops": {"data_engineers": ["USE SCHEMA", "SELECT", "MODIFY", "CREATE TABLE",
                               "READ VOLUME", "WRITE VOLUME"],
            "risk_analysts": ["USE SCHEMA"]},
}

# bảng nhạy cảm: không nhóm đọc giới hạn nào được cấp (kiểm tra bằng test)
RESTRICTED_TABLES = ["ops.dq_quarantine", "ops.user_branch"]

# bảng gold có cột branchnbr -> lọc dòng theo chi nhánh cho branch_users
BRANCH_FILTERED = ["gold.loan_fact_daily", "gold.core_fact_transaction", "gold.dim_customer",
                   "gold.loan_mart_overdue_daily", "gold.dep_mart_daily",
                   "gold.cust_mart_360", "gold.card_mart_kpi_daily"]

# group -> bảng được SELECT (cấp theo bảng)
TABLE_GRANTS: dict[str, list[str]] = {
    "risk_analysts": ["ops.dq_results", "ops.recon_card_kpi", "ops.recon_deposit",
                      "ops.pipeline_runs", "ops.published_batches", "ops.task_runs",
                      "ops.file_manifest"],
    "branch_users": BRANCH_FILTERED + ["gold.dim_date", "gold.dim_branch", "gold.dim_product"],
}


@dataclass(frozen=True)
class Mask:
    function: str       # tên hàm trong schema ops
    dtype: str          # kiểu dữ liệu của cột
    masked: str         # biểu thức SQL trên tham số `val` cho người KHÔNG được xem PII


MASKS = {
    # 0912345678 -> *******678
    "phone": Mask("mask_phone", "STRING",
                  "CASE WHEN val IS NULL THEN NULL "
                  "ELSE concat(repeat('*', greatest(length(val) - 3, 0)), right(val, 3)) END"),
    # 012345678901 -> ********8901
    "id_no": Mask("mask_id_no", "STRING",
                  "CASE WHEN val IS NULL THEN NULL "
                  "ELSE concat(repeat('*', greatest(length(val) - 4, 0)), right(val, 4)) END"),
    # user123@example.com -> u***@example.com
    "email": Mask("mask_email", "STRING",
                  "CASE WHEN val IS NULL OR instr(val, '@') = 0 THEN val "
                  "ELSE concat(left(val, 1), '***', substr(val, instr(val, '@'))) END"),
    # NGUYEN VAN AN -> N*** V*** A***
    "name": Mask("mask_name", "STRING",
                 "CASE WHEN val IS NULL THEN NULL ELSE array_join(transform(split(val, ' '), "
                 "w -> concat(left(w, 1), '***')), ' ') END"),
    "address": Mask("mask_address", "STRING", "CASE WHEN val IS NULL THEN NULL ELSE '***' END"),
    # ngày sinh -> chỉ giữ năm (01/01/yyyy), đủ cho phân tích theo tuổi
    "dob": Mask("mask_dob", "DATE",
                "CASE WHEN val IS NULL THEN NULL ELSE make_date(year(val), 1, 1) END"),
    "income": Mask("mask_income", "DECIMAL(22,2)", "CAST(NULL AS DECIMAL(22,2))"),
}

# bảng -> {cột: loại mask}
PII_COLUMNS: dict[str, dict[str, str]] = {
    "silver.core_customers": {"full_name": "name", "id_no": "id_no", "phone": "phone",
                         "email": "email", "address": "address", "dob": "dob",
                         "monthly_income": "income"},
    "gold.dim_customer": {"full_name": "name", "id_no": "id_no", "phone": "phone",
                          "email": "email", "address": "address", "dob": "dob",
                          "monthly_income": "income"},
    "gold.cust_mart_360": {"full_name": "name", "monthly_income": "income"},
}



def _members(groups: list[str]) -> str:
    return " OR ".join(f"is_account_group_member('{g}')" for g in groups)


def is_privileged_expr() -> str:
    return _members(PII_READERS)


def mask_body(kind: str, privileged: str | None = None) -> str:
    """Thân hàm che: người có quyền thấy `val`, người khác thấy bản che."""
    m = MASKS[kind]
    return f"CASE WHEN {privileged or is_privileged_expr()} THEN val ELSE {m.masked} END"


def mask_function_sql(catalog: str, kind: str) -> str:
    m = MASKS[kind]
    return (f"CREATE OR REPLACE FUNCTION {catalog}.ops.{m.function}(val {m.dtype}) "
            f"RETURNS {m.dtype} RETURN {mask_body(kind)}")


def branch_filter_body(catalog: str, all_branches: str | None = None,
                       user_expr: str = "current_user()", mapping: str | None = None) -> str:
    """TRUE nếu người dùng xem được mọi chi nhánh, hoặc chi nhánh được gán trong ops.user_branch.
    `all_branches` / `user_expr` / `mapping` chỉ đổi khi test (Spark local không có UC)."""
    table = mapping or f"{catalog}.ops.user_branch"
    return (f"{all_branches or _members(ALL_BRANCHES)} OR EXISTS (SELECT 1 FROM "
            f"{table} ub WHERE ub.user_email = {user_expr} "
            f"AND ub.branchnbr = branchnbr_val)")


def statements(catalog: str) -> list[str]:
    """Toàn bộ câu lệnh governance theo thứ tự chạy. Chạy lại an toàn."""
    out = [f"GRANT USE CATALOG ON CATALOG {catalog} TO `{g}`" for g in GROUPS]
    out.append(f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `bank_admins`")
    for schema, grants in SCHEMA_GRANTS.items():
        for group, privs in grants.items():
            out.append(f"GRANT {', '.join(privs)} ON SCHEMA {catalog}.{schema} TO `{group}`")

    # bảng ánh xạ người dùng -> chi nhánh (do admin cập nhật)
    out.append(f"CREATE TABLE IF NOT EXISTS {catalog}.ops.user_branch "
               "(user_email STRING NOT NULL, branchnbr STRING NOT NULL)")
    # hàm che cột + hàm lọc dòng
    out += [mask_function_sql(catalog, k) for k in MASKS]
    body = branch_filter_body(catalog)
    out.append(f"CREATE OR REPLACE FUNCTION {catalog}.ops.branch_filter(branchnbr_val STRING) "
               f"RETURNS BOOLEAN RETURN {body}")
    # người dùng cuối cần EXECUTE để hàm chạy trong ngữ cảnh của họ
    for g in GROUPS:
        out.append(f"GRANT EXECUTE ON SCHEMA {catalog}.ops TO `{g}`")

    for table, cols in PII_COLUMNS.items():
        for col, kind in cols.items():
            out.append(f"ALTER TABLE {catalog}.{table} ALTER COLUMN {col} "
                       f"SET MASK {catalog}.ops.{MASKS[kind].function}")
            out.append(f"ALTER TABLE {catalog}.{table} ALTER COLUMN {col} "
                       f"SET TAGS ('pii' = '{kind}')")
    for table in BRANCH_FILTERED:
        out.append(f"ALTER TABLE {catalog}.{table} "
                   f"SET ROW FILTER {catalog}.ops.branch_filter ON (branchnbr)")
    # SELECT theo bảng: cấp SAU khi mask/row filter đã gắn -> không có khoảnh khắc lộ dữ liệu
    for group, tables in TABLE_GRANTS.items():
        for table in tables:
            out.append(f"GRANT SELECT ON TABLE {catalog}.{table} TO `{group}`")
    return out


def target_tables() -> set[str]:
    """Bảng mà governance cần tồn tại trước khi chạy (để notebook bỏ qua bảng chưa có)."""
    return set(PII_COLUMNS) | set(BRANCH_FILTERED) | {t for ts in TABLE_GRANTS.values()
                                                          for t in ts}


def assert_pipeline_identity(spark, catalog: str) -> None:
    """Job ghi dữ liệu phải chạy bằng identity thấy PII và mọi chi nhánh.

    Nếu governance đã bật (có ops.user_branch) mà identity không thuộc PII_READERS thì:
    SCD2 sẽ băm giá trị ĐÃ CHE (mọi KH đều 'thay đổi'), Gold đọc bảng bị lọc dòng -> sai số.
    Dừng ngay thay vì ghi dữ liệu sai.
    """
    if not spark.catalog.tableExists(f"{catalog}.ops.user_branch"):
        return
    ok = spark.sql(f"SELECT {is_privileged_expr()} AS ok").first()["ok"]
    if not ok:
        raise PermissionError(
            "Governance đang bật: job phải chạy bằng service principal / user thuộc nhóm "
            f"{' hoặc '.join(PII_READERS)} (run_as trong databricks.yml)."
        )
