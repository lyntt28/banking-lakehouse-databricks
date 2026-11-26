"""Batch control: ngày nào cần xử lý, file nguồn đã đủ chưa, ghi trạng thái chạy.

Bảng `ops.pipeline_runs` (1 dòng / ngày dữ liệu / layer / lần chạy):
    as_of_date, layer (silver|gold), run_id, status (RUNNING|SUCCESS|FAILED),
    started_at, finished_at, message

Watermark = ngày lớn nhất có status SUCCESS của 1 layer. Job hằng ngày không còn lấy
"ngày mới nhất trong bronze" nữa mà xử lý MỌI ngày theo lịch nghiệp vụ > watermark (kể cả ngày
chưa có file: dừng ở đó, không nhảy qua), theo thứ tự tăng dần, nên job bị gián đoạn 2 ngày thì
lần chạy sau tự bù đủ 3 ngày.
"""

from __future__ import annotations

from common.dates import check_date
from common.names import fq

# Bảng phải có file MỖI ngày. ref_branches / ref_products / ref_targets chỉ gửi ngày đầu
# (hoặc khi thay đổi) nên không bắt buộc.
OPTIONAL_DAILY = {"ref_branches", "ref_products", "ref_targets"}

PIPELINE_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  as_of_date DATE,
  layer STRING,
  run_id STRING,
  status STRING,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  message STRING
) USING DELTA
"""

PUBLISHED_BATCHES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  as_of_date DATE,
  run_id STRING,
  published_at TIMESTAMP,
  checks STRING
) USING DELTA
"""

# Mỗi lần chạy 1 bước (task) của 1 ngày: thời gian, số dòng vào/ra, lỗi -> dashboard vận hành
TASK_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  run_id STRING,
  as_of_date DATE,
  layer STRING,
  step STRING,
  status STRING,
  rows_in BIGINT,
  rows_out BIGINT,
  duration_s DOUBLE,
  error STRING,
  logged_at TIMESTAMP
) USING DELTA
"""

# File nguồn theo ngày: có về chưa, mấy file, bao nhiêu dòng, nạp lúc nào
FILE_MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  as_of_date DATE,
  table_name STRING,
  required BOOLEAN,
  status STRING,
  file_count BIGINT,
  rows BIGINT,
  source_files ARRAY<STRING>,
  first_loaded_at TIMESTAMP,
  checked_at TIMESTAMP
) USING DELTA
"""

# Version Delta của từng bảng NGAY TRƯỚC khi publish -> RESTORE TABLE ... TO VERSION AS OF
PUBLISH_VERSIONS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  run_id STRING,
  as_of_date DATE,
  layer STRING,
  table_name STRING,
  version_before BIGINT,
  captured_at TIMESTAMP
) USING DELTA
"""

OPS_DDL = {"publish_versions": PUBLISH_VERSIONS_DDL,
           "pipeline_runs": PIPELINE_RUNS_DDL, "published_batches": PUBLISHED_BATCHES_DDL,
           "task_runs": TASK_RUNS_DDL, "file_manifest": FILE_MANIFEST_DDL}


def ensure_ops_tables(spark, catalog: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq(catalog, 'ops')}")
    for name, ddl in OPS_DDL.items():
        spark.sql(ddl.format(table=fq(catalog, "ops", name)))


# Data contract: bảng gửi NHIỀU file / ngày, mỗi file 1 nhóm (tên file bắt đầu bằng mã nhóm).
# card_kpi_source: 1 file / nhóm KPI. Phải khớp data_generator.summaries.KPI_FILES.
REQUIRED_FILE_GROUPS = {
    "card_kpi_source": ("01_member_eop", "02_member_usage", "03_new_consumer",
                        "04_member_closed", "05_balance_eop", "06_spend_sector",
                        "07_spend_location", "08_spend_channel", "09_risk_dpd"),
}
# Số file nguồn gửi mỗi ngày theo hợp đồng dữ liệu (mặc định 1).
EXPECTED_FILES = {t: len(g) for t, g in REQUIRED_FILE_GROUPS.items()}


def _basename(path: str) -> str:
    return path.rstrip("/").rsplit("/", 1)[-1]


def missing_file_groups(table: str, files: list[str]) -> list[str]:
    """Nhóm file theo data contract mà ngày đó chưa nhận (vd: 5/9 file KPI thẻ)."""
    names = [_basename(f) for f in files]
    return [g for g in REQUIRED_FILE_GROUPS.get(table, ())
            if not any(n.startswith(g) for n in names)]


def manifest_status(table: str, files: list[str], order: list[str]) -> str:
    """ARRIVED | MISSING (bảng bắt buộc chưa có file) | NOT_SENT (bảng tham chiếu không gửi)
    | INCOMPLETE (mới nhận 1 phần: thiếu nhóm file theo data contract)
    | MULTIPLE_FILES (đủ nhóm nhưng nhiều file hơn hợp đồng: nguồn gửi lại / sửa file)."""
    if not files:
        return "MISSING" if table in required_tables(order) else "NOT_SENT"
    if missing_file_groups(table, files):
        return "INCOMPLETE"
    return "MULTIPLE_FILES" if len(files) > EXPECTED_FILES.get(table, 1) else "ARRIVED"


def required_tables(order: list[str]) -> list[str]:
    return [t for t in order if t not in OPTIONAL_DAILY]


def missing_tables(row_counts: dict[str, int], order: list[str]) -> list[str]:
    """Bảng bắt buộc nhưng ngày đó không có dòng nào trong bronze (file chưa về)."""
    return [t for t in required_tables(order) if row_counts.get(t, 0) == 0]


def incomplete_tables(statuses: dict[str, str]) -> list[str]:
    """Bảng đã có file nhưng chưa đủ theo data contract -> chưa được xử lý ngày đó."""
    return sorted(t for t, st in statuses.items() if st == "INCOMPLETE")


def business_dates(start: str, end: str, holidays: frozenset[str] = frozenset()) -> list[str]:
    """Lịch ngày nghiệp vụ dự kiến [start, end]: nguồn phải gửi file MỖI ngày (kể cả cuối tuần,
    hệ thống core chốt số hằng ngày), trừ ngày trong `holidays`."""
    from datetime import date, timedelta

    d, last = date.fromisoformat(check_date(start)), date.fromisoformat(check_date(end))
    out = []
    while d <= last:
        if d.isoformat() not in holidays:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def pending_dates(bronze_dates: list[str], watermark: str | None,
                  requested: str | None = None,
                  holidays: frozenset[str] = frozenset()) -> list[str]:
    """Danh sách ngày cần xử lý (tăng dần).

    - requested: chạy đúng 1 ngày (rerun / backfill thủ công).
    - không requested: MỌI ngày theo lịch nghiệp vụ từ sau watermark (chưa có watermark: từ ngày
      bronze đầu tiên) tới ngày bronze mới nhất. Ngày không có file vẫn nằm trong danh sách ->
      Silver dừng ở đó với SourceFilesMissing thay vì nhảy qua (watermark không vượt ngày thiếu,
      file gửi trễ của ngày đó vẫn được xử lý khi về).
    """
    if requested:
        return [check_date(requested)]
    received = sorted({check_date(str(d)) for d in bronze_dates})
    if not received:
        return []
    if watermark:
        from datetime import date, timedelta

        start = (date.fromisoformat(check_date(str(watermark))) + timedelta(days=1)).isoformat()
    else:
        start = received[0]
    return business_dates(start, received[-1], holidays) if start <= received[-1] else []


def corrected_days(last_loaded: dict[str, object], silver_done_at: dict[str, object]) -> list[str]:
    """Ngày đã Silver SUCCESS nhưng Bronze lại nhận file SAU lần SUCCESS đó (nguồn gửi file
    sửa / correction) -> Silver của ngày đó đã cũ, phải xử lý lại Silver trước khi dựng Gold.

    last_loaded:    ngày -> _load_ts lớn nhất trong Bronze (mọi bảng nguồn)
    silver_done_at: ngày -> finished_at của lần Silver SUCCESS gần nhất
    """
    return sorted(d for d, done in silver_done_at.items()
                  if d in last_loaded and last_loaded[d] > done)


def run_row(as_of_date: str, layer: str, run_id: str, status: str,
            message: str = "") -> tuple:
    return (check_date(as_of_date), layer, run_id, status, message)


def record_run(spark, catalog: str, as_of_date: str, layer: str, run_id: str,
               status: str, message: str = "") -> None:
    """Ghi 1 dòng trạng thái (append-only, giữ lịch sử mọi lần chạy để audit)."""
    from pyspark.sql import functions as F

    df = spark.createDataFrame(
        [run_row(as_of_date, layer, run_id, status, message[:2000])],
        "as_of_date string, layer string, run_id string, status string, message string",
    )
    (df.select(F.col("as_of_date").cast("date"), "layer", "run_id", "status",
               F.current_timestamp().alias("started_at"),
               F.when(F.col("status") != "RUNNING", F.current_timestamp())
               .cast("timestamp").alias("finished_at"),
               "message")
     .write.format("delta").mode("append").saveAsTable(fq(catalog, "ops", "pipeline_runs")))


# Bảng dùng để suy ra watermark lần đầu nâng cấp (đã có dữ liệu nhưng chưa có pipeline_runs)
BOOTSTRAP = {"silver": ("silver.loan_snapshots", "as_of_date"),
             "gold": ("gold.loan_mart_overdue_daily", "as_of_date")}


def watermark(spark, catalog: str, layer: str) -> str | None:
    if layer not in BOOTSTRAP:
        raise ValueError(f"layer không hợp lệ: {layer}")
    t = fq(catalog, "ops", "pipeline_runs")
    if spark.catalog.tableExists(t):
        v = spark.sql(f"SELECT max(as_of_date) FROM {t} WHERE layer = '{layer}' "
                      "AND status = 'SUCCESS'").first()[0]
        if v:
            return str(v)
    table, col = BOOTSTRAP[layer]
    full = fq(catalog, *table.split("."))
    if spark.catalog.tableExists(full):
        v = spark.sql(f"SELECT max({col}) FROM {full}").first()[0]
        return str(v) if v else None
    return None


def assert_reset_allowed(catalog: str) -> None:
    """reset (drop Silver/Gold) chỉ cho dev/test. Prod phải backfill qua staging + gate."""
    if catalog.lower().endswith("_prod") or catalog.lower() == "prod":
        raise PermissionError(f"reset=true bị cấm trên {catalog}: dùng backfill thường "
                              "(staging -> gate -> publish), xem docs/runbook.md")


def assert_fresh(latest_published, today: str, max_age_days: int) -> None:
    """Batch Gold mới nhất phải trong max_age_days ngày (ML không chạy trên data cũ)."""
    from datetime import date

    if latest_published is None:
        raise RuntimeError("Chưa có batch Gold nào được publish (ops.published_batches rỗng)")
    age = (date.fromisoformat(check_date(today))
           - date.fromisoformat(check_date(str(latest_published)))).days
    if age > max_age_days:
        raise RuntimeError(f"Gold cũ {age} ngày (batch mới nhất {latest_published}) > "
                           f"{max_age_days}: job daily lỗi/trễ? Dừng để không dùng dữ liệu cũ")


def table_version(spark, table: str) -> int | None:
    """Version Delta hiện tại; None nếu bảng chưa tồn tại (publish lần đầu)."""
    if not spark.catalog.tableExists(table):
        return None
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def record_versions(spark, catalog: str, run_id: str, as_of_date: str, layer: str,
                    tables: list[str]) -> None:
    from pyspark.sql import functions as F

    rows = [(run_id, check_date(as_of_date), layer, t, table_version(spark, t)) for t in tables]
    if not rows:
        return
    (spark.createDataFrame(rows, "run_id string, as_of_date string, layer string, "
                                 "table_name string, version_before long")
     .withColumn("as_of_date", F.col("as_of_date").cast("date"))
     .withColumn("captured_at", F.current_timestamp())
     .write.format("delta").mode("append").saveAsTable(fq(catalog, "ops", "publish_versions")))


def rollback_statements(versions: list[tuple[str, int | None, int | None]]) -> list[str]:
    """SQL đưa các bảng về trạng thái TRƯỚC publish. versions: (bảng, version_before, hiện tại).

    - version_before NULL + bảng đang tồn tại: bảng được tạo trong lần publish lỗi -> DROP
    - hiện tại == version_before: bảng chưa bị ghi -> bỏ qua
    - còn lại: RESTORE về version_before
    """
    out = []
    for table, before, current in versions:
        if before is None:
            if current is not None:
                out.append(f"DROP TABLE IF EXISTS {table}")
        elif current != before:
            out.append(f"RESTORE TABLE {table} TO VERSION AS OF {int(before)}")
    return out


def rollback_publish(spark, catalog: str, run_id: str) -> list[str]:
    """Publish Gold lỗi giữa chừng (vd: bảng thứ 3/17): đưa MỌI bảng đã ghi về version trước
    publish -> Gold toàn vẹn ở batch cũ, không trộn bảng mới / cũ. Trả về các lệnh đã chạy."""
    t = fq(catalog, "ops", "publish_versions")
    rows = spark.sql(f"SELECT table_name, version_before FROM {t} "
                     f"WHERE run_id = '{run_id}'").collect()
    stmts = rollback_statements([(r.table_name, r.version_before,
                                  table_version(spark, r.table_name)) for r in rows])
    for sql in stmts:
        spark.sql(sql)
    return stmts
