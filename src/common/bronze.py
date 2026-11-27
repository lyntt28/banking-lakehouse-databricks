"""Bronze: nạp file thô từ Volume landing vào bảng Delta, GIỮ NGUYÊN dữ liệu.

Nguyên tắc Bronze:
- Đọc mọi cột dạng STRING (không ép kiểu) -> không mất dữ liệu sai định dạng;
  việc ép kiểu / làm sạch để Silver xử lý.
- Thêm cột kỹ thuật: _load_ts, _source_file, _as_of_date (lấy từ thư mục as_of_date=...).
- Cột không khớp schema được giữ trong _rescued_data (Auto Loader rescue mode).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Source:
    table: str                  # tên thư mục trong landing = tên bảng bronze
    fmt: str                    # csv | json
    options: dict = field(default_factory=dict)


CSV = {"header": "true"}
TSV = {"header": "true", "sep": "\t"}

SOURCES = [
    Source("branches", "csv", CSV),
    Source("products", "csv", CSV),
    Source("customers", "csv", CSV),
    Source("accounts", "csv", CSV),
    Source("loans", "csv", CSV),
    Source("transactions", "json"),
    Source("cards", "csv", CSV),
    Source("card_transactions", "json"),
    Source("card_statements", "csv", CSV),
    Source("insurance_policies", "csv", CSV),
    Source("card_kpi", "csv", TSV),          # 9 file .txt tab-separated / ngày
    Source("deposit_summary", "csv", TSV),   # bang1247_YYYYMMDD.txt
    Source("card_overdue", "csv", TSV),      # credit_card_overdue_YYYYMMDD.txt
    Source("targets", "csv", CSV),           # chỉ tiêu kế hoạch (ngày đầu)
    Source("fx_rates", "csv", CSV),          # tỷ giá về VND mỗi ngày
]


def autoloader_options(src: Source, schema_location: str) -> dict:
    """Option cho Auto Loader (cloudFiles). Mọi cột đọc là string."""
    opts = {
        "cloudFiles.format": src.fmt,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.inferColumnTypes": "false",     # giữ string, Silver mới ép kiểu
        "cloudFiles.schemaEvolutionMode": "rescue",  # cột lạ -> _rescued_data, không dừng stream
        "cloudFiles.includeExistingFiles": "true",
    }
    opts.update(src.options)
    return opts


def as_of_date_from_path(path: Column) -> Column:
    """'/.../loans/as_of_date=2026-11-01/loans.csv' -> date 2026-11-01 (NULL nếu không có)."""
    s = F.regexp_extract(path, r"as_of_date=(\d{4}-\d{2}-\d{2})", 1)
    return F.when(s == "", F.lit(None).cast("date")).otherwise(F.to_date(s))


def add_bronze_metadata(df: DataFrame, path: Column) -> DataFrame:
    """Thêm cột kỹ thuật. Trên Databricks truyền path = F.col('_metadata.file_path')."""
    return (
        df.withColumn("_source_file", path)
        .withColumn("_as_of_date", as_of_date_from_path(path))
        .withColumn("_load_ts", F.current_timestamp())
    )
