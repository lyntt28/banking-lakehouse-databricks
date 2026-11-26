# Coding standards

## Cấu trúc
| Thư mục | Trách nhiệm | Quy tắc |
|---|---|---|
| `src/0*/` (notebook) | Entry point: đọc widget -> gọi `run()` | Không chứa logic nghiệp vụ; `display` chỉ để xem |
| `src/pipelines/` | Điều phối 1 layer: đọc bảng, gọi transform, gate, ghi, ghi trạng thái | Không dùng `dbutils`; tên bảng qua `common.names.fq` (chạy được local) |
| `src/common/` | Transform thuần (DataFrame vào -> DataFrame ra), DQ rule, ghi Delta idempotent | Không đọc/ghi bảng theo tên trừ `delta_io` / `batch` |
| `tests/` | Unit test (nhanh) | Không cần Databricks |
| `tests/integration/` | Bronze -> Silver -> Gold trên Delta local + kịch bản lỗi | `pytest -m integration` |

## Quy tắc
- **Idempotent**: mọi lệnh ghi phải chạy lại được (MERGE theo khóa, `replaceWhere` theo ngày,
  SCD2 so hash). Không `append` vào bảng nghiệp vụ trừ `insert_only` có khóa.
- **Fail fast, không bỏ qua**: thiếu file, DQ vượt ngưỡng, đối soát lệch -> raise exception có
  ngữ cảnh (ngày, bảng, run_id, bảng ops cần xem). Không `except: pass`.
- **Không ghi Gold khi chưa qua gate**: mart mới cần đối soát -> stage trước, kiểm tra, rồi publish.
- **Quan sát được**: bước mới bọc trong `StepRecorder.step(...)`; log bằng `ops_log.log` (JSON).
- **Serverless**: không `.cache()`, `.persist()`, RDD, `sparkContext` (có test chặn).
- **SQL động**: ngày đi qua `check_date`, chuỗi tự do phải escape; không ghép input người dùng.
- **Quyền**: bảng mới ở `gold`/`ops` mặc định không cấp cho nhóm đọc giới hạn; thêm vào
  `TABLE_GRANTS` có chủ đích.
- Python 3.11, `ruff` (line 100). Comment/docstring tiếng Việt cho nghiệp vụ, tên code tiếng Anh.

## Chạy local
```bash
pip install -r requirements-dev.txt && pip install -e .
pytest -m "not integration" --cov
pytest -m integration
```
