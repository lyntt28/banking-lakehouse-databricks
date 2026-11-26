## Thay đổi gì / vì sao
<!-- 1-3 câu: vấn đề nghiệp vụ hoặc kỹ thuật, và cách giải quyết -->

## Ảnh hưởng dữ liệu
- [ ] Bảng / grain bị ảnh hưởng: <!-- vd gold.loan_fact_daily (as_of_date x acctnbr) -->
- [ ] Đổi schema? Nếu có: tương thích ngược / cần backfill?
- [ ] Ngữ nghĩa chạy lại (retry / rerun / correction / backfill) vẫn đúng (docs/runbook.md)
- [ ] Gate đối soát / DQ rule có cần cập nhật?

## Kiểm thử
- [ ] `ruff check .`
- [ ] `pytest -m "not integration"` (unit)
- [ ] `pytest -m integration` nếu đổi `src/pipelines/` hoặc logic Silver/Gold
- [ ] Test mới cho bug / kịch bản lỗi (nếu sửa sự cố: thêm regression test)
- [ ] Đã chạy trên Databricks dev? (đính kèm run_id / ảnh `ops.pipeline_runs` nếu có)

## Bảo mật
- [ ] Không có secret / hostname / dữ liệu thật
- [ ] Bảng mới cần cấp quyền / mask / row filter? (`src/common/governance.py`)
