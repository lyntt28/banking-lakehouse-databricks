# Incident runbook

Mở dashboard **Lakehouse Operations Monitor** trước. Mọi truy vấn dưới đây thay `<c>` = catalog.

## 0. Phân loại nhanh
```sql
-- trạng thái mới nhất của từng ngày / layer
SELECT * FROM <c>.ops.pipeline_runs
QUALIFY row_number() OVER (PARTITION BY as_of_date, layer ORDER BY started_at DESC) = 1
ORDER BY as_of_date DESC;
-- bước lỗi
SELECT * FROM <c>.ops.task_runs WHERE status = 'FAILED' ORDER BY logged_at DESC LIMIT 20;
-- Gold đang phục vụ batch nào
SELECT max(as_of_date) FROM <c>.ops.published_batches;
```
**Dashboard business có đang sai không?** Không: người đọc chỉ đọc `gold_serving.*`, chỉ thấy batch
đã kích hoạt trong `ops.published_batches`. Lỗi ở `gold:publish` hoặc sau đó → mục 5.

## 1. `SourceFilesMissing` (bước `file_manifest`)
- Xem `ops.file_manifest` ngày đó, `status IN ('MISSING', 'INCOMPLETE')`. `INCOMPLETE` = đã
  nhận một phần (vd: 5/9 file KPI thẻ); thông báo lỗi liệt kê nhóm file còn thiếu.
- Liên hệ hệ thống nguồn; khi file về landing: chạy lại job (as_of_date trống). Bronze nạp file
  mới, pipeline tiếp tục từ ngày lỗi. Không cần thao tác dữ liệu.

## 2. `DQGateFailed` (bước `silver:<table>`)
- `SELECT * FROM <c>.ops.dq_results WHERE run_id = '<run_id>' AND status <> 'PASS'`
- Mẫu lỗi: `ops.dq_quarantine` (chỉ data_engineers – có PII).
- Lỗi nguồn → yêu cầu gửi lại file (tên khác) → chạy lại. Rule quá chặt → sửa rule qua PR.

## 3. `GateFailed` (bước `gold:gate`)
- `ops.recon_card_kpi` / `ops.recon_deposit` ngày đó, status ≠ MATCH; số liệu gate ở cột
  `error` của `task_runs` và log JSON `gold_gate`.
- Gold **chưa** bị ghi. Sửa nguyên nhân (file tổng hợp sai → nguồn gửi lại; lỗi logic → PR + test
  hồi quy) rồi chạy lại job: lần chạy sau tự dựng lại Gold ngày đó trước.

## 4. `OutOfOrder` / `NotReproducible`
- Có người chạy lại ngày cũ hơn watermark. Không làm gì với dữ liệu: lệnh đã bị chặn trước khi ghi.
- Cần sửa ngày cũ thật sự → mục 6.

## 5. Lỗi ở / sau `gold:publish` (lỗi hạ tầng sau khi đã qua mọi gate)
Silver: **stage → kiểm tra tất cả → publish**, lỗi DQ / đối soát không để lại bảng ghi dở.
Gold: ghi THÊM batch mới (vô hình) → **kích hoạt** (1 dòng `ops.published_batches`) → tạo + kiểm
tra view `gold_serving` → dọn batch cũ. Xem `pipeline_runs.message` để biết lỗi ở giai đoạn nào:
- **"rollback N bảng"** – lỗi TRƯỚC kích hoạt. Batch chưa từng được thấy; job đã `RESTORE` các
  bảng về version trước publish. Chạy lại job.
- **"ROLLBACK FAILED"** – lỗi trước kích hoạt và rollback cũng lỗi. Dashboard vẫn đúng (batch dở
  không được kích hoạt), chỉ còn dòng rác. Chạy lại job (batch cũ / rác được dọn khi batch mới
  kích hoạt) hoặc RESTORE tay:
  ```sql
  SELECT table_name, version_before FROM <c>.ops.publish_versions WHERE run_id = '<run_id>';
  RESTORE TABLE <table_name> TO VERSION AS OF <version_before>;   -- từng bảng
  ```
  `version_before` NULL = bảng được tạo trong lần chạy đó → DROP nếu cần.
  **Không** RESTORE run đã kích hoạt (run_id có trong `published_batches`): sẽ xoá dòng mà
  dashboard đang đọc.
- **"batch đã kích hoạt trước khi lỗi -> không rollback"** – dashboard đang đọc batch mới, đầy đủ.
  Run FAILED để lần chạy sau dựng lại ngày đó (batch mới thay thế). Không cần thao tác dữ liệu.
- **`ServingViewsInvalid`** – batch đã kích hoạt nhưng view `gold_serving` thiếu / lệch cột (vd:
  bảng thêm cột, view lỗi). Dashboard có thể thiếu cột mới / lỗi. Xem danh sách trong message, sửa
  rồi chạy lại job (tạo lại view). Kiểm tra: `SHOW VIEWS IN <c>.gold_serving`.
- **SUCCESS có "WARN dọn batch cũ lỗi"** – dữ liệu đang phục vụ đúng; còn dòng batch cũ trong
  `gold`. Lần publish sau của ngày đó dọn tiếp; không gấp.

## 5b. `WriterBusy`
Một job khác đang ghi lakehouse (daily, Silver / Gold chạy tay, maintenance, backfill).
```sql
SELECT * FROM <c>.ops.writer_lock;   -- job nào, owner nào, hết hạn lúc nào
```
- Job kia đang chạy thật (Jobs → Runs): chờ xong rồi chạy lại. Không xoá khóa.
- Job kia đã chết (không còn run nào chạy) mà khóa chưa hết hạn (8h): xoá tay để chạy ngay
  `DELETE FROM <c>.ops.writer_lock WHERE owner = '<owner>'`.

## 6. Correction / backfill ngày cũ (prod)
Job daily tự nhận diện file sửa: Bronze có file nạp SAU lần Silver SUCCESS của ngày đó.
- Ngày sửa = watermark Silver: job tự chạy lại Silver rồi Gold ngày đó (log `daily_correction`),
  không dựng Gold từ Silver cũ. Không cần thao tác.
- Ngày sửa < watermark: job dừng với `CorrectionNeedsReplay`, làm thủ công như dưới:
1. Dựng catalog song song (vd `bank_prod_rebuild`) với `05_backfill reset=true` từ Bronze.
2. So sánh với prod (đối soát, số dòng, tổng số dư).
3. Chuyển consumer (view / dashboard `dataset_catalog`) hoặc `DEEP CLONE` từng bảng vào prod
   trong cửa sổ bảo trì. Không drop bảng prod đang phục vụ.

## Sau sự cố
- Ghi lại: ngày, run_id, nguyên nhân, thời gian khắc phục.
- Thêm regression test (`tests/integration/test_pipeline_e2e.py` có mẫu kịch bản lỗi).
