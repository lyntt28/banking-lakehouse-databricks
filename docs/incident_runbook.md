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
**Dashboard business có đang sai không?** Không, nếu lỗi xảy ra trước bước `gold:publish`:
Gold chỉ ghi sau gate. Nếu lỗi ở `gold:publish` (hạ tầng giữa chừng) → mục 5.

## 1. `SourceFilesMissing` (bước `file_manifest`)
- Xem `ops.file_manifest` ngày đó, `status = 'MISSING'`.
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

## 5. Lỗi giữa `silver:publish` / `gold:publish` (lỗi hạ tầng sau khi đã qua mọi gate)
Silver và Gold đều **stage → kiểm tra tất cả → publish**: lỗi DQ / đối soát không bao giờ để lại
bảng ghi dở. Chỉ lỗi hạ tầng trong chính bước publish mới có thể làm một số bảng đã ghi, số khác chưa.
- Cách 1 (thường dùng): chạy lại job. Publish idempotent (MERGE / replaceWhere / overwrite từ
  bản stage đã đạt) nên kết quả hội tụ.
- Cách 2 (cần quay lại ngay): version trước publish được lưu ở `ops.publish_versions`:
  ```sql
  SELECT table_name, version_before FROM <c>.ops.publish_versions WHERE run_id = '<run_id>';
  RESTORE TABLE <table_name> TO VERSION AS OF <version_before>;   -- từng bảng
  ```
  `version_before` NULL = bảng được tạo trong lần chạy đó → DROP nếu cần.

## 6. Correction / backfill ngày cũ (prod)
1. Dựng catalog song song (vd `bank_prod_rebuild`) với `05_backfill reset=true` từ Bronze.
2. So sánh với prod (đối soát, số dòng, tổng số dư).
3. Chuyển consumer (view / dashboard `dataset_catalog`) hoặc `DEEP CLONE` từng bảng vào prod
   trong cửa sổ bảo trì. Không drop bảng prod đang phục vụ.

## Sau sự cố
- Ghi lại: ngày, run_id, nguyên nhân, thời gian khắc phục.
- Thêm regression test (`tests/integration/test_pipeline_e2e.py` có mẫu kịch bản lỗi).
