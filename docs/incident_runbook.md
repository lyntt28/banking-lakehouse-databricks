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

## 5. Lỗi giữa `silver:publish` / `gold:publish` (lỗi hạ tầng sau khi đã qua mọi gate)
Silver và Gold đều **stage → kiểm tra tất cả → publish**: lỗi DQ / đối soát không bao giờ để lại
bảng ghi dở. Chỉ lỗi hạ tầng trong chính bước publish mới có thể làm một số bảng đã ghi, số khác chưa.
Gold publish ghi THÊM dòng của batch mới (`batch_id` = run_id) rồi mới kích hoạt bằng 1 dòng vào
`ops.published_batches`. Người đọc chỉ đọc `gold_serving` (view lọc batch đã kích hoạt), nên batch
dở dang luôn vô hình. Xem `pipeline_runs.message` để biết lỗi xảy ra trước hay sau kích hoạt:
- **Trước kích hoạt** (message có "rollback N bảng"): job TỰ `RESTORE` các bảng Gold đã ghi về
  version trước publish (log `gold_rolled_back`) để dọn dòng batch lỗi. Người đọc vẫn ở batch cũ.
- **Sau kích hoạt** (message có "batch ĐÃ kích hoạt, không rollback"): batch mới đầy đủ và đang
  được dùng; job KHÔNG RESTORE (RESTORE lúc này sẽ để con trỏ trỏ vào batch đã bị xoá). Kiểm tra
  `SELECT * FROM <c>.ops.published_batches ORDER BY published_at DESC LIMIT 1`, đối chiếu số liệu,
  rồi chạy lại job để ghi SUCCESS (tạo batch mới cùng nội dung).
- **View gold_serving lỗi** (`ServingViewsInvalid`, message có "view gold_serving cần sửa"): batch
  đã kích hoạt nhưng view thiếu / không truy vấn được (vd: bảng mới, cột đổi kiểu). Sửa nguyên nhân
  rồi chạy `03_gold/02_dashboard_views` hoặc chạy lại job.
- `message` có "ROLLBACK FAILED": rollback tự động cũng lỗi -> làm tay theo Cách 2.
- Cách 1 (thường dùng): chạy lại job. Publish idempotent (batch mới thay batch cũ, dòng batch cũ
  được dọn sau kích hoạt) nên kết quả hội tụ.
- Cách 2 (cần quay lại ngay): version trước publish được lưu ở `ops.publish_versions`:
  ```sql
  SELECT table_name, version_before FROM <c>.ops.publish_versions WHERE run_id = '<run_id>';
  RESTORE TABLE <table_name> TO VERSION AS OF <version_before>;   -- từng bảng
  ```
  `version_before` NULL = bảng được tạo trong lần chạy đó → DROP nếu cần.

## 5b. `WriterBusy` (khoá ghi `ops.writer_lock`)
Mọi job ghi (daily, chạy tay Silver / Gold, backfill reset, bảo trì OPTIMIZE / VACUUM) giữ chung
1 khoá. Job thứ 2 dừng ngay với `WriterBusy`, không ghi gì.
- Có job khác đang chạy thật: chờ nó xong rồi chạy lại.
- Khoá treo (job trước bị kill, không nhả khoá): khoá tự hết hạn sau 6 giờ (`expires_at`). Cần
  gỡ sớm, sau khi CHẮC CHẮN không còn job nào chạy:
  ```sql
  SELECT * FROM <c>.ops.writer_lock;
  DELETE FROM <c>.ops.writer_lock WHERE owner = '<owner>';
  ```

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
