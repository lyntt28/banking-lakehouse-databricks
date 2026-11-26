# Interview prep – Banking Lakehouse on Databricks

Câu trả lời gợi ý ngắn (nói 30–60 giây), luôn gắn với project này. ⭐ = rất hay hỏi.

## 1. Kiến trúc & Lakehouse
1. ⭐ **Medallion là gì, vì sao 3 tầng?** Bronze giữ nguyên dữ liệu nguồn (replay, audit),
   Silver làm sạch + chuẩn hóa + DQ (một nguồn sự thật), Gold mô hình theo nhu cầu dùng
   (star schema cho BI, feature table cho ML). Tách tầng để sửa logic mà không phải lấy lại nguồn.
2. ⭐ **Delta Lake khác Parquet thường?** Có transaction log → ACID, MERGE/UPDATE/DELETE,
   time travel, schema enforcement, OPTIMIZE/clustering. Project dùng MERGE cho upsert/SCD2,
   `replaceWhere` để ghi lại đúng 1 ngày.
3. **Vì sao Bronze để mọi cột string?** Không mất bản ghi sai định dạng; ép kiểu ở Silver bằng
   `try_cast` → giá trị sai thành NULL và bị rule DQ bắt, không làm hỏng job.
4. **Gold 2 nhánh?** BI cần star schema/mart tổng hợp; ML cần 1 dòng / KH / ngày, point-in-time.
   Cùng Silver nhưng hình dạng khác nhau.

## 2. Ingestion & idempotency
5. ⭐ **Auto Loader vs `spark.read`?** Auto Loader dùng checkpoint nhớ file đã đọc → chỉ đọc file
   mới, chạy lại không nhân đôi; `schemaEvolutionMode=rescue` đưa cột lạ vào `_rescued_data`.
6. ⭐ **Idempotent là gì, làm thế nào?** Chạy lại cùng input ra cùng kết quả: Bronze nhờ
   checkpoint, Silver MERGE theo khóa, quarantine xoá lô cũ trước khi ghi, Gold `replaceWhere`
   theo ngày. Kiểm chứng: chạy 2 lần số dòng không đổi (test `test_delta_io.py`).
7. **`availableNow` trigger?** Streaming nhưng chạy như batch: xử lý hết file mới rồi dừng → hợp
   với Job hằng ngày, vẫn giữ checkpoint.

## 3. Data quality
8. ⭐ **Thiết kế DQ gate?** Rule là biểu thức SQL; FAIL → quarantine kèm lý do, WARN → ghi log,
   vẫn đi tiếp. Rule FAIL > 5% → dừng job trước khi ghi Silver/Gold. Kết quả từng rule →
   `ops.dq_results` để làm dashboard DQ.
9. **Vì sao không xoá bản ghi lỗi?** Audit, báo nguồn sửa, xử lý lại; đối soát
   bronze = silver + quarantine + dòng trùng y hệt.
10. **Trùng khóa khác nội dung xử lý sao?** Quarantine cả các bản ghi (không đoán bản nào đúng);
    trùng y hệt (file gửi lặp) thì bỏ bớt và đếm riêng.
11. ⭐ **Đối soát với nguồn?** Gold tính lại KPI thẻ và số dư tiền gửi, so với file tổng hợp
    hệ thống nguồn gửi (`card_kpi`, `bang1247`) có dung sai → `ops.recon_*`; lệch → job lỗi.

## 4. Mô hình dữ liệu
12. ⭐ **SCD1 vs SCD2?** SCD1 ghi đè, mất lịch sử. SCD2 giữ phiên bản với `valid_from`,
    `valid_to`, `is_current`; dùng khi báo cáo quá khứ phải theo thuộc tính tại thời điểm đó
    (segment, chi nhánh của KH).
13. **SCD2 bằng 1 MERGE thế nào?** Staging nhân đôi bản ghi đổi: 1 dòng khớp khóa để đóng bản cũ,
    1 dòng `merge_key = NULL` để chèn bản mới; so `row_hash` để phát hiện thay đổi.
14. ⭐ **Fact vs dimension, grain?** `loan_fact_daily` grain = 1 khoản vay / ngày (snapshot);
    `core_fact_transaction` grain = 1 giao dịch. Dimension: customer (SCD2), branch, product, date.
15. **Periodic snapshot vs transaction fact?** Dư nợ là snapshot cuối ngày (cộng theo ngày
    không có nghĩa), giao dịch là transaction fact (cộng được).

## 5. Nghiệp vụ ngân hàng
16. ⭐ **Phân nhóm nợ, NPL?** TT 11/2021: DPD <10 nhóm 1, 10–89 nhóm 2, 90–179 nhóm 3,
    180–359 nhóm 4, ≥360 nhóm 5; NPL = nhóm 3–5. Silver tính lại từ DPD, nguồn lệch → WARN.
17. **Tỷ lệ quá hạn thẻ DPD 30+ / NPL 90+ tính trên gì?** Dư nợ các thẻ có DPD ≥ ngưỡng / tổng
    dư nợ thẻ đang hoạt động.

## 6. Governance (Unity Catalog)
18. ⭐ **Phân quyền?** Catalog / schema / table; GRANT theo nhóm (engineer ghi, analyst đọc
    Silver/Gold, branch user chỉ Gold). Mỗi môi trường 1 catalog.
19. **Che PII, lọc dòng?** Column mask (hàm SQL theo nhóm người xem) cho CCCD, SĐT, email, tên;
    row filter theo chi nhánh qua bảng `ops.user_branch`. Job ghi dữ liệu phải chạy bằng identity
    thấy PII, nếu không SCD2 sẽ băm giá trị đã che.
20. **Lineage?** Unity Catalog tự ghi lineage bảng/cột khi đọc–ghi qua Spark/SQL.

## 7. CI/CD & Git
21. ⭐ **DAB (Declarative Automation Bundles) là gì?** Mô tả job/notebook/biến bằng YAML; 1 code,
    nhiều target (dev/test/prod) chỉ khác biến (catalog, email, run_as). `bundle deploy -t prod`.
22. ⭐ **Pipeline CI/CD?** CI: mỗi PR chạy ruff + pytest (Spark + Delta local) + bundle validate.
    CD: merge `main` → deploy test → chạy Job smoke test → người duyệt (GitHub Environment) →
    deploy prod. Auth bằng OIDC, không lưu token.
23. **Branch / PR / merge commit / squash?** Mỗi phần 1 nhánh `feat/...`, PR để review + CI;
    PR xếp chồng thì dùng merge commit để không lặp lịch sử.
24. **So với quy trình ngân hàng?** Branch ≈ môi trường dev cá nhân, PR ≈ change request,
    CI ≈ QA tự động, approval prod ≈ CAB duyệt, Job ≈ lịch Control-M.

## 8. Tối ưu & chi phí
25. ⭐ **Tối ưu bảng Delta?** Liquid clustering theo cột hay lọc (`as_of_date`, `branchnbr`),
    OPTIMIZE gom file nhỏ, VACUUM dọn file cũ; đo trước/sau trong `ops.perf_benchmark`.
26. **Skew, AQE, broadcast?** AQE tự gộp partition nhỏ, tách partition lệch; dimension nhỏ
    (branch, product) broadcast khi join với fact.
27. **Giảm chi phí serverless?** Job `availableNow` thay vì stream chạy liên tục, chỉ xử lý
    ngày mới, mart ghi đè đúng ngày thay vì tính lại toàn bộ.

## 9. ML
28. **Point-in-time / leakage?** Feature chỉ dùng dữ liệu ≤ as_of; label "mua trong N ngày sau";
    KH đã có hợp đồng bị loại khỏi tập ứng viên.
29. **Đánh giá model cross-sell?** ROC-AUC, PR-AUC (dữ liệu lệch), lift top 10% cho chiến dịch;
    log MLflow, đăng ký model trong Unity Catalog.

## 10. Câu hỏi tình huống
30. **File nguồn hôm nay lỗi 30%?** DQ gate dừng trước khi ghi Gold, alert email, dashboard giữ
    số hôm qua; sửa nguồn → chạy lại cùng ngày (idempotent).
31. **Nguồn thêm cột mới?** Auto Loader rescue → `_rescued_data`; thêm cột vào cấu hình Silver
    (`optional`) để file cũ thiếu cột vẫn chạy.
32. **Dữ liệu về trễ 3 ngày?** Backfill theo thứ tự ngày; Silver/Gold ghi theo ngày nên chạy lại
    an toàn; SCD2 cần chạy đúng thứ tự thời gian.
