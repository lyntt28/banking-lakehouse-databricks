# Runbook – chạy project trên Databricks (Free Edition)

Chạy tay lần đầu theo đúng thứ tự dưới đây. Mỗi bước: mở notebook trong Git folder →
chọn **Serverless** → điền widget → **Run all** → chạy câu SQL **KIỂM TRA**.
Khi mọi bước đã đúng mới deploy Job bằng bundle (bước 10).

```
[1] setup → [2] sinh data → [3] bronze → [4] backfill (silver + gold mọi ngày)
→ [5] dashboard views → [6] governance → [7] ML → [8] maintenance/benchmark
→ [9] dashboard SQL → [10] bundle deploy + Job
```

Trước mỗi lần chạy: Git folder → nút nhánh **main** → **Pull** để lấy code mới nhất.

---

## 1. `src/00_setup/01_create_catalog_schema`
- **MỤC ĐÍCH:** tạo catalog, 5 schema (landing/bronze/silver/gold/ops), volume `raw_files`,
  bảng `ops.dq_results`, `ops.dq_quarantine`.
- **Widget:** `catalog = bank_dev`
- **KIỂM TRA:**
  ```sql
  SHOW SCHEMAS IN bank_dev;               -- bronze, gold, landing, ops, silver
  SHOW VOLUMES IN bank_dev.landing;       -- raw_files
  ```
- **NẾU LỖI:** không có quyền tạo catalog → Catalog → + → Create catalog `bank_dev`, chạy lại.

## 2. `src/00_setup/02_generate_sample_data`
- **MỤC ĐÍCH:** giả lập hệ thống nguồn đẩy file mỗi ngày (15 nguồn, có lỗi cố ý) +
  `history_months` snapshot cuối tháng để có MoM / YoY.
- **Widget:** `catalog = bank_dev`, `start_date = 2026-11-01`, `days = 3`, `history_months = 13`
- **KIỂM TRA:**
  ```sql
  LIST '/Volumes/bank_dev/landing/raw_files/loans/';   -- thư mục as_of_date=... (13 tháng + 3 ngày)
  SELECT * FROM read_files('/Volumes/bank_dev/landing/raw_files/loans/',
                           format => 'csv', header => true) LIMIT 5;  -- etl_dt dạng MM/DD/YYYY
  ```
- **NẾU LỖI:** `ModuleNotFoundError: data_generator` → notebook không thấy thư mục gốc repo;
  gửi ảnh cell lỗi.

## 3. `src/01_bronze/01_bronze_ingest`
- **MỤC ĐÍCH:** Auto Loader nạp file mới vào `bank_dev.bronze.<bảng>`, giữ nguyên mọi cột
  dạng string + `_source_file`, `_as_of_date`, `_load_ts`.
- **Widget:** `catalog = bank_dev`, `tables` để trống (= tất cả)
- **KIỂM TRA:**
  ```sql
  SELECT _as_of_date, count(*) FROM bank_dev.bronze.loans GROUP BY 1 ORDER BY 1;
  ```
  **Run all lần 2 → số dòng không đổi** (idempotent, checkpoint nhớ file đã đọc).
- **NẾU LỖI:** `PATH_NOT_FOUND` → bước 2 chưa chạy; schema lạ → xem cột `_rescued_data`.

## 4. `src/00_setup/05_backfill` (Silver + Gold cho mọi ngày, cũ → mới)
- **MỤC ĐÍCH:** gọi `02_silver/00_daily_silver_gold` không truyền ngày: xử lý mọi ngày sau
  watermark trong `ops.pipeline_runs`, mỗi ngày Silver rồi Gold.
- **Widget:** `catalog = bank_dev`, `reset = false`
- **`reset = true`:** chỉ dev/test (bị chặn khi catalog là `*_prod`). Drop Silver, Gold, gold_stage
  và trạng thái batch rồi dựng lại; sau đó chạy lại governance.
- **Lưu ý SCD2:** `silver.core_customers` từ chối nạp ngày cũ hơn bản hiện hành (báo lỗi
  "thứ tự ngày"). Xem mục *Ngữ nghĩa chạy lại* bên dưới.
- **KIỂM TRA:**
  ```sql
  -- DQ từng rule, ngày mới nhất
  SELECT table_name, rule_name, severity, failed_rows, fail_pct, status
  FROM bank_dev.ops.dq_results ORDER BY run_ts DESC, table_name LIMIT 60;
  -- lý do quarantine
  SELECT table_name, explode(failed_rules) AS rule, count(*) FROM bank_dev.ops.dq_quarantine
  GROUP BY 1, 2 ORDER BY 3 DESC;
  -- SCD2: KH có nhiều phiên bản
  SELECT customer_id, province, segment, valid_from, valid_to, is_current
  FROM bank_dev.silver.core_customers WHERE customer_id IN
    (SELECT customer_id FROM bank_dev.silver.core_customers GROUP BY 1 HAVING count(*) > 1)
  ORDER BY customer_id, valid_from LIMIT 20;
  -- đối soát với file nguồn: phải toàn MATCH
  SELECT status, count(*) FROM bank_dev.ops.recon_card_kpi GROUP BY 1;
  SELECT status, count(*) FROM bank_dev.ops.recon_deposit GROUP BY 1;
  -- NPL theo ngày
  SELECT * FROM bank_dev.gold.loan_mart_npl_daily ORDER BY as_of_date DESC LIMIT 10;
  ```
  Đúng: fail_pct các rule FAIL < 5%; NPL ~3%; recon toàn `MATCH`.
- **NẾU LỖI:**
  - `DQ gate: ... > 5%` → xem `ops.dq_results` rule nào vượt; data sinh lỗi quá nhiều?
  - `Đối soát lệch` → bronze ≠ silver + quarantine + exact_dupes + superseded; gửi bảng report cuối notebook.
  - recon `MISMATCH` → gửi `SELECT * FROM bank_dev.ops.recon_card_kpi WHERE status <> 'MATCH'`.

## 5. `src/03_gold/02_dashboard_views`
- **MỤC ĐÍCH:** tạo 21 view `gold.v_*`, mỗi view = 1 chart của 3 dashboard
  (`docs/gold_dashboards.md`).
- **KIỂM TRA:** `SHOW VIEWS IN bank_dev.gold;` và `SELECT * FROM bank_dev.gold.v_overdue_tickers;`

## 6. `src/00_setup/03_governance`
- **MỤC ĐÍCH:** GRANT theo nhóm, che PII (CCCD, SĐT, email, tên…), lọc dòng theo chi nhánh.
- **Widget:** `dry_run = true` trên **Free Edition** (không có account groups → chỉ in SQL).
- **KIỂM TRA:** đọc các câu SQL in ra; trên Azure (đợt 2) chạy `dry_run = false`.
- **NẾU LỖI:** `PRINCIPAL_DOES_NOT_EXIST` → đang chạy `dry_run = false` mà chưa có group.

## 7. `src/04_ml/01_insurance_cross_sell`
- **MỤC ĐÍCH:** feature table `gold.ins_feature_customer`, train model, log MLflow,
  chấm điểm KH chưa có bảo hiểm → `gold.ins_propensity_scores`.
- **Widget:** `label_mode = holder`, `register = false` nếu Free Edition báo lỗi registry.
- **KIỂM TRA:** metrics in ra (roc_auc > 0.7 là có tín hiệu); Experiments → `insurance_cross_sell`;
  ```sql
  SELECT score_decile, count(*), avg(score) FROM bank_dev.gold.ins_propensity_scores
  GROUP BY 1 ORDER BY 1;
  ```

## 8. `src/00_setup/04_maintenance`
- **MỤC ĐÍCH:** đo thời gian query trước/sau liquid clustering + OPTIMIZE → `ops.perf_benchmark`.
- **Widget:** `mode = benchmark`, `repeats = 3`
- **KIỂM TRA:** `SELECT * FROM bank_dev.ops.perf_benchmark ORDER BY run_ts DESC;` – chụp lại
  cho README (số liệu trước/sau).

## 9. Dashboard Databricks SQL
Dashboards → Create → mỗi chart chọn 1 view `gold.v_*` (tên view = tên chart). Bộ lọc:
ngày, vùng, chi nhánh, sản phẩm. Chụp ảnh 3 dashboard cho README.

## 10. Deploy Job bằng bundle
```bash
databricks auth login --host <workspace-url>
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run banking_lakehouse_daily -t dev
```
- **KIỂM TRA:** Jobs & Pipelines → `[dev ...] banking_lakehouse_daily` → các task xanh;
  chạy lại lần 2 số dòng các bảng không đổi. Chụp ảnh Job graph + lineage (Catalog → bảng →
  Lineage) cho README.

---

## Batch control & ngữ nghĩa chạy lại

**Bảng trạng thái**
- `ops.pipeline_runs`: mỗi ngày dữ liệu × layer (silver/gold) × lần chạy: RUNNING → SUCCESS/FAILED.
  Watermark = ngày SUCCESS lớn nhất. Lần đầu nâng cấp (chưa có bảng) → suy từ dữ liệu sẵn có.
- `ops.published_batches`: con trỏ kích hoạt batch Gold (as_of_date, run_id, published_at, số
  liệu gate dạng JSON). Batch đang dùng của ngày D = dòng `published_at` mới nhất của D.
  Consumer (dashboard, ML) đọc schema `gold_serving` (view chỉ thấy batch đang dùng, không đọc
  bảng `gold` trực tiếp) và ML dừng nếu batch mới nhất cũ hơn `max_gold_age_days`.
- `ops.writer_lock`: khoá ghi dùng chung cho daily / backfill / bảo trì / chạy tay
  (`max_concurrent_runs: 1` chỉ chặn trong 1 job). Job thứ 2 dừng với `WriterBusy`.

**Luồng** (task `silver_gold` → `src/pipelines/daily.py`): với từng ngày chờ, chạy Silver
rồi Gold ngay, sau đó mới sang ngày sau. Bắt buộc xen kẽ vì Silver snapshot (TK, thẻ) là MERGE
chỉ giữ trạng thái mới nhất: Gold ngày D phải dựng trước khi Silver ngày D+1 ghi đè. Vì vậy:
- chạy lại Silver ngày cũ hơn watermark → `OutOfOrder`; Gold ngày cũ hơn Silver → `NotReproducible`
  (chặn trước khi ghi). Sửa ngày cũ = replay, xem `docs/incident_runbook.md` mục 6.
- Gold ngày D fail gate → lần chạy sau dựng lại Gold ngày D trước khi xử lý ngày mới.

**Từng ngày**
1. Silver: mọi bảng chạy DQ + đối soát số dòng và ghi vào `silver_stage` trước; chỉ khi TẤT CẢ
   đạt mới publish vào `silver` (bảng thứ 5 lỗi thì 4 bảng trước vẫn nguyên). Thiếu file của bảng bắt buộc (mọi bảng trừ `ref_branches`, `ref_products`,
   `ref_targets`) → FAILED, dừng; các ngày sau **không** chạy (SCD2 cần đúng thứ tự).
2. Gold: tính mart dùng để đối soát → ghi `gold_stage` → đối soát (dư nợ Silver–Gold, KPI thẻ,
   tiền gửi bang1247) → lệch thì dừng, **bảng Gold mà dashboard đọc không bị đụng tới**;
   đạt thì mới publish: ghi THÊM dòng của batch mới (`batch_id`) vào `gold` (vô hình với người
   đọc), rồi kích hoạt bằng 1 dòng vào `ops.published_batches` (1 commit = nguyên tử: người đọc
   thấy toàn bộ batch mới hoặc toàn bộ batch cũ), rồi dọn dòng batch cũ và kiểm tra view
   `gold_serving`. Lỗi trước kích hoạt → RESTORE dọn batch lỗi; lỗi sau kích hoạt → không
   rollback, run FAILED để kiểm tra (`docs/incident_runbook.md` mục 5).

| Tình huống | Làm gì | Cơ chế |
|---|---|---|
| **Retry** (task lỗi tạm thời) | Job tự retry / chạy lại job | Idempotent: MERGE theo khóa, replaceWhere theo ngày, SCD2 so hash |
| **Gián đoạn nhiều ngày** | Chạy job như thường, `as_of_date` trống | Xử lý mọi ngày sau watermark |
| **File về trễ** | Chờ file, chạy lại job | Ngày thiếu file FAILED, watermark đứng yên. Ngày KHÔNG có file nào (kể cả loans) cũng được tính theo lịch nghiệp vụ nên không bị nhảy qua |
| **Rerun 1 ngày** (cùng dữ liệu) | Job với `as_of_date=<ngày>` | Không đổi kết quả |
| **Correction** (nguồn gửi lại file sửa của ngày D = ngày mới nhất đã xử lý) | Đặt file mới (tên khác, đủ nội dung cả ngày) vào landing; chạy job như thường | Job tự nhận ra file nạp sau lần Silver SUCCESS → chạy lại Silver rồi Gold ngày D. File nạp sau cùng **thay thế toàn bộ** file trước của cùng bảng / nhóm file (`latest_delivery`, log `file_superseded`). `loan_snapshots` + snapshot Gold: ghi đè cả ngày → dòng bị rút cũng biến mất. Bảng snapshot MERGE (`core_accounts`, `card_master`, ...): giá trị sửa được cập nhật, nhưng thực thể bị rút khỏi file sửa **vẫn còn** → cần replay. SCD2 cùng ngày: sửa tại chỗ phiên bản ngày D. Giao dịch (`core_transactions`, insert-only): **không** cập nhật bản đã có – cần quy trình sửa riêng (reversal/adjustment từ nguồn) |
| **Correction / backfill ngày cũ hơn watermark** | dev/test: `05_backfill reset=true`. prod: dựng catalog song song từ Bronze, kiểm tra, rồi chuyển consumer (`docs/incident_runbook.md`) | Bị chặn (`OutOfOrder`): snapshot MERGE + SCD2 không cho ghi vào quá khứ |

**SCD2 (`silver.core_customers`)**: ngữ nghĩa *daily snapshot* – tối đa 1 phiên bản / khách hàng /
ngày, khoảng `[valid_from, valid_to]` không chồng lấn → as-of join ra đúng 1 dòng, `customer_sk`
(hash `customer_id`, `valid_from`) duy nhất.

## Identity & quyền
- **Deploy identity**: service principal dùng trong CI/CD qua GitHub OIDC (không PAT); federation
  policy riêng cho `environment:ci` (chỉ validate), `environment:test`, `environment:prod`.
- **Runtime identity**: service principal khác (`run_as` trong `databricks.yml`, biến `run_as_sp`),
  thuộc `data_engineers` để thấy PII khi ghi (xem `assert_pipeline_identity`).
- Governance: prod chạy `dry_run=false`. Nhóm đọc giới hạn được cấp SELECT **theo bảng**
  (`TABLE_GRANTS`), không theo schema; `ops.dq_quarantine` (record_json chứa PII gốc) chỉ
  `data_engineers`/`bank_admins` đọc được. Trên Free Edition đây là **governance simulation**
  (không có account group) – chưa được enforce thật.

## Quan sát
- `ops.task_runs`: từng bước (file_manifest, silver:<bảng>, gold:build_stage, gold:gate,
  gold:publish) với thời gian, số dòng, lỗi. `ops.file_manifest`: file về / thiếu / gửi nhiều lần.
- Log driver: mỗi dòng là JSON (`event`, `run_id`, `as_of_date`, ...) – lọc theo `run_id`.
- Dashboard **Lakehouse Operations Monitor** (`dashboards/ops_monitor.lvdash.json`).
- Xử lý sự cố: `docs/incident_runbook.md`.

## Trạng thái kiểm chứng
| Phần | Unit test (CI) | Chạy trên Databricks |
|---|---|---|
| Generator, Bronze, Silver, Gold, views, governance SQL, ML features | ✅ | ⬜ |
| MERGE / SCD2 / replaceWhere trên Delta (local) | ✅ (PR #13) | ⬜ |
| Batch control, SCD2 cùng ngày, governance grants (unit test) | ✅ | ⬜ |
| Silver → Gold end-to-end + kịch bản lỗi (thiếu file, file trễ, file trùng, chạy lại, gate lệch, chạy sai thứ tự) | ✅ integration test, Delta local | ⬜ |
| Bundle validate / deploy, CD | ⬜ (`CD_ENABLED` chưa bật) | ⬜ |

## Quy ước tên bảng (tiền tố theo nghiệp vụ)

Tiền tố: `ref_` danh mục, `core_` khách hàng/TK/giao dịch, `loan_` tín dụng, `dep_` tiền gửi,
`card_` thẻ, `ins_` bảo hiểm, `cust_` khách hàng 360. Bronze giữ tên thư mục landing (= tên file
nguồn); Silver map qua `SilverTable.source`. Dimension dùng chung giữ `dim_*`; `ops.*` giữ nguyên.

| Tầng | Tên cũ | Tên mới |
|---|---|---|
| silver | branches / products / fx_rates / targets | ref_branches / ref_products / ref_fx_rates / ref_targets |
| silver | customers / accounts / transactions | core_customers / core_accounts / core_transactions |
| silver | loans | loan_snapshots |
| silver | cards / card_transactions / card_statements / card_kpi | card_master / card_transactions_hist / card_statements_hist / card_kpi_source |
| silver | deposit_summary / insurance_policies | dep_summary_source / ins_policies |
| gold | fact_loan_daily / mart_loan_overdue_daily / mart_npl_daily | loan_fact_daily / loan_mart_overdue_daily / loan_mart_npl_daily |
| gold | fact_transaction / mart_customer_360 | core_fact_transaction / cust_mart_360 |
| gold | mart_deposit_daily / _trend / _by_tenor / mart_casa_customer / mart_deposit_new_accounts | dep_mart_daily / dep_mart_trend / dep_mart_by_tenor / dep_mart_casa_customer / dep_mart_new_accounts |
| gold | mart_card_kpi_daily / _monthly / mart_card_spend_mix | card_mart_kpi_daily / card_mart_kpi_monthly / card_mart_spend_mix |
| gold | feature_customer_insurance / insurance_propensity_scores / model insurance_propensity | ins_feature_customer / ins_propensity_scores / ins_propensity_model |

Sau khi đổi tên: deploy lại bundle, chạy lại `05_backfill` (Silver/Gold tạo bảng mới), chạy
`02_dashboard_views` và import lại dashboard. Bảng tên cũ không còn dùng, xoá bằng `DROP TABLE` khi đã kiểm tra.

## Test end-to-end nhiều ngày (`tests/test_pipeline_e2e.py`)

Chạy **chính notebook** `02_silver/01_silver_transform` và `03_gold/01_gold_build` qua 3 ngày
(cuối tháng + 2 ngày liên tiếp) trên Spark + Delta local, giống vòng lặp `05_backfill`; chỉ giả
lập `dbutils` / `display`. Bronze nạp thẳng từ file (Auto Loader không chạy local).
Kiểm tra: mọi ngày qua được assert đối soát trong Gold, `ops.recon_*` toàn MATCH, chạy lại
ngày cuối không đổi số dòng. Lỗi kiểu "ngày đầu chạy được, ngày 2 lỗi" phải đỏ ở đây (CI) trước
khi lên Databricks.

```bash
pytest -q tests/test_pipeline_e2e.py
```

Quy tắc file nguồn bang1247 (generator): cộng mọi TK tiền gửi, trừ TK có `customer_id` chưa
từng xuất hiện hợp lệ trong file khách hàng (hôm nay hoặc các ngày trước) — đúng bằng tập TK
mà Silver đưa vào quarantine theo FK.
