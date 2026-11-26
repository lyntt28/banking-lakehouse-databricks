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
- **MỤC ĐÍCH:** giả lập hệ thống nguồn đẩy file mỗi ngày (13 bảng, có lỗi cố ý) +
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
- **MỤC ĐÍCH:** chạy `02_silver/01_silver_transform` rồi `03_gold/01_gold_build` cho từng ngày
  theo thứ tự thời gian (SCD2 và so sánh D-1/M-1/Y-1 cần đúng thứ tự).
- **Widget:** `catalog = bank_dev`, `from_date` để trống
- **Lưu ý SCD2:** `silver.core_customers` từ chối nạp ngày cũ hơn bản hiện hành (báo lỗi
  "thứ tự ngày"). Muốn chạy lại backfill từ ngày cũ: `DROP TABLE <catalog>.silver.core_customers`
  (và `gold.dim_customer`) trước, rồi chạy lại từ đầu.
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
  - `Đối soát lệch` → bronze ≠ silver + quarantine + exact_dupes; gửi bảng report cuối notebook.
  - recon `MISMATCH` → gửi `SELECT * FROM bank_dev.ops.recon_card_kpi WHERE status <> 'MATCH'`.

## 5. `src/03_gold/02_dashboard_views`
- **MỤC ĐÍCH:** tạo 21 view `gold.v_*`, mỗi view = 1 chart của 3 dashboard
  (`docs/gold_dashboards.md`, code cũ trong `reference/dashboards/`).
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

## Trạng thái kiểm chứng
| Phần | Unit test (CI) | Chạy trên Databricks |
|---|---|---|
| Generator, Bronze, Silver, Gold, views, governance SQL, ML features | ✅ | ⬜ |
| MERGE / SCD2 / replaceWhere trên Delta (local) | ✅ (PR #13) | ⬜ |
| Bundle validate / deploy, CD | ⬜ (chưa có secret) | ⬜ |

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
