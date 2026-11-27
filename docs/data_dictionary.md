# Data dictionary – grain & lineage (Gold + Ops)

Gold publish theo **batch** (`common/serving.py`): mỗi lần publish ghi THÊM dòng có
`batch_id` (= run_id Gold) vào bảng `gold.*`, rồi kích hoạt bằng 1 dòng trong
`ops.published_batches`. Người đọc chỉ đọc view `gold_serving.<table>` (lọc batch đang dùng, bỏ
cột `batch_id`); không ai ngoài pipeline có quyền trên schema `gold`. Batch cũ được xoá sau khi
batch mới kích hoạt.

Ký hiệu batch đang dùng: **snapshot/day** = batch mới nhất của từng `as_of_date` (correction thay
cả ngày); **snapshot/batch** = cả bảng là batch mới nhất; **insert-only** = mọi dòng của batch đã
kích hoạt (fact bất biến, MERGE chỉ chèn khóa mới); **full** = bảng tính lại toàn bộ mỗi batch.
Mọi bảng Gold chỉ được ghi sau khi ngày đó qua gate đối soát (`pipelines/gold.py`).

## Lineage
```
landing (CSV/JSON, Volume)
  -> bronze.<source>                 Auto Loader, append, string + _source_file/_as_of_date/_load_ts
  -> silver.<table>                  pipelines/silver.py: cast, dedup, DQ, quarantine, MERGE/SCD2
  -> gold_stage.<mart>               pipelines/gold.py: bản ứng viên để đối soát
  -> gold.<table>                    publish sau gate: ghi thêm dòng của batch (chỉ pipeline đọc)
  -> ops.published_batches           kích hoạt batch (1 commit)
  -> gold_serving.<table>            view batch đang dùng (người đọc, Lakeview, ML)
  -> gold_serving.v_* (02_dashboard_views) -> dashboard AI/BI
```

## Gold
| Bảng | Grain | Ghi | Nguồn (Silver) | Ghi chú |
|---|---|---|---|---|
| `dim_date` | date_key | full | – | Từ ngày Silver sớm nhất / năm trước đến hết năm sau |
| `dim_branch` | branchnbr | full | ref_branches | |
| `dim_product` | prodcode | full | ref_products | |
| `dim_customer` | customer_sk (= hash customer_id, valid_from) | full | core_customers (SCD2) | Tối đa 1 phiên bản / KH / ngày; khoảng hiệu lực không chồng lấn |
| `loan_fact_daily` | as_of_date × acctnbr | snapshot/day | loan_snapshots | customer_sk theo as-of join |
| `core_fact_transaction` | txn_id | insert-only | core_transactions, core_accounts | Sửa giao dịch cần bút toán điều chỉnh từ nguồn |
| `loan_mart_overdue_daily` | as_of_date × branchnbr × product_group × scope × debt_group | snapshot/day | loan_fact_daily | Gate: Σ portfolio_balance = Σ dư nợ Silver |
| `loan_mart_npl_daily` | as_of_date | full | loan_mart_overdue_daily | So sánh D-1 / M-1 / Y-1 |
| `dep_mart_daily` | as_of_date × branchnbr × product_type × currencycd | snapshot/day | core_accounts (snapshot ngày) | Gate: khớp bang1247 |
| `dep_mart_trend` | as_of_date × … | snapshot/day | core_accounts, ref_fx_rates | |
| `dep_mart_by_tenor` | as_of_date × tenor | snapshot/day | core_accounts | |
| `dep_mart_casa_customer` | as_of_date × … | snapshot/day | core_accounts, dim_customer | |
| `dep_mart_new_accounts` | as_of_date × tháng mở | snapshot/batch | core_accounts | Tính lại toàn bộ, lưu 1 bản / batch: dashboard đọc bản của batch đã publish |
| `cust_mart_360` | customer_id | full | dim_customer, facts | Trạng thái ngày chạy gần nhất |
| `card_mart_kpi_daily` | as_of_date × branchnbr × … | snapshot/day | card_master, card_transactions_hist, card_statements_hist | Gate: khớp file card_kpi |
| `card_mart_spend_mix` | as_of_date × dimension | snapshot/day | card_transactions_hist | |
| `card_mart_kpi_monthly` | as_of_date × month × branchnbr | snapshot/batch | card_mart_kpi_daily, ref_targets | Tính lại toàn bộ, lưu 1 bản / batch |

## Ops
| Bảng | Grain | Dùng để |
|---|---|---|
| `pipeline_runs` | as_of_date × layer × run_id × status (append) | Watermark, lịch sử chạy |
| `task_runs` | run_id × step | Thời gian, số dòng, lỗi từng bước |
| `file_manifest` | as_of_date × table_name | File nào về / thiếu / gửi nhiều lần |
| `published_batches` | as_of_date × run_id | Con trỏ kích hoạt: batch đang dùng của ngày D = dòng `published_at` mới nhất |
| `publish_versions` | run_id × bảng | Version trước publish, để rollback batch CHƯA kích hoạt |
| `writer_lock` | 1 dòng (`lakehouse`) | Khóa ghi chung: daily / silver / gold / maintenance / backfill không chạy song song |
| `dq_results` | run_id × table × rule | Kết quả DQ |
| `dq_quarantine` | bản ghi lỗi | **Restricted** – chứa PII gốc |
| `recon_card_kpi`, `recon_deposit` | as_of_date × chỉ tiêu | Kết quả đối soát |
