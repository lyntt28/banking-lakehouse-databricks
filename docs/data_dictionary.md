# Data dictionary – grain & lineage (Gold + Ops)

Ký hiệu ghi: **snapshot/day** = `replaceWhere as_of_date = D` (chạy lại / correction ghi đè cả
ngày); **merge** = MERGE theo khóa; **insert-only** = chỉ chèn khóa mới; **full** = ghi đè cả bảng.
Mọi bảng Gold chỉ được ghi sau khi ngày đó qua gate đối soát (`pipelines/gold.py`).

## Lineage
```
landing (CSV/JSON, Volume)
  -> bronze.<source>                 Auto Loader, append, string + _source_file/_as_of_date/_load_ts
  -> silver.<table>                  pipelines/silver.py: cast, dedup, DQ, quarantine, MERGE/SCD2
  -> gold_stage.<mart>               pipelines/gold.py: bản ứng viên để đối soát
  -> gold.<table>                    publish sau gate
  -> gold views (02_dashboard_views) -> 4 dashboard AI/BI; ML feature table
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
| `dep_mart_new_accounts` | tháng mở | full | core_accounts | |
| `cust_mart_360` | customer_id | full | dim_customer, facts | Trạng thái ngày chạy gần nhất |
| `card_mart_kpi_daily` | as_of_date × branchnbr × … | snapshot/day | card_master, card_transactions_hist, card_statements_hist | Gate: khớp file card_kpi |
| `card_mart_spend_mix` | as_of_date × dimension | snapshot/day | card_transactions_hist | |
| `card_mart_kpi_monthly` | month × branchnbr | full | card_mart_kpi_daily, ref_targets | |

## Ops
| Bảng | Grain | Dùng để |
|---|---|---|
| `pipeline_runs` | as_of_date × layer × run_id × status (append) | Watermark, lịch sử chạy |
| `task_runs` | run_id × step | Thời gian, số dòng, lỗi từng bước |
| `file_manifest` | as_of_date × table_name | File nào về / thiếu / gửi nhiều lần |
| `published_batches` | as_of_date × run_id | Batch Gold đã qua gate (consumer chỉ dùng ngày ở đây) |
| `dq_results` | run_id × table × rule | Kết quả DQ |
| `dq_quarantine` | bản ghi lỗi | **Restricted** – chứa PII gốc |
| `recon_card_kpi`, `recon_deposit` | as_of_date × chỉ tiêu | Kết quả đối soát |
