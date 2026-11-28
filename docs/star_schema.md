# Gold star schema

Gold is organised as a Kimball star schema with **conformed dimensions**. Every dashboard mart is
an aggregate built **from the facts** (not from Silver directly), so all marts share the same
grain, keys and definitions. Code: `src/common/gold.py` (dims, facts), `gold_deposit.py` /
`gold_cards.py` (marts), wired in `src/pipelines/gold.py`.

```
                                   dim_date (date_key)
                                         │
     dim_customer (customer_sk, SCD2) ───┼─── dim_branch (branchnbr; role-playing mng_br / sal_br)
                                         │
                    dim_product (prodcode)    dim_card (card_no, SCD1)
                                         │
 ┌───────────────────┬───────────────────┼────────────────────┬──────────────────────┐
 loan_fact_daily   core_fact_transaction  fact_deposit_daily   fact_card_daily   fact_card_transaction
 (snapshot)        (transaction)          (snapshot)           (snapshot)        (transaction)
     │                                        │                   │     │                │
 loan_mart_overdue_daily               dep_mart_daily        card_mart_kpi_daily   card_mart_spend_mix
 loan_mart_npl_daily                   dep_mart_trend ─► dep_mart_monthly  │
 cust_mart_360                         dep_mart_by_tenor     card_mart_kpi_monthly (+ MTD distinct
                                       dep_mart_casa_customer                 cards from fact_card_transaction)

 card_mart_overdue_daily: aggregate fact at the source-file grain (no account-level source),
 conformed to dim_date and dim_branch (management / sales branch).
```

## Dimensions

| Table | Key | Type | Source |
|---|---|---|---|
| `dim_date` | `date_key` (yyyyMMdd) | static calendar | generated |
| `dim_branch` | `branchnbr` | SCD1 | `silver.ref_branches` |
| `dim_product` | `prodcode` | SCD1 | `silver.ref_products` |
| `dim_customer` | `customer_sk` = hash(customer_id, valid_from) | **SCD2** | `silver.core_customers` |
| `dim_card` | `card_no` | SCD1 (daily status lives in the fact) | `silver.card_master` |

## Facts

| Table | Grain | Type | Keys | Measures |
|---|---|---|---|---|
| `loan_fact_daily` | as_of_date × acctnbr | periodic snapshot | date_key, customer_sk, branchnbr, prodcode | balance, balance_vnd, dpd, debt_group … |
| `core_fact_transaction` | txn_id | transaction (insert-only) | date_key, customer_sk, branchnbr | amount, signed_amount |
| `fact_deposit_daily` | as_of_date × account_id | periodic snapshot | date_key, customer_sk, branchnbr, product_type→prodcode | balance, balance_vnd (same-day FX), int_rate, tenor_months |
| `fact_card_daily` | as_of_date × card_no | periodic snapshot | date_key, customer_sk, branchnbr, card_no | credit_limit, outstanding / revolving balance, dpd |
| `fact_card_transaction` | txn_id | transaction (insert-only) | date_key, customer_sk, branchnbr, card_no | amount |
| `card_mart_overdue_daily` | as_of_date × product × mng_br × sal_br × del_grp | aggregate fact | date_key, mng_br / sal_br → dim_branch | port/ovd accounts and balances |

`customer_sk` is resolved **as of the fact date** (the customer version effective that day).
It is NULL only when the customer is not in `dim_customer` — a customer record quarantined in
Silver (attribute error) whose id is still a valid account / card owner. Tests check that NULL
never comes from a wrong join. (A Kimball "unknown member" row with key -1 is a possible
follow-up; not added to avoid changing existing dim_customer consumers.)

## Rules
- Marts read facts only; facts read Silver. Facts keep Silver column names so mart logic is
  unchanged (tests prove mart-from-fact = mart-from-Silver).
- Facts are published with the same atomic batch mechanism (`common.serving.TABLE_KIND`):
  snapshot facts as `daily`, transaction facts as `append` (MERGE insert-only on txn_id),
  `dim_card` as `snapshot`.
- Branch row filter applies to all new facts and `dim_card` (`common.governance.BRANCH_FILTERED`).

## Tests
`tests/test_star_schema.py`: unique grain per fact/dim, every foreign key resolves to its
dimension (customer_sk, branchnbr, card_no, product), card KPI and deposit marts built from facts
equal the marts built from Silver, `balance_vnd` = balance × FX.

## Migration
New tables `dim_card`, `fact_deposit_daily`, `fact_card_daily`, `fact_card_transaction` are
created on the first Gold run. The MTD distinct-cards count reads `fact_card_transaction`, so run
the planned history replay (backfill) once so the fact covers the current month.
