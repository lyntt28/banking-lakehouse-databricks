# Dashboard query benchmark

Warehouse `53685e6f148121ba`, catalog `bank_dev`, report_date 2026-11-28, 2 runs per dataset, params {'branch': 'all', 'customer_type': 'consumer', 'region': 'all', 'product': 'all', 'currency': 'all', 'nationality': 'all'}. Measured 2026-11-28 11:25 UTC.

### Before refactor (2c7c15e)

| Dashboard | Dataset | State | 1st run (ms) | Median next runs (ms) | Read (MB) | Files read | Rows |
|---|---|---|---:|---:|---:|---:|---:|
| card_business | card_kpi | SUCCEEDED  | 4289 | 343 | 1.01 | 115 | 1 |
| card_business | card_kpi_compare | SUCCEEDED  | 56787 | 580 | 2.47 | 167 | 4 |
| card_business | card_new_acq | SUCCEEDED  | 1468 | 382 | 0.76 | 81 | 14 |
| card_business | card_in_out | SUCCEEDED  | 2521 | 286 | 1.18 | 99 | 14 |
| card_business | card_base_using | SUCCEEDED  | 2264 | 279 | 1.18 | 99 | 14 |
| card_business | card_balance | SUCCEEDED  | 2893 | 302 | 1.23 | 120 | 14 |
| card_business | card_volume | SUCCEEDED  | 1559 | 243 | 0.96 | 108 | 7 |
| card_business | card_ticket | SUCCEEDED  | 1520 | 255 | 0.96 | 108 | 7 |
| card_business | card_mix_sector | SUCCEEDED  | 9882 | 251 | 0.64 | 62 | 4 |
| card_business | card_mix_location | SUCCEEDED  | 5284 | 277 | 0.61 | 58 | 2 |
| card_business | card_mix_channel | SUCCEEDED  | 5087 | 238 | 0.61 | 58 | 2 |
| card_business | card_risk_compare | SUCCEEDED  | 4579 | 279 | 0.88 | 89 | 2 |
| card_business | card_customer_types | SUCCEEDED  | 242 | 209 | 0.00 | 0 | 3 |
| loan_overdue | ovd_fx | SUCCEEDED  | 501 | 174 | 0.05 | 21 | 1 |
| loan_overdue | ovd_tickers | SUCCEEDED  | 1879 | 247 | 0.48 | 78 | 1 |
| loan_overdue | ovd_compare | SUCCEEDED  | 3878 | 252 | 0.85 | 127 | 3 |
| loan_overdue | ovd_last5 | SUCCEEDED  | 1643 | 233 | 0.76 | 118 | 5 |
| loan_overdue | ovd_monthly | SUCCEEDED  | 1565 | 241 | 0.76 | 118 | 6 |
| loan_overdue | ovd_debt_groups | SUCCEEDED  | 1582 | 241 | 0.76 | 118 | 11 |
| loan_overdue | ovd_product_amount | SUCCEEDED  | 1526 | 284 | 0.48 | 78 | 5 |
| loan_overdue | ovd_product_ratio | SUCCEEDED  | 1515 | 238 | 0.43 | 57 | 5 |
| retail_deposit | dep_tickers | SUCCEEDED  | 3671 | 314 | 1.86 | 166 | 1 |
| retail_deposit | dep_compare | SUCCEEDED  | 6911 | 314 | 4.72 | 276 | 3 |
| retail_deposit | dep_gauges | SUCCEEDED  | 2606 | 318 | 1.09 | 84 | 1 |
| retail_deposit | dep_daily30 | SUCCEEDED  | 1325 | 279 | 1.40 | 107 | 10 |
| retail_deposit | dep_mom | SUCCEEDED  | 1234 | 294 | 1.40 | 107 | 39 |
| retail_deposit | dep_region | SUCCEEDED  | 1620 | 278 | 1.07 | 78 | 4 |
| retail_deposit | dep_term | SUCCEEDED  | 1922 | 249 | 0.94 | 104 | 4 |
| retail_deposit | dep_casa_customer | SUCCEEDED  | 4286 | 233 | 0.27 | 89 | 3 |
| retail_deposit | dep_edusave | SUCCEEDED  | 1647 | 253 | 0.81 | 82 | 12 |
| retail_deposit | dep_by_product | SUCCEEDED  | 10284 | 373 | 4.35 | 181 | 10 |

**Total:** 31 queries, 1st run 147,970 ms, warm 8,739 ms, read 34.98 MB

### After refactor (current)

| Dashboard | Dataset | State | 1st run (ms) | Median next runs (ms) | Read (MB) | Files read | Rows |
|---|---|---|---:|---:|---:|---:|---:|
| card_business | card_kpi | SUCCEEDED  | 1845 | 276 | 1.50 | 109 | 1 |
| card_business | card_kpi_compare | SUCCEEDED  | 4271 | 361 | 3.59 | 157 | 4 |
| card_business | card_new_acq | SUCCEEDED  | 1222 | 309 | 1.43 | 85 | 12 |
| card_business | card_in_out | SUCCEEDED  | 1733 | 269 | 1.95 | 101 | 12 |
| card_business | card_base_using | SUCCEEDED  | 1879 | 258 | 1.95 | 101 | 12 |
| card_business | card_balance | SUCCEEDED  | 2442 | 298 | 2.05 | 128 | 12 |
| card_business | card_volume | SUCCEEDED  | 1850 | 311 | 1.52 | 112 | 6 |
| card_business | card_ticket | SUCCEEDED  | 1522 | 270 | 1.52 | 112 | 6 |
| card_business | card_mix_sector | SUCCEEDED  | 1475 | 290 | 1.45 | 88 | 4 |
| card_business | card_mix_location | SUCCEEDED  | 1256 | 357 | 1.45 | 88 | 2 |
| card_business | card_mix_channel | SUCCEEDED  | 1298 | 415 | 1.45 | 88 | 2 |
| card_business | card_risk_compare | SUCCEEDED  | 2504 | 304 | 0.78 | 126 | 2 |
| card_business | card_kpi_cards | SUCCEEDED  | 4822 | 548 | 4.15 | 191 | 5 |
| card_business | card_customer_types | SUCCEEDED  | 202 | 204 | 0.00 | 0 | 3 |
| loan_overdue | ovd_kpi | SUCCEEDED  | 1581 | 249 | 0.63 | 142 | 1 |
| loan_overdue | ovd_compare | SUCCEEDED  | 2060 | 277 | 0.75 | 153 | 3 |
| loan_overdue | ovd_daily_trend | SUCCEEDED  | 1270 | 222 | 0.58 | 120 | 5 |
| loan_overdue | ovd_monthly_trend | SUCCEEDED  | 1473 | 224 | 0.58 | 120 | 6 |
| loan_overdue | ovd_debt_groups | SUCCEEDED  | 1333 | 235 | 0.56 | 115 | 55 |
| loan_overdue | ovd_product | SUCCEEDED  | 1110 | 232 | 0.42 | 91 | 5 |
| loan_overdue | ovd_product_top5 | SUCCEEDED  | 1024 | 220 | 0.37 | 70 | 5 |
| loan_overdue | ovd_branch_top5 | SUCCEEDED  | 1375 | 249 | 0.42 | 91 | 5 |
| loan_overdue | ovd_kpi_cards | SUCCEEDED  | 2319 | 316 | 0.87 | 164 | 3 |
| retail_deposit | dep_tickers | SUCCEEDED  | 1836 | 265 | 0.95 | 111 | 1 |
| retail_deposit | dep_compare | SUCCEEDED  | 2279 | 329 | 1.15 | 171 | 3 |
| retail_deposit | dep_gauges | SUCCEEDED  | 1634 | 342 | 0.89 | 100 | 1 |
| retail_deposit | dep_daily30 | SUCCEEDED  | 1238 | 265 | 1.17 | 120 | 10 |
| retail_deposit | dep_monthly | SUCCEEDED  | 1749 | 254 | 1.24 | 123 | 39 |
| retail_deposit | dep_region | SUCCEEDED  | 1185 | 231 | 0.84 | 91 | 4 |
| retail_deposit | dep_term | SUCCEEDED  | 1277 | 247 | 1.02 | 102 | 4 |
| retail_deposit | dep_casa_customer | SUCCEEDED  | 1498 | 252 | 0.88 | 93 | 3 |
| retail_deposit | dep_edusave | SUCCEEDED  | 1632 | 267 | 0.92 | 91 | 12 |
| retail_deposit | dep_by_product | SUCCEEDED  | 1487 | 258 | 0.87 | 94 | 10 |
| retail_deposit | dep_kpi_cards | SUCCEEDED  | 3129 | 899 | 1.28 | 186 | 3 |

**Total:** 34 queries, 1st run 60,810 ms, warm 10,303 ms, read 41.20 MB


## Reading these numbers

- Data: demo dataset on Free Edition (21 published days, `--scale 1`, ~13 M source rows); Gold
  marts are a few MB, so bytes read are similar before / after. The test measures query shape
  (planning + number of scans), not large-data scanning.
- *1st run* includes per-query compilation and cold caches; the very first statement of the
  "before" run (`card_kpi_compare`, 56.8 s) also absorbed warehouse warm-up, which inflates the
  "before" 1st-run total. Warm runs are the fairer comparison and are roughly equal
  (8.7 s / 31 queries before vs 10.3 s / 34 queries after).
- "After" includes 3 helper datasets for Vega-Lite KPI cards (`*_kpi_cards`) that no native
  widget uses yet, and returns more columns / rows (actual comparison dates, N/A handling,
  debt-group breakdown) - functionality the "before" version did not have.
- Excluding warm-up, the clearest gains are on queries that used to aggregate the whole
  history to find dates: e.g. `dep_by_product` 10.3 s → 1.5 s 1st run, `dep_casa_customer`
  4.3 s → 1.5 s; most others are within noise at this data size.
- Not measured: dashboard page load in the browser, concurrent viewers, larger data volumes.
  Re-run `tools/benchmark_dashboards.py` after the full backfill for production-like sizes.
