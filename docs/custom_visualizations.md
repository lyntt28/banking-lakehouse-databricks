# Custom visualizations (Vega-Lite) for the 3 dashboards

Goal: bring the AI/BI (Lakeview) dashboards close to the original HTML / Chart.js design
without Databricks Apps or extra infrastructure. Business logic and Gold marts are unchanged.

## What Databricks documents (checked 2026-11-28)

Source: https://docs.databricks.com/aws/en/dashboards/manage/visualizations/custom-visualizations
and https://docs.databricks.com/aws/en/dashboards/manage/visualizations/types

- Custom visualizations are **Public Preview**. They are created in the dashboard editor:
  select a dataset → **Custom Viz** (Advanced) → declare **Fields** (each has a Name) → paste a
  Vega-Lite JSON spec that references fields by Name.
- The dashboard theme (fonts, grid lines, transparent background, light/dark) is inherited.
- Cross-filter / drill-through work by default for single-view specs. Layered specs (most of
  ours) do not cross-filter unless they declare `databricks_mark_selection`.
- **Treemaps are not supported** → the Overdue branch treemap stays a ranked bar chart (same
  rules: exclude 8125, > 10,000 USD, Top 5 by ratio).
- Native options we keep: counter (value + comparison + conditional formatting), combo with
  **Enable dual axis**, bar Stack / Group, pie, multiple pages.
- **Not documented:** how a Custom Viz widget is serialized in `.lvdash.json`. The generator
  therefore does **not** write Custom Viz widgets into the dashboard files. Specs are generated
  to `dashboards/custom_viz/` and pasted in the UI.

## Generated files

`PYTHONPATH=src python -m common.lakeview` writes:

- `dashboards/*.lvdash.json` — native dashboards (Deposit now has 2 pages, like the original
  tabs) plus helper datasets `card_kpi_cards`, `dep_kpi_cards`, `ovd_kpi_cards` (one row per
  KPI: value + up to 3 comparisons + 2 notes, built from the existing KPI / comparison datasets).
- `dashboards/custom_viz/<dashboard>/<widget>.json` — Vega-Lite specs.
- `dashboards/custom_viz/README.md` — for every spec: page, native widget it replaces, dataset,
  Fields to declare (Name = column name).

| Visual | Spec | Dataset |
|---|---|---|
| KPI card with value + green/red comparisons + target notes | `kpi_*.json` | `*_kpi_cards` |
| Semicircle gauge (Deposit target achievement) | `gauge_all/casa/tda.json` | `dep_gauges` |
| Grouped + stacked bar, this year vs last year | `retail_deposit/monthly_years.json` | `dep_monthly` |
| Dual-axis combo with value labels | `ovd_daily_trend`, `ovd_monthly_trend`, `term_combo` | same names |
| Stacked bar + dashed Total line | `card_business/new_acquisition.json` | `card_new_acq` |
| Donut with % in the centre | `card_business/donut_*.json` | `card_mix_*` |
| Comparison table, green/red (red = risk up) | `card_business/risk_compare.json` | `card_risk_compare` |

Colors come from the original HTML (`CARD_COLORS`, `DEP_COLORS`, `OVD_COLORS`).

## Applying (one dashboard at a time)

1. Deploy the bundle as usual (native dashboards + datasets).
2. Open the dashboard in Draft, select the native widget listed under *Replaces*, change its
   visualization to **Custom Viz**, keep the dataset, add each listed Field with Name = column,
   paste the spec. Keep size / position.
3. Publish. Re-run `python -m common.lakeview` only regenerates specs; the bundle deploy
   overwrites UI changes, so after the first widget is done, export the dashboard and share the
   file: with the real serialized format the generator can emit Custom Viz widgets directly and
   the manual step disappears.

## Validation done locally
- Every spec compiles with Vega-Lite 5.21 (`node compile_all.js dashboards/custom_viz`).
- Prototype renders (sample data) checked visually: KPI card, gauge, comparison table,
  dual-axis combo, stacked + total, donut, grouped stacked years.
- `tests/test_lakeview.py::test_custom_viz_fields_exist_in_datasets` — every field a spec reads
  exists in its dataset; KPI-card datasets return one row per KPI.
- Not verified: rendering inside a Databricks workspace (Preview feature, theme signals).
