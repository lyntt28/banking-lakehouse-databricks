"""Dashboard AI/BI: cấu trúc JSON hợp lệ, không chồng ô, mọi dataset chạy được trên Spark
và mọi cột widget dùng đều có trong kết quả dataset."""

import re

import pytest
from dashboard_sql import localize
from pyspark.sql import functions as F

from common import gold as g
from common import gold_cards as gc
from common import gold_deposit as gd
from common import serving as sv
from common.lakeview import CARD_COLORS, build_all

AS_OF = "2026-11-01"
DASHES = build_all("")


@pytest.fixture(scope="module")
def tables(spark, results):
    r = {t: x.valid for t, x in results.items()}
    cust = r["core_customers"].withColumn("valid_from", F.lit(AS_OF).cast("date")) \
        .withColumn("valid_to", F.lit(None).cast("date")).withColumn("is_current", F.lit(True))
    dim_customer = g.build_dim_customer(cust)
    dim_product = g.build_dim_product(r["ref_products"])
    fact_loan = g.build_fact_loan_daily(r["loan_snapshots"], dim_customer)
    card_daily = gc.build_mart_card_kpi_daily(
        r["card_master"], r["card_transactions_hist"], r["card_statements_hist"], AS_OF)
    trend = gd.build_mart_deposit_trend(
        r["core_accounts"], dim_product, r["ref_fx_rates"], r["ref_branches"], AS_OF,
        customers=dim_customer)
    m = {
        "gold_dim_branch": g.build_dim_branch(r["ref_branches"]),
        "gold_dim_product": dim_product,
        "gold_loan_mart_overdue_daily": g.build_mart_loan_overdue_daily(fact_loan, dim_product),
        "gold_card_mart_overdue_daily": gc.build_mart_card_overdue_daily(
            r["card_overdue_source"]),
        "gold_card_mart_kpi_daily": card_daily,
        "gold_card_mart_kpi_monthly": gc.build_mart_card_kpi_monthly(
            card_daily, AS_OF, using_mtd=gc.using_members_by_month(
                r["card_transactions_hist"], r["card_master"])),
        "gold_dep_mart_trend": trend,
        "gold_dep_mart_monthly": gd.build_mart_deposit_monthly(trend, AS_OF),
        "gold_dep_mart_by_tenor": gd.build_mart_deposit_by_tenor(
            r["core_accounts"], dim_product, r["ref_fx_rates"], AS_OF, r["ref_branches"]),
        "gold_dep_mart_casa_customer": gd.build_mart_casa_customer(
            r["core_accounts"], dim_product, dim_customer, AS_OF, r["ref_branches"]),
        "gold_dep_mart_new_accounts": gd.build_mart_deposit_new_accounts(
            r["core_accounts"], AS_OF, r["ref_branches"])
        .withColumn("as_of_date", F.lit(AS_OF).cast("date")),
        "silver_ref_targets": r["ref_targets"], "silver_ref_fx_rates": r["ref_fx_rates"],
    }
    ops = {
        "pipeline_runs": ("as_of_date date, layer string, run_id string, status string, "
                          "started_at timestamp, finished_at timestamp, message string"),
        "published_batches": ("as_of_date date, run_id string, published_at timestamp, "
                              "checks string"),
        "task_runs": ("run_id string, as_of_date date, layer string, step string, status string, "
                      "rows_in long, rows_out long, duration_s double, error string, "
                      "logged_at timestamp"),
        "file_manifest": ("as_of_date date, table_name string, required boolean, status string, "
                          "file_count long, rows long, source_files array<string>, "
                          "first_loaded_at timestamp, checked_at timestamp"),
        "dq_results": ("run_id string, run_ts timestamp, table_name string, rule_name string, "
                       "severity string, total_rows long, failed_rows long, fail_pct double, "
                       "status string"),
        "recon_card_kpi": ("as_of_date date, metric_code string, gold_value double, "
                           "source_value double, diff double, diff_pct double, status string"),
        "recon_deposit": ("as_of_date date, account_group string, currencycd string, "
                          "branchnbr string, gold_mil_vnd double, source_mil_vnd double, "
                          "diff double, status string"),
    }
    for name, schema in ops.items():
        m[f"ops_{name}"] = spark.createDataFrame([], schema)
    # batch Gold đang phục vụ: dataset chỉ đọc tới ngày đã publish
    m["ops_published_batches"] = spark.sql(
        f"SELECT DATE'{AS_OF}' AS as_of_date, 'r1' AS run_id, "
        "current_timestamp() AS published_at, '{}' AS checks")
    for name, df in m.items():
        # chốt dữ liệu (cắt lineage): trên Databricks mart là bảng thật; ở test DataFrame lười
        # bị nhân lên mỗi lần CTE tham chiếu -> plan khổng lồ, hết heap
        df = df.localCheckpoint()
        m[name] = df
        df.createOrReplaceTempView(name)
    # lịch báo cáo Overdue: test không có batch -> mọi ngày của mart là ngày đã publish
    spark.sql(sv.report_calendar_body(
        "SELECT DISTINCT as_of_date FROM gold_card_mart_overdue_daily")).localCheckpoint() \
        .createOrReplaceTempView("gold_overdue_report_calendar")
    spark.sql(sv.report_calendar_body(
        "SELECT DISTINCT as_of_date FROM gold_dep_mart_trend")).localCheckpoint() \
        .createOrReplaceTempView("gold_deposit_report_calendar")
    spark.sql(sv.report_calendar_body(
        "SELECT DISTINCT as_of_date FROM gold_card_mart_kpi_daily")).localCheckpoint() \
        .createOrReplaceTempView("gold_card_report_calendar")
    return m


def _local_sql(ds: dict, params: dict) -> str:
    return localize("".join(ds["queryLines"]), params)


PARAMS = [{"report_date": "DATE'2026-11-01'", "customer_type": "'consumer'", "branch": "'all'",
           "region": "'all'", "product": "'all'", "currency": "'all'", "nationality": "'all'"},
          {"report_date": "DATE'2026-12-31'", "customer_type": "'all'", "branch": "'101'",
           "region": "'North'", "product": "'1-CASA'", "currency": "'VND'",
           "nationality": "'VIETNAMESE'"}]


def _items(body):
    """Mọi widget của mọi trang."""
    return [i for pg in body["pages"] for i in pg["layout"]]
ALL_DS = [(f, ds) for f, body in DASHES.items() for ds in body["datasets"]]


@pytest.mark.parametrize("params", PARAMS, ids=["default", "filtered"])
@pytest.mark.parametrize("item", ALL_DS, ids=[ds["name"] for _, ds in ALL_DS])
def test_dataset_runs(spark, tables, item, params):
    _, ds = item
    spark.sql(_local_sql(ds, params)).collect()


def test_core_datasets_return_rows(spark, tables):
    for _, ds in ALL_DS:
        if ds["name"] in ("ovd_daily_trend", "card_kpi", "ovd_kpi", "dep_tickers",
                          "dep_by_product", "card_mix_sector", "ovd_debt_groups"):
            rows = spark.sql(_local_sql(ds, PARAMS[0])).collect()
            assert rows, ds["name"]


def test_widget_fields_exist_in_dataset(spark, tables):
    for _, body in DASHES.items():
        cols = {ds["name"]: set(spark.sql(_local_sql(ds, PARAMS[0])).columns)
                for ds in body["datasets"]}
        for item in _items(body):
            for q in item["widget"].get("queries", []):
                query = q["query"]
                for f in query.get("fields", []):
                    for ref in re.findall(r"`([^`]+)`", f["expression"]):
                        assert ref in cols[query["datasetName"]], (query["datasetName"], ref)
                for p in query.get("parameters", []):
                    ds = next(d for d in body["datasets"] if d["name"] == query["datasetName"])
                    assert p["keyword"] in {x["keyword"] for x in ds["parameters"]}


def test_layout_fits_grid_without_overlap():
    for name, body in DASHES.items():
        for pg in body["pages"]:
            cells = set()
            for item in pg["layout"]:
                p = item["position"]
                assert p["x"] >= 0 and p["x"] + p["width"] <= 6, name
                for x in range(p["x"], p["x"] + p["width"]):
                    for y in range(p["y"], p["y"] + p["height"]):
                        assert (x, y) not in cells, (name, item["widget"]["name"], x, y)
                        cells.add((x, y))


def test_widget_names_unique_and_specs_present():
    for body in DASHES.values():
        names = [i["widget"]["name"] for i in _items(body)]
        assert len(names) == len(set(names))
        for i in _items(body):
            w = i["widget"]
            assert "spec" in w or "multilineTextboxSpec" in w


def test_section_order_matches_original():
    """Thứ tự tiêu đề widget từ trên xuống phải theo đúng dashboard gốc."""
    def titles(name, page=0):
        items = sorted(DASHES[name]["pages"][page]["layout"],
                       key=lambda i: (i["position"]["y"], i["position"]["x"]))
        return [i["widget"]["spec"]["frame"]["title"] for i in items
                if "spec" in i["widget"] and not i["widget"]["spec"]["widgetType"]
                .startswith("filter")]

    card = titles("card_business.lvdash.json")
    assert card[:5] == ["New Acquisition", "Customer Base", "Using Customer",
                        "Sales Volume (K USD)", "Balance (K USD)"]
    # Delinquency đưa lên ngay dưới Overview (theo yêu cầu)
    order = ["DPD 1+ (%)", "DPD 10+ (%)", "DPD 30+ (%)", "NPL (%)", "New Acquisition",
             "In & Out", "Customer Base & Using Mem", "Balance (K USD)",
             "Sales Volume (K USD)", "Spend per Member (USD)", "By Sectors", "By Locations",
             "By Channels"]
    assert [t for t in card[5:] if t in order] == order
    ovd = titles("loan_overdue.lvdash.json")
    assert ovd == ["FX (VND/USD)", "Overdue Ratio", "Overdue Balance (USD)",
                   "Ending Balance (USD)", "Overdue Ratio vs Last Day / Month / Year",
                   "Overdue Balance (USD) vs Last Day / Month / Year",
                   "Ending Balance (USD) vs Last Day / Month / Year",
                   "Overdue Trend (Last 5 Days)", "Overdue Trend (Monthly)",
                   "Overdue Amount by Debt Groups", "Overdue Amount by Product Groups",
                   "Overdue Ratio by Product Groups", "Top 5 Branches by Overdue Ratio",
                   "Top 5 Branches - Overdue Amount"]
    # 1 trang (2 tab của bản gốc gộp lại theo yêu cầu)
    assert len(DASHES["retail_deposit.lvdash.json"]["pages"]) == 1
    assert titles("retail_deposit.lvdash.json") == [
        "Ending Balance", "Monthly Avg Balance", "Retail Customers", "", "All Products Target (%)",
        "CASA Target (%)", "TDA/ISA Target (%)", "Ending Balance - Last 30 Days",
        "Monthly Ending Balance - Year Comparison", "Regional Deposit Distribution",
        "Deposit by Term and IR (in Month)", "CASA Customer Distribution",
        "Edu-Save New open Account_ Accumulate by Month", "Balance by Product (K USD)"]


def test_bundle_files_up_to_date():
    """dashboards/*.lvdash.json phải khớp với generator.
    Sinh lại: PYTHONPATH=src python -m common.lakeview dashboards"""
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "dashboards"
    for name, body in build_all("").items():
        on_disk = json.loads((root / name).read_text(encoding="utf-8"))
        assert on_disk == body, (f"{name} cũ - chạy: "
                                 "PYTHONPATH=src python -m common.lakeview dashboards")


def test_one_palette_across_dashboards():
    """Một bảng màu chung (core): mọi màu trong mọi dashboard thuộc bảng màu đã kiểm tra;
    cùng thực thể cùng màu (vd Smart Credit / CASA / Overdue amount = S1)."""
    from common.lakeview import core

    allowed = set(core.SERIES) | {core.NEUTRAL, core.TEXT, core.S1_LIGHT, core.S2_LIGHT,
                                  core.S3_LIGHT} | set(core.SEVERITY)
    for name, body in DASHES.items():
        if name == "ops_monitor.lvdash.json":
            continue
        colors = set()
        for i in _items(body):
            spec = i["widget"].get("spec", {})
            enc = spec.get("encodings", {})
            colors |= {m["color"] for m in enc.get("color", {}).get("scale", {})
                       .get("mappings", [])}
            colors |= set(spec.get("mark", {}).get("colors", []))
            if "style" in enc.get("value", {}):
                colors.add(enc["value"]["style"]["color"])
        assert colors <= allowed, (name, colors - allowed)
    body = DASHES["card_business.lvdash.json"]
    maps = {}
    for i in _items(body):
        spec = i["widget"].get("spec", {})
        for m in spec.get("encodings", {}).get("color", {}).get("scale", {}).get("mappings", []):
            maps[(spec["frame"]["title"], m["value"])] = m["color"]
    assert maps[("New Acquisition", "Smart Credit")] == core.S1
    assert maps[("Balance (K USD)", "Revolving")] == core.S2
    assert maps[("By Sectors", "Purchasing")] == CARD_COLORS["sector"]["Purchasing"] == core.S1


def test_filters_bound_on_every_page():
    """Nhiều trang: mỗi bộ lọc phải gắn với dataset của chính trang đó."""
    for name, body in DASHES.items():
        if len(body["pages"]) < 2:   # 1 trang: gắn mọi dataset có tham số (kể cả chưa dùng)
            continue
        for pg in body["pages"]:
            on_page = {q["query"]["datasetName"] for i in pg["layout"]
                       for q in i["widget"].get("queries", []) if q["name"] == "main_query"}
            for i in pg["layout"]:
                spec = i["widget"].get("spec", {})
                if spec.get("widgetType", "").startswith("filter"):
                    bound = {q["query"]["datasetName"] for q in i["widget"]["queries"]
                             if q["name"].startswith("param_")}
                    assert bound and bound <= on_page, (name, pg["name"], spec["frame"])


def test_custom_viz_fields_exist_in_datasets(spark, tables):
    """Spec Vega-Lite chỉ đọc cột có thật trong dataset tương ứng; thẻ KPI có đủ dòng."""
    from common.lakeview.custom_viz import catalog

    files = {"card_business": "card_business.lvdash.json",
             "retail_deposit": "retail_deposit.lvdash.json",
             "loan_overdue": "loan_overdue.lvdash.json"}
    for w in catalog():
        ds = next(d for d in DASHES[files[w.dashboard]]["datasets"] if d["name"] == w.dataset)
        cols = set(spark.sql(_local_sql(ds, PARAMS[0])).columns)
        assert set(w.fields) <= cols, (w.name, set(w.fields) - cols)
    for name, n in (("card_kpi_cards", 5), ("dep_kpi_cards", 3), ("ovd_kpi_cards", 3)):
        ds = next(d for b in DASHES.values() for d in b["datasets"] if d["name"] == name)
        rows = spark.sql(_local_sql(ds, PARAMS[0])).collect()
        assert len(rows) == n and all(r.title for r in rows), name


def test_custom_viz_specs_up_to_date(tmp_path):
    """dashboards/custom_viz phải khớp generator (PYTHONPATH=src python -m common.lakeview)."""
    from pathlib import Path

    from common.lakeview.custom_viz import write_specs

    root = Path(__file__).resolve().parents[1] / "dashboards" / "custom_viz"
    for p in write_specs(tmp_path):
        rel = p.relative_to(tmp_path)
        assert (root / rel).read_text(encoding="utf-8") == p.read_text(encoding="utf-8"), rel
