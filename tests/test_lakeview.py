"""Dashboard AI/BI: cấu trúc JSON hợp lệ, không chồng ô, mọi dataset chạy được trên Spark
và mọi cột widget dùng đều có trong kết quả dataset."""

import re

import pytest
from pyspark.sql import functions as F

from common import gold as g
from common import gold_cards as gc
from common import gold_deposit as gd
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
    m = {
        "gold_dim_branch": g.build_dim_branch(r["ref_branches"]),
        "gold_loan_mart_overdue_daily": g.build_mart_loan_overdue_daily(fact_loan, dim_product),
        "gold_card_mart_overdue_daily": gc.build_mart_card_overdue_daily(
            r["card_overdue_source"]),
        "gold_card_mart_kpi_daily": gc.build_mart_card_kpi_daily(
            r["card_master"], r["card_transactions_hist"], r["card_statements_hist"], AS_OF),
        "gold_dep_mart_trend": gd.build_mart_deposit_trend(
            r["core_accounts"], dim_product, r["ref_fx_rates"], r["ref_branches"], AS_OF),
        "gold_dep_mart_by_tenor": gd.build_mart_deposit_by_tenor(
            r["core_accounts"], dim_product, r["ref_fx_rates"], AS_OF),
        "gold_dep_mart_casa_customer": gd.build_mart_casa_customer(
            r["core_accounts"], dim_product, dim_customer, AS_OF),
        "gold_dep_mart_new_accounts": gd.build_mart_deposit_new_accounts(r["core_accounts"]),
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
    for name, df in m.items():
        df.createOrReplaceTempView(name)
    return m


def _local_sql(ds: dict, params: dict) -> str:
    sql = "".join(ds["queryLines"])
    sql = re.sub(r"\bgold\.", "gold_", sql)
    sql = re.sub(r"\bsilver\.", "silver_", sql)
    sql = re.sub(r"\bops\.", "ops_", sql)
    for k, v in params.items():
        sql = re.sub(r":" + k + r"\b", v, sql)
    return sql


PARAMS = [{"report_date": "DATE'2026-11-01'", "customer_type": "'consumer'", "branch": "'all'",
           "region": "'all'"},
          {"report_date": "DATE'2026-12-31'", "customer_type": "'all'", "branch": "'101'",
           "region": "'North'"}]
ALL_DS = [(f, ds) for f, body in DASHES.items() for ds in body["datasets"]]


@pytest.mark.parametrize("params", PARAMS, ids=["default", "filtered"])
@pytest.mark.parametrize("item", ALL_DS, ids=[ds["name"] for _, ds in ALL_DS])
def test_dataset_runs(spark, tables, item, params):
    _, ds = item
    spark.sql(_local_sql(ds, params)).collect()


def test_core_datasets_return_rows(spark, tables):
    for _, ds in ALL_DS:
        if ds["name"] in ("ovd_last5", "card_kpi", "ovd_tickers", "dep_tickers",
                          "dep_by_product", "card_mix_sector", "ovd_debt_groups"):
            rows = spark.sql(_local_sql(ds, PARAMS[0])).collect()
            assert rows, ds["name"]


def test_widget_fields_exist_in_dataset(spark, tables):
    for _, body in DASHES.items():
        cols = {ds["name"]: set(spark.sql(_local_sql(ds, PARAMS[0])).columns)
                for ds in body["datasets"]}
        for item in body["pages"][0]["layout"]:
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
        cells = set()
        for item in body["pages"][0]["layout"]:
            p = item["position"]
            assert p["x"] >= 0 and p["x"] + p["width"] <= 6, name
            for x in range(p["x"], p["x"] + p["width"]):
                for y in range(p["y"], p["y"] + p["height"]):
                    assert (x, y) not in cells, (name, item["widget"]["name"], x, y)
                    cells.add((x, y))


def test_widget_names_unique_and_specs_present():
    for body in DASHES.values():
        names = [i["widget"]["name"] for i in body["pages"][0]["layout"]]
        assert len(names) == len(set(names))
        for i in body["pages"][0]["layout"]:
            w = i["widget"]
            assert "spec" in w or "multilineTextboxSpec" in w


def test_section_order_matches_original():
    """Thứ tự tiêu đề widget từ trên xuống phải theo đúng dashboard gốc."""
    def titles(name):
        items = sorted(DASHES[name]["pages"][0]["layout"],
                       key=lambda i: (i["position"]["y"], i["position"]["x"]))
        return [i["widget"]["spec"]["frame"]["title"] for i in items
                if "spec" in i["widget"] and not i["widget"]["spec"]["widgetType"]
                .startswith("filter")]

    card = titles("card_business.lvdash.json")
    assert card[:5] == ["New Acquisition", "Customer Base", "Using Customer",
                        "Sales Volume (K USD)", "Balance (K USD)"]
    order = ["New Acquisition", "In & Out", "Customer Base & Using Mem", "Balance (K USD)",
             "Sales Volume (K USD)", "Spend per cus (USD)", "By Sectors", "By Locations",
             "By Channels", "DPD 1+ (%)", "DPD 10+ (%)", "DPD 30+ (%)", "NPL (%)"]
    assert [t for t in card[6:] if t in order] == order
    ovd = titles("loan_overdue.lvdash.json")
    assert ovd == ["FX (VND/USD)", "Overdue Ratio", "Overdue Balance (USD)", "Ending Balance (USD)",
                   "Overdue Ratio vs Last Day / Month / Year",
                   "Overdue Balance (USD) vs Last Day / Month / Year",
                   "Ending Balance (USD) vs Last Day / Month / Year",
                   "Overdue Trend (Last 5 Days)", "Overdue Trend (Monthly)",
                   "Overdue Amount by Debt Groups", "Overdue Amount by Product Groups",
                   "Overdue Ratio by Product Groups"]
    dep = titles("retail_deposit.lvdash.json")
    assert dep == ["Ending Balance", "Monthly Avg Balance", "Retail Customers",
                   "Ending Balance vs Last Day / Month / Year", "All Products Target (%)",
                   "CASA Target (%)", "TDA/ISA Target (%)", "Ending Balance - Last 30 Days",
                   "Monthly Average Balance (MoM)", "Regional Deposit Distribution",
                   "Deposit by Term and IR (in Month)", "CASA Customer Distribution",
                   "Edu-Save New open Account_ Accumulate by Month", "Balance by Product"]


def test_bundle_files_up_to_date():
    """dashboards/*.lvdash.json phải khớp với generator (chạy lại: python -m common.lakeview)."""
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "dashboards"
    for name, body in build_all("").items():
        on_disk = json.loads((root / name).read_text(encoding="utf-8"))
        assert on_disk == body, f"{name} cũ - chạy: python src/common/lakeview.py dashboards"


def test_card_colors_from_original_palette():
    """Màu theo card_dashboard_final.html (Dashboard_CARD_v4): xanh navy / xanh nhạt / vàng."""
    body = DASHES["card_business.lvdash.json"]
    maps = {}
    for i in body["pages"][0]["layout"]:
        spec = i["widget"].get("spec", {})
        for m in spec.get("encodings", {}).get("color", {}).get("scale", {}).get("mappings", []):
            maps[(spec["frame"]["title"], m["value"])] = m["color"]
    assert maps[("New Acquisition", "Smart Credit")] == "#4472C4"
    assert maps[("New Acquisition", "Branches")] == "#B4C7E7"
    assert maps[("In & Out", "New Issuance")] == "#2F5597"
    assert maps[("Balance (K USD)", "Revolving")] == "#BF9000"
    assert maps[("By Sectors", "Purchasing")] == CARD_COLORS["sector"]["Purchasing"] == "#2F5597"
