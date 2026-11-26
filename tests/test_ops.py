"""Batch control, gate và structured logging (không cần Spark)."""

import json

import pytest

from common.batch import (
    REQUIRED_FILE_GROUPS,
    incomplete_tables,
    manifest_status,
    missing_file_groups,
)
from common.names import fq
from common.ops_log import StepRecorder, log
from common.silver import ORDER
from pipelines.gold import NotReproducible, assert_reproducible, gate_errors

OK = {"silver_balance": 10.0, "gold_balance": 10.0, "recon_card_bad": 0, "recon_deposit_bad": 0}


def test_fq_local_and_unity_catalog():
    assert fq("", "ops", "pipeline_runs") == "ops.pipeline_runs"
    assert fq("bank_prod", "ops", "pipeline_runs") == "bank_prod.ops.pipeline_runs"
    assert fq("bank_prod", "gold") == "bank_prod.gold"


KPI = [f"/landing/card_kpi/{g}_20261101.txt" for g in REQUIRED_FILE_GROUPS["card_kpi_source"]]


@pytest.mark.parametrize("table,files,expected", [
    ("core_transactions", [], "MISSING"),
    ("ref_branches", [], "NOT_SENT"),
    ("loan_snapshots", ["a.csv"], "ARRIVED"),
    ("loan_snapshots", ["a.csv", "a.csv.resent"], "MULTIPLE_FILES"),
    ("card_kpi_source", KPI, "ARRIVED"),                 # 9 nhóm KPI = 9 file / ngày
    ("card_kpi_source", KPI + KPI, "MULTIPLE_FILES"),     # gửi lại đủ bộ
    ("card_kpi_source", KPI[:5], "INCOMPLETE"),          # mới nhận 5/9 file
    ("card_kpi_source", KPI[:8] + [KPI[0]], "INCOMPLETE"),  # đủ 9 file nhưng thiếu nhóm 09
])
def test_manifest_status(table, files, expected):
    assert manifest_status(table, files, ORDER) == expected


def test_missing_file_groups_and_incomplete_tables():
    assert missing_file_groups("card_kpi_source", KPI) == []
    assert missing_file_groups("card_kpi_source", KPI[:7]) == ["08_spend_channel",
                                                               "09_risk_dpd"]
    assert missing_file_groups("loan_snapshots", ["a.csv"]) == []
    assert incomplete_tables({"a": "ARRIVED", "c": "INCOMPLETE", "b": "INCOMPLETE"}) == ["b", "c"]


def test_gate_passes_only_when_everything_reconciles():
    assert gate_errors(OK) == []
    assert len(gate_errors({**OK, "gold_balance": 9.0})) == 1
    assert len(gate_errors({**OK, "recon_card_bad": 3, "recon_deposit_bad": 1})) == 2


def test_gold_refuses_day_older_than_silver():
    assert_reproducible("2026-11-02", "2026-11-02")
    assert_reproducible("2026-11-03", "2026-11-02")
    assert_reproducible("2026-11-01", None)
    with pytest.raises(NotReproducible):
        assert_reproducible("2026-11-01", "2026-11-02")


def test_log_is_one_json_line(capsys):
    log("x", run_id="r1", rows=3)
    rec = json.loads(capsys.readouterr().out.strip())
    assert rec["event"] == "x" and rec["run_id"] == "r1" and rec["rows"] == 3 and "ts" in rec


def test_step_recorder_keeps_failed_step():
    rec = StepRecorder("r1", "2026-11-01", "silver")
    with rec.step("ok") as s:
        s.rows_in, s.rows_out = 5, 4
    with pytest.raises(ValueError), rec.step("boom"):
        raise ValueError("bad file")
    rows = rec.rows()
    assert [(r[3], r[4]) for r in rows] == [("ok", "SUCCESS"), ("boom", "FAILED")]
    assert rows[0][5:7] == (5, 4)
    assert "ValueError: bad file" in rows[1][8]


def test_expected_card_kpi_files_match_generator():
    from common.batch import EXPECTED_FILES
    from data_generator.summaries import KPI_FILES

    assert EXPECTED_FILES["card_kpi_source"] == len(KPI_FILES)
    assert list(REQUIRED_FILE_GROUPS["card_kpi_source"]) == KPI_FILES


def test_rollback_restores_written_tables_and_drops_new_ones():
    from common.batch import rollback_statements

    assert rollback_statements([
        ("gold.dim_date", 4, 5),          # đã ghi trong publish lỗi -> RESTORE
        ("gold.loan_fact_daily", 7, 7),   # chưa tới lượt ghi -> giữ nguyên
        ("gold.dep_mart_trend", None, 0),  # tạo mới trong publish lỗi -> DROP
        ("gold.card_mart_spend_mix", None, None),  # chưa từng tồn tại -> bỏ qua
    ]) == ["RESTORE TABLE gold.dim_date TO VERSION AS OF 4",
           "DROP TABLE IF EXISTS gold.dep_mart_trend"]
