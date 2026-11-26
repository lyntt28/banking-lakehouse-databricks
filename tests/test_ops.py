"""Batch control, gate và structured logging (không cần Spark)."""

import json

import pytest

from common.batch import manifest_status
from common.names import fq
from common.ops_log import StepRecorder, log
from common.silver import ORDER
from pipelines.gold import NotReproducible, assert_reproducible, gate_errors

OK = {"silver_balance": 10.0, "gold_balance": 10.0, "recon_card_bad": 0, "recon_deposit_bad": 0}


def test_fq_local_and_unity_catalog():
    assert fq("", "ops", "pipeline_runs") == "ops.pipeline_runs"
    assert fq("bank_prod", "ops", "pipeline_runs") == "bank_prod.ops.pipeline_runs"
    assert fq("bank_prod", "gold") == "bank_prod.gold"


@pytest.mark.parametrize("table,files,expected", [
    ("core_transactions", 0, "MISSING"),
    ("ref_branches", 0, "NOT_SENT"),
    ("loan_snapshots", 1, "ARRIVED"),
    ("loan_snapshots", 2, "MULTIPLE_FILES"),
    ("card_kpi_source", 9, "ARRIVED"),          # 9 nhóm KPI = 9 file / ngày theo thiết kế
    ("card_kpi_source", 18, "MULTIPLE_FILES"),
])
def test_manifest_status(table, files, expected):
    assert manifest_status(table, files, ORDER) == expected


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
