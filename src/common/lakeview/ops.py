"""Dashboard Operations Monitor (cho DE): chỉ sửa file này khi đổi dashboard này."""

from __future__ import annotations

from common.lakeview.core import Dash, _q


# ============================================================ 4. OPERATIONS MONITOR (cho DE)
def ops_dashboard(catalog: str) -> dict:
    """Dashboard vận hành: batch nào lỗi, ngày nào, bước nào, file nào chưa về, DQ, đối soát,
    Gold đang phục vụ batch nào. Đọc các bảng ops.* do pipelines/ ghi."""
    OPS = _q(catalog, "ops")
    d = Dash("ops", "Lakehouse Operations Monitor")
    ds_kpi = d.dataset("ops_kpi", "Current state", f"""
WITH pub AS (SELECT max(as_of_date) AS d FROM {OPS}.published_batches),
last_run AS (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY as_of_date, layer
                                        ORDER BY started_at DESC, status DESC) AS rn
                             FROM {OPS}.pipeline_runs) WHERE rn = 1)
SELECT pub.d AS serving_batch,
       datediff(current_date(), pub.d) AS serving_age_days,
       (SELECT count(*) FROM last_run WHERE status = 'FAILED') AS open_failures,
       (SELECT round(avg(CASE WHEN status = 'SUCCESS' THEN 100.0 ELSE 0 END), 1)
        FROM {OPS}.pipeline_runs WHERE status <> 'RUNNING'
          AND started_at >= current_timestamp() - INTERVAL 30 DAYS) AS success_rate_30d
FROM pub
""", [])
    ds_status = d.dataset("ops_status", "Batch status (last 14 days)", f"""
SELECT as_of_date, layer, status, run_id, started_at, finished_at, message
FROM (SELECT *, row_number() OVER (PARTITION BY as_of_date, layer
                                   ORDER BY started_at DESC, status DESC) AS rn
      FROM {OPS}.pipeline_runs WHERE as_of_date >= date_sub(current_date(), 14))
WHERE rn = 1
ORDER BY as_of_date DESC, layer
""", [])
    ds_steps = d.dataset("ops_steps", "Failed steps", f"""
SELECT logged_at, as_of_date, layer, step, rows_in, rows_out, duration_s, error, run_id
FROM {OPS}.task_runs WHERE status = 'FAILED' ORDER BY logged_at DESC LIMIT 50
""", [])
    ds_dur = d.dataset("ops_duration", "Processing time per day (minutes)", f"""
SELECT date_format(as_of_date, 'yyyy-MM-dd') AS day, layer, sum(duration_s) / 60 AS minutes
FROM {OPS}.task_runs WHERE status = 'SUCCESS' AND as_of_date >= date_sub(current_date(), 30)
GROUP BY 1, 2
""", [])
    ds_files = d.dataset("ops_files", "Source files not normal", f"""
SELECT as_of_date, table_name, status, file_count, rows, first_loaded_at
FROM {OPS}.file_manifest
WHERE status IN ('MISSING', 'INCOMPLETE', 'MULTIPLE_FILES')
  AND as_of_date >= date_sub(current_date(), 14)
ORDER BY as_of_date DESC, table_name
""", [])
    ds_dq = d.dataset("ops_dq", "DQ failed rows per day", f"""
SELECT date_format(run_ts, 'yyyy-MM-dd') AS day, severity, sum(failed_rows) AS failed_rows
FROM {OPS}.dq_results WHERE run_ts >= current_timestamp() - INTERVAL 30 DAYS AND failed_rows > 0
GROUP BY 1, 2
""", [])
    ds_recon = d.dataset("ops_recon", "Reconciliation issues", f"""
SELECT as_of_date, 'card_kpi' AS check, metric_code AS item, status, diff
FROM {OPS}.recon_card_kpi WHERE status NOT IN ('MATCH', 'WITHIN_TOLERANCE')
UNION ALL
SELECT as_of_date, 'deposit', concat_ws('/', account_group, currencycd, branchnbr), status, diff
FROM {OPS}.recon_deposit WHERE status <> 'MATCH'
ORDER BY as_of_date DESC LIMIT 100
""", [])

    d.text("# LAKEHOUSE OPERATIONS MONITOR\nBatch status, failures, source files, DQ and "
           "reconciliation. Gold only serves batches that passed the reconciliation gate.",
           0, 0, 6, 2)
    d.counter(ds_kpi, "serving_batch", "Gold serving batch", 0, 2, w=2)
    d.counter(ds_kpi, "serving_age_days", "Serving age (days)", 2, 2)
    d.counter(ds_kpi, "open_failures", "Open failures", 3, 2, color="#D62728")
    d.counter(ds_kpi, "success_rate_30d", "Success rate 30d (%)", 4, 2, w=2)
    d.table(ds_status, "Batch status (last 14 days)", [
        ("as_of_date", "Data date", "text"), ("layer", "Layer", "text"),
        ("status", "Status", "text"), ("message", "Message", "text"),
        ("run_id", "Run ID", "text")], 0, 5, 6, 6)
    d.table(ds_steps, "Failed steps (latest 50)", [
        ("as_of_date", "Data date", "text"), ("layer", "Layer", "text"),
        ("step", "Step", "text"), ("error", "Error", "text"),
        ("duration_s", "Duration (s)", "num"), ("run_id", "Run ID", "text")], 0, 11, 6, 6)
    d.chart("bar", ds_dur, "Processing time per day (minutes)", 0, 17, 3, 6, cat="day",
            series="layer", val="minutes", layout="stack", val_title="minutes")
    d.chart("bar", ds_dq, "DQ failed rows per day", 3, 17, 3, 6, cat="day",
            series="severity", val="failed_rows", layout="stack", val_title="rows")
    d.table(ds_files, "Source files missing / sent more than once (14 days)", [
        ("as_of_date", "Data date", "text"), ("table_name", "Table", "text"),
        ("status", "Status", "text"), ("file_count", "Files", "int"),
        ("rows", "Rows", "int")], 0, 23, 3, 6)
    d.table(ds_recon, "Reconciliation issues", [
        ("as_of_date", "Data date", "text"), ("check", "Check", "text"),
        ("item", "Item", "text"), ("status", "Status", "text"), ("diff", "Diff", "num")],
        3, 23, 3, 6)
    return d.to_json()
