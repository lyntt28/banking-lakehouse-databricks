"""Benchmark dataset của dashboard AI/BI trên SQL Warehouse thật (Statement Execution API).

Chạy MỌI dataset của 1 hoặc nhiều file .lvdash.json với cùng tham số, mỗi dataset `--repeat`
lần (lần 1 ~ cold hơn, các lần sau ~ warm), rồi lấy số liệu từ Query History:
thời gian thực thi, bytes / files đọc, số dòng. So sánh 2 phiên bản dashboard (vd trước / sau
refactor) trên CÙNG dữ liệu:

    DATABRICKS_HOST=https://<workspace> DATABRICKS_TOKEN=<pat> \\
    python tools/benchmark_dashboards.py --warehouse <id> --catalog bank_dev \\
        --report-date 2026-11-28 --label new dashboards/*.lvdash.json \\
        --label old /tmp/old/*.lvdash.json --out docs/benchmark.md

Chỉ ghi số đo được. Mỗi câu lệnh thêm 1 comment ngẫu nhiên để không trúng result cache của
warehouse (cache dữ liệu / disk cache vẫn có tác dụng -> lần sau thường nhanh hơn lần đầu).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import time
import urllib.request
import uuid
from pathlib import Path

DEFAULT_PARAMS = {"branch": "all", "customer_type": "consumer", "region": "all",
                  "product": "all", "currency": "all", "nationality": "all"}


def api(method: str, path: str, body: dict | None = None) -> dict:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    req = urllib.request.Request(
        host + path, method=method, data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {os.environ.get('DATABRICKS_TOKEN', '')}",
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read() or b"{}")


def run_statement(warehouse: str, catalog: str, sql: str, params: dict[str, str]) -> dict:
    used = set(re.findall(r":([A-Za-z_]\w*)", sql))
    plist = [{"name": k, "value": v, "type": "DATE" if k == "report_date" else "STRING"}
             for k, v in params.items() if k in used]
    body = {"warehouse_id": warehouse, "catalog": catalog, "wait_timeout": "50s",
            "statement": f"/* bench {uuid.uuid4().hex} */\n{sql}", "parameters": plist,
            "disposition": "INLINE", "format": "JSON_ARRAY"}
    t0 = time.time()
    r = api("POST", "/api/2.0/sql/statements", body)
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(1)
        r = api("GET", f"/api/2.0/sql/statements/{r['statement_id']}")
    r["_client_s"] = time.time() - t0
    return r


def query_metrics(statement_ids: list[str]) -> dict[str, dict]:
    """Query History: total_time_ms, read_bytes, read_files_count, rows_produced_count."""
    out: dict[str, dict] = {}
    body = {"filter_by": {"statement_ids": statement_ids}, "include_metrics": True,
            "max_results": len(statement_ids)}
    for _ in range(10):   # history có độ trễ vài giây
        res = api("GET", "/api/2.0/sql/history/queries", body)
        for q in res.get("res", []):
            out[q["query_id"]] = q.get("metrics", {}) | {"duration": q.get("duration")}
        if len(out) >= len(statement_ids):
            break
        time.sleep(3)
    return out


def bench(files: list[Path], warehouse: str, catalog: str, params: dict[str, str],
          repeat: int) -> list[dict]:
    rows = []
    for f in files:
        body = json.loads(f.read_text(encoding="utf-8"))
        for ds in body["datasets"]:
            sql = "".join(ds["queryLines"])
            if not ds.get("parameters") and "options" in ds["name"]:
                continue          # danh sách bộ lọc: không phải truy vấn của visual
            runs = [run_statement(warehouse, catalog, sql, params) for _ in range(repeat)]
            rows.append({"dashboard": f.stem.replace(".lvdash", ""), "dataset": ds["name"],
                         "runs": runs})
    ids = [r["statement_id"] for row in rows for r in row["runs"]]
    metrics = {}
    for i in range(0, len(ids), 50):
        metrics |= query_metrics(ids[i:i + 50])
    for row in rows:
        row["metrics"] = [metrics.get(r["statement_id"], {}) for r in row["runs"]]
        row["state"] = row["runs"][-1]["status"]["state"]
        row["error"] = (row["runs"][-1]["status"].get("error") or {}).get("message", "")[:120]
    return rows


def summarize(label: str, rows: list[dict]) -> list[str]:
    def ms(m: dict) -> float | None:
        return m.get("total_time_ms") or m.get("execution_time_ms") or m.get("duration")

    lines = [f"### {label}", "",
             "| Dashboard | Dataset | State | 1st run (ms) | Median next runs (ms) | "
             "Read (MB) | Files read | Rows |", "|---|---|---|---:|---:|---:|---:|---:|"]
    tot_first = tot_warm = tot_bytes = 0.0
    for r in rows:
        t = [ms(m) for m in r["metrics"]]
        first = t[0]
        warm = statistics.median([x for x in t[1:] if x is not None]) if len(t) > 1 and any(
            x is not None for x in t[1:]) else None
        m0 = r["metrics"][0]
        mb = (m0.get("read_bytes") or 0) / 1e6
        tot_first += first or 0
        tot_warm += warm or 0
        tot_bytes += mb
        lines.append(f"| {r['dashboard']} | {r['dataset']} | {r['state']} "
                     f"{('- ' + r['error']) if r['error'] else ''} | {first or '-'} | "
                     f"{warm if warm is not None else '-'} | {mb:.2f} | "
                     f"{m0.get('read_files_count', '-')} | {m0.get('rows_produced_count', '-')} |")
    lines += ["", f"**Total:** {len(rows)} queries, 1st run {tot_first:,.0f} ms, "
                  f"warm {tot_warm:,.0f} ms, read {tot_bytes:,.2f} MB", ""]
    return lines


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--warehouse", required=True)
    p.add_argument("--catalog", default="bank_dev")
    p.add_argument("--report-date", required=True)
    p.add_argument("--repeat", type=int, default=3)
    p.add_argument("--label", nargs="+", action="append", required=True,
                   help="--label <tên> <file.lvdash.json>... (lặp lại để so sánh)")
    p.add_argument("--out")
    a = p.parse_args()
    params = {"report_date": a.report_date, **DEFAULT_PARAMS}
    out = ["# Dashboard query benchmark", "",
           f"Warehouse `{a.warehouse}`, catalog `{a.catalog}`, report_date {a.report_date}, "
           f"{a.repeat} runs per dataset, params {DEFAULT_PARAMS}. Measured "
           f"{time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}.", ""]
    for label, *files in a.label:
        rows = bench([Path(f) for f in files], a.warehouse, a.catalog, params, a.repeat)
        out += summarize(label, rows)
    text = "\n".join(out)
    print(text)
    if a.out:
        Path(a.out).write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
