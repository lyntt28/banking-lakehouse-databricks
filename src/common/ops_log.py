"""Structured logging + đo từng bước (task) của pipeline.

- `log(event, **fields)`: in 1 dòng JSON (ts, event, run_id, as_of_date, ...). Trên Databricks
  dòng này nằm trong driver log / output của task -> lọc được theo run_id.
- `StepRecorder`: ghi mỗi bước (step) với status, số dòng vào/ra, thời gian, lỗi; `flush()`
  append vào `ops.task_runs` (kể cả khi bước lỗi) -> dashboard vận hành đọc bảng này.
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone


def log(event: str, **fields) -> dict:
    rec = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "event": event,
           **fields}
    print(json.dumps(rec, ensure_ascii=False, default=str))
    return rec


@dataclass
class StepResult:
    step: str
    status: str = "RUNNING"
    rows_in: int | None = None
    rows_out: int | None = None
    duration_s: float = 0.0
    error: str | None = None


@dataclass
class StepRecorder:
    run_id: str
    as_of_date: str
    layer: str
    steps: list[StepResult] = field(default_factory=list)

    @contextmanager
    def step(self, name: str):
        """with rec.step("dq:core_accounts") as s: ...; s.rows_in = n"""
        s = StepResult(name)
        self.steps.append(s)
        t0 = time.monotonic()
        ctx = {"run_id": self.run_id, "as_of_date": self.as_of_date, "layer": self.layer}
        log("step_start", step=name, **ctx)
        try:
            yield s
        except Exception as e:
            s.status, s.error = "FAILED", f"{type(e).__name__}: {e}"[:2000]
            raise
        else:
            s.status = "SUCCESS"
        finally:
            s.duration_s = round(time.monotonic() - t0, 3)
            log("step_end", step=name, status=s.status, rows_in=s.rows_in,
                rows_out=s.rows_out, duration_s=s.duration_s, error=s.error, **ctx)

    def rows(self) -> list[tuple]:
        return [(self.run_id, self.as_of_date, self.layer, s.step, s.status, s.rows_in,
                 s.rows_out, float(s.duration_s), s.error) for s in self.steps]

    def flush(self, spark, table: str) -> None:
        from pyspark.sql import functions as F

        if not self.steps:
            return
        df = spark.createDataFrame(
            self.rows(), "run_id string, as_of_date string, layer string, step string, "
                         "status string, rows_in long, rows_out long, duration_s double, "
                         "error string")
        (df.withColumn("as_of_date", F.col("as_of_date").cast("date"))
         .withColumn("logged_at", F.current_timestamp())
         .write.format("delta").mode("append").saveAsTable(table))
        self.steps = []
