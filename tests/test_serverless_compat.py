"""Code chạy trên Databricks serverless (Free Edition) không được dùng API bị chặn ở đó."""

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src"
# NOT_SUPPORTED_WITH_SERVERLESS: cache/persist, RDD / SparkContext
BANNED = re.compile(r"\.(cache|persist|unpersist)\(\)|\bsparkContext\b|\.rdd\b")


def test_no_serverless_banned_calls_in_pipeline_code():
    hits = []
    for p in SRC.rglob("*.py"):
        for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1):
            code = line.split("#", 1)[0]
            if BANNED.search(code):
                hits.append(f"{p.relative_to(SRC)}:{i}: {line.strip()}")
    assert not hits, "\n".join(hits)
