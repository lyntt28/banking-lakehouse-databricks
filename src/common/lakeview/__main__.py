"""PYTHONPATH=src python -m common.lakeview [thư mục ra=dashboards] [catalog]

Sinh dashboards/*.lvdash.json + dashboards/custom_viz/** (spec Vega-Lite để dán vào UI)."""

import sys
from pathlib import Path

from common.lakeview import write_all
from common.lakeview.custom_viz import write_specs

out = sys.argv[1] if len(sys.argv) > 1 else "dashboards"
for path in write_all(out, sys.argv[2] if len(sys.argv) > 2 else ""):
    print(path)
for path in write_specs(Path(out) / "custom_viz"):
    print(path)
