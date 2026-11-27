"""PYTHONPATH=src python -m common.lakeview [thư mục ra=dashboards] [catalog]"""

import sys

from common.lakeview import write_all

for path in write_all(sys.argv[1] if len(sys.argv) > 1 else "dashboards",
                      sys.argv[2] if len(sys.argv) > 2 else ""):
    print(path)
