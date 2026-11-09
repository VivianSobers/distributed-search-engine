from __future__ import annotations

import importlib
import sys

if __name__ == "__main__":
    sys.exit(importlib.import_module(sys.argv[1]).main(sys.argv[2:]))
