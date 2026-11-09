from __future__ import annotations

import os
from pathlib import Path
from fastapi import FastAPI
from dse.serving.app import create_app


def make_app() -> FastAPI:
    return create_app(Path(os.environ.get("SERVING_ROOT", "/serving")))
