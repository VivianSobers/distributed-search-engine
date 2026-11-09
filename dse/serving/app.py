from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from dse.serving.store import ExportError, SearchIndex, load_version


def create_app(serving_root: Path) -> FastAPI:
    index_ref: dict[str, SearchIndex | None] = {"index": None}
    static_html = Path(__file__).with_name("static") / "index.html"

    def _resolve_current() -> Path:
        current = serving_root / "current"
        if not current.exists():
            raise ExportError("missing current")
        return current.resolve()

    def _load_current() -> SearchIndex:
        return load_version(_resolve_current())

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            index_ref["index"] = _load_current()
        except ExportError:
            index_ref["index"] = None
        yield

    app = FastAPI(lifespan=lifespan)

    @app.get("/health")
    def health() -> dict[str, int | str | None]:
        current = index_ref["index"]
        if current is None:
            return {"status": "unloaded", "version": None, "docs": 0}
        return {"status": "ok", "version": current.version, "docs": current.docs_count}

    @app.get("/search")
    def search(q: str = "", k: int = Query(10, ge=1, le=100)) -> dict[str, object]:
        current = index_ref["index"]
        if current is None:
            raise HTTPException(status_code=503, detail="index not loaded")
        results = [] if q.strip() == "" else current.search(q, k=k)
        return {"version": current.version, "query": q, "results": results}

    @app.post("/reload")
    def reload_current() -> dict[str, int | str]:
        try:
            loaded = _load_current()
        except ExportError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        index_ref["index"] = loaded
        return {"status": "ok", "version": loaded.version, "docs": loaded.docs_count}

    @app.get("/", response_class=HTMLResponse)
    def root() -> HTMLResponse:
        html = static_html.read_text(encoding="utf-8")
        return HTMLResponse(content=html)

    return app
