import json
import threading
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from dse.ranking import bm25_term, combine_scores, idf
from dse.serving.app import create_app
from dse.serving.store import ExportError, SearchIndex, load_version


def _write_export(
    root: Path,
    version: int,
    docs_rows: list[dict],
    index_rows: list[dict],
) -> Path:
    version_dir = root / f"v{version}"
    docs_dir = version_dir / "docs"
    index_dir = version_dir / "index"
    docs_dir.mkdir(parents=True)
    index_dir.mkdir(parents=True)

    pq.write_table(pa.Table.from_pylist(docs_rows), docs_dir / "part-000.parquet")
    pq.write_table(pa.Table.from_pylist(index_rows), index_dir / "part-000.parquet")

    manifest = {
        "version": version,
        "docs": len(docs_rows),
        "postings": len(index_rows),
    }
    (version_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return version_dir


@pytest.fixture()
def serving_root(tmp_path: Path) -> Path:
    docs = [
        {
            "doc_id": "d1",
            "url": "https://example.com/d1",
            "title": "Alpha One",
            "length": 3,
            "pagerank": 0.5,
        },
        {
            "doc_id": "d2",
            "url": "https://example.com/d2",
            "title": "Beta Two",
            "length": 2,
            "pagerank": 0.3,
        },
        {
            "doc_id": "d3",
            "url": "https://example.com/d3",
            "title": "Delta Three",
            "length": 2,
            "pagerank": 0.2,
        },
    ]
    index_rows = [
        {"term": "alpha", "doc_id": "d1", "tf": 2},
        {"term": "beta", "doc_id": "d1", "tf": 1},
        {"term": "beta", "doc_id": "d2", "tf": 1},
        {"term": "gamma", "doc_id": "d2", "tf": 1},
        {"term": "delta", "doc_id": "d3", "tf": 1},
    ]
    v1 = _write_export(tmp_path, 1, docs, index_rows)
    (tmp_path / "current").symlink_to(v1.name)
    return tmp_path


def test_load_version_requires_manifest_and_dirs(tmp_path: Path):
    version_dir = tmp_path / "v1"
    version_dir.mkdir()
    with pytest.raises(ExportError):
        load_version(version_dir)


def test_load_version_rejects_count_mismatch(serving_root: Path):
    version_dir = serving_root / "v1"
    manifest = json.loads((version_dir / "manifest.json").read_text(encoding="utf-8"))
    manifest["docs"] += 1
    (version_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ExportError):
        load_version(version_dir)


def test_load_version_rejects_truncated_parquet(serving_root: Path):
    part = serving_root / "v1" / "docs" / "part-000.parquet"
    part.write_bytes(part.read_bytes()[:20])
    with pytest.raises(ExportError):
        load_version(serving_root / "v1")


def test_load_version_rejects_posting_for_unknown_document(serving_root: Path):
    index_dir = serving_root / "v1" / "index"
    rows = [{"term": "ghost", "doc_id": "nope", "tf": 1}]
    pq.write_table(pa.Table.from_pylist(rows), index_dir / "part-000.parquet")
    manifest_path = serving_root / "v1" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["postings"] = 1
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ExportError):
        load_version(serving_root / "v1")


def test_search_ranking_uses_repeated_query_terms(serving_root: Path):
    index = load_version(serving_root / "v1")
    results = index.search("alpha beta beta", k=10)
    assert [row["doc_id"] for row in results] == ["d1", "d2"]

    n_docs = 3
    avgdl = (3 + 2 + 2) / 3
    idf_alpha = idf(n_docs, 1)
    idf_beta = idf(n_docs, 2)
    d1_bm25 = bm25_term(2, 3, avgdl, idf_alpha) + bm25_term(1, 3, avgdl, idf_beta) + bm25_term(
        1, 3, avgdl, idf_beta
    )
    d2_bm25 = bm25_term(1, 2, avgdl, idf_beta) + bm25_term(1, 2, avgdl, idf_beta)
    combined = combine_scores({"d1": d1_bm25, "d2": d2_bm25}, {"d1": 0.5, "d2": 0.3}, 0.5)

    assert results[0]["bm25"] == pytest.approx(d1_bm25)
    assert results[1]["bm25"] == pytest.approx(d2_bm25)
    assert results[0]["score"] == pytest.approx(combined["d1"])
    assert results[1]["score"] == pytest.approx(combined["d2"])


def test_search_handles_unicode_and_punctuation_queries(serving_root: Path):
    index = load_version(serving_root / "v1")
    assert index.search("!!! ???") == []
    assert index.search("Привет") == []


def test_api_search_and_k_bounds(serving_root: Path):
    app = create_app(serving_root)
    with TestClient(app) as client:
        ok = client.get("/search", params={"q": "beta", "k": 1})
        assert ok.status_code == 200
        body = ok.json()
        assert body["version"] == 1
        assert body["query"] == "beta"
        assert len(body["results"]) == 1

        blank = client.get("/search", params={"q": "   "})
        assert blank.status_code == 200
        assert blank.json()["results"] == []

        assert client.get("/search", params={"q": "beta", "k": 0}).status_code == 422
        assert client.get("/search", params={"q": "beta", "k": 101}).status_code == 422


def test_api_search_returns_503_when_unloaded(tmp_path: Path):
    app = create_app(tmp_path)
    with TestClient(app) as client:
        response = client.get("/search", params={"q": "anything"})
        assert response.status_code == 503


def test_reload_keeps_old_index_on_corrupt_export(serving_root: Path):
    docs = [
        {
            "doc_id": "n1",
            "url": "https://example.com/n1",
            "title": "New One",
            "length": 2,
            "pagerank": 1.0,
        }
    ]
    rows = [{"term": "new", "doc_id": "n1", "tf": 1}]
    new_dir = _write_export(serving_root, 2, docs, rows)
    bad_manifest = {"version": 2, "docs": 999, "postings": 999}
    (new_dir / "manifest.json").write_text(json.dumps(bad_manifest), encoding="utf-8")

    app = create_app(serving_root)
    with TestClient(app) as client:
        before = client.get("/search", params={"q": "beta"}).json()
        assert before["version"] == 1

        current = serving_root / "current"
        current.unlink()
        current.symlink_to(new_dir.name)

        reload_resp = client.post("/reload")
        assert reload_resp.status_code == 409

        after = client.get("/search", params={"q": "beta"}).json()
        assert after["version"] == 1
        assert after["results"] == before["results"]


def test_reload_swap_is_atomic_for_concurrent_readers(serving_root: Path):
    docs = [
        {
            "doc_id": "z9",
            "url": "https://example.com/z9",
            "title": "Zeta Nine",
            "length": 1,
            "pagerank": 1.0,
        }
    ]
    rows = [{"term": "beta", "doc_id": "z9", "tf": 1}]
    new_dir = _write_export(serving_root, 2, docs, rows)

    app = create_app(serving_root)
    seen_versions: list[int] = []
    seen_top_docs: list[str] = []
    stop = threading.Event()

    def reader(client: TestClient):
        while not stop.is_set():
            resp = client.get("/search", params={"q": "beta"})
            if resp.status_code == 200:
                payload = resp.json()
                seen_versions.append(payload["version"])
                if payload["results"]:
                    seen_top_docs.append(payload["results"][0]["doc_id"])

    with TestClient(app) as client:
        workers = [threading.Thread(target=reader, args=(client,)) for _ in range(4)]
        for worker in workers:
            worker.start()

        current = serving_root / "current"
        current.unlink()
        current.symlink_to(new_dir.name)
        reload_resp = client.post("/reload")
        assert reload_resp.status_code == 200

        stop.set()
        for worker in workers:
            worker.join()

    assert set(seen_versions).issubset({1, 2})
    assert set(seen_top_docs).issubset({"d1", "z9"})


def test_reload_refuses_truncated_parquet_and_keeps_serving(serving_root: Path):
    app = create_app(serving_root)
    with TestClient(app) as client:
        assert client.get("/search", params={"q": "beta"}).json()["version"] == 1
        part = serving_root / "v1" / "index" / "part-000.parquet"
        part.write_bytes(b"not parquet")
        assert client.post("/reload").status_code == 409
        assert client.get("/search", params={"q": "beta"}).json()["version"] == 1


def test_frontend_never_writes_results_as_html(serving_root: Path):
    with TestClient(create_app(serving_root)) as client:
        html = client.get("/").text
    assert "innerHTML" not in html


def test_health_and_root_endpoint(serving_root: Path):
    app = create_app(serving_root)
    with TestClient(app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        payload = health.json()
        assert payload["status"] == "ok"
        assert payload["version"] == 1
        assert payload["docs"] == 3

        root = client.get("/")
        assert root.status_code == 200
        assert "text/html" in root.headers["content-type"]
        assert "Live version" in root.text
        assert "doc id" in root.text.lower()


def test_search_index_tie_breaks_on_doc_id():
    docs = {
        "a": {"url": "u", "title": "A", "doc_len": 1.0, "pagerank": 1.0},
        "b": {"url": "u", "title": "B", "doc_len": 1.0, "pagerank": 1.0},
    }
    postings = {"x": {"a": 1.0, "b": 1.0}}
    index = SearchIndex(version=7, docs=docs, postings=postings)
    results = index.search("x", k=10)
    assert [row["doc_id"] for row in results] == ["a", "b"]
