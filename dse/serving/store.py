from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds

from dse.ranking import bm25_term, combine_scores, idf
from dse.text import tokenize


class ExportError(ValueError):
    pass


class SearchIndex:
    def __init__(
        self,
        version: int,
        docs: dict[str, dict[str, Any]],
        postings: dict[str, dict[str, float]],
    ) -> None:
        if not isinstance(version, int):
            raise ExportError("invalid version")
        self.version = version
        self._docs: dict[str, dict[str, float | str]] = {}
        for doc_id, row in docs.items():
            if not isinstance(doc_id, str) or doc_id == "":
                raise ExportError("invalid doc_id")
            url = row.get("url")
            title = row.get("title")
            if not isinstance(url, str) or not isinstance(title, str):
                raise ExportError("invalid document metadata")
            doc_len = float(row.get("doc_len", 0.0))
            pagerank = float(row.get("pagerank", 0.0))
            self._docs[doc_id] = {
                "url": url,
                "title": title,
                "doc_len": max(doc_len, 0.0),
                "pagerank": pagerank,
            }
        self._postings = postings

        self.docs_count = len(self._docs)
        avgdl = (
            sum(float(row["doc_len"]) for row in self._docs.values()) / self.docs_count
            if self.docs_count > 0
            else 1.0
        )
        self._avgdl = max(1.0, avgdl)
        self._max_pagerank = (
            max(float(row["pagerank"]) for row in self._docs.values()) if self._docs else 0.0
        )

    def search(self, query: str, k: int = 10) -> list[dict[str, float | str]]:
        if self.docs_count == 0:
            return []

        terms = tokenize(query)
        if not terms:
            return []

        bm25_scores: dict[str, float] = {}
        for term in terms:
            postings = self._postings.get(term)
            if not postings:
                continue
            df = len(postings)
            term_idf = idf(self.docs_count, df)
            for doc_id, tf in postings.items():
                doc = self._docs.get(doc_id)
                if doc is None:
                    continue
                contrib = bm25_term(float(tf), float(doc["doc_len"]), self._avgdl, term_idf)
                bm25_scores[doc_id] = bm25_scores.get(doc_id, 0.0) + contrib

        if not bm25_scores:
            return []

        pagerank = {doc_id: float(row["pagerank"]) for doc_id, row in self._docs.items()}
        combined = combine_scores(bm25_scores, pagerank, self._max_pagerank)

        rows: list[dict[str, float | str]] = []
        for doc_id, score in combined.items():
            doc = self._docs[doc_id]
            rows.append(
                {
                    "doc_id": doc_id,
                    "url": str(doc["url"]),
                    "title": str(doc["title"]),
                    "score": float(score),
                    "bm25": float(bm25_scores.get(doc_id, 0.0)),
                    "pagerank": float(doc["pagerank"]),
                }
            )

        rows.sort(key=lambda row: (-float(row["score"]), str(row["doc_id"])))
        return rows[:k]


def _required_dir(path: Path, name: str) -> Path:
    target = path / name
    if not target.is_dir():
        raise ExportError(f"missing {name}/")
    return target


def _manifest_count(manifest: dict[str, Any], key: str) -> int:
    value = manifest.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ExportError(f"manifest missing {key} count")
    return value


def _manifest_version(manifest: dict[str, Any]) -> int:
    value = manifest.get("version")
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExportError("manifest missing version")
    return value


def _load_rows(dir_path: Path) -> list[dict[str, Any]]:
    try:
        return ds.dataset(str(dir_path), format="parquet").to_table().to_pylist()
    except (OSError, pa.ArrowException) as exc:
        raise ExportError(f"unreadable parquet in {dir_path.name}/") from exc


def _parse_doc(row: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    doc_id, url, title = row.get("doc_id"), row.get("url"), row.get("title")
    length, pagerank = row.get("length"), row.get("pagerank")
    if not (isinstance(doc_id, str) and doc_id and isinstance(url, str) and isinstance(title, str)):
        raise ExportError("invalid docs row")
    if not isinstance(length, int) or not isinstance(pagerank, (int, float)):
        raise ExportError("invalid docs row")
    return doc_id, {"url": url, "title": title, "doc_len": float(length), "pagerank": float(pagerank)}


def load_version(version_dir: Path) -> SearchIndex:
    manifest_path = version_dir / "manifest.json"
    if not manifest_path.is_file():
        raise ExportError("missing manifest.json")
    docs_dir = _required_dir(version_dir, "docs")
    index_dir = _required_dir(version_dir, "index")

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExportError("invalid manifest.json") from exc
    if not isinstance(manifest, dict):
        raise ExportError("invalid manifest.json")

    docs_rows = _load_rows(docs_dir)
    index_rows = _load_rows(index_dir)
    if _manifest_count(manifest, "docs") != len(docs_rows):
        raise ExportError("manifest docs count does not match data")
    if _manifest_count(manifest, "postings") != len(index_rows):
        raise ExportError("manifest postings count does not match data")

    docs: dict[str, dict[str, Any]] = {}
    for row in docs_rows:
        doc_id, doc = _parse_doc(row)
        if doc_id in docs:
            raise ExportError("duplicate doc_id")
        docs[doc_id] = doc

    postings: dict[str, dict[str, float]] = {}
    for row in index_rows:
        term, doc_id, tf = row.get("term"), row.get("doc_id"), row.get("tf")
        if not (isinstance(term, str) and term and isinstance(tf, int)) or doc_id not in docs:
            raise ExportError("invalid index row")
        postings.setdefault(term, {})[doc_id] = float(tf)

    return SearchIndex(version=_manifest_version(manifest), docs=docs, postings=postings)
