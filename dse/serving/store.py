from __future__ import annotations

from typing import Any

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
