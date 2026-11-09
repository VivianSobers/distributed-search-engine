import math

from dse.config import BM25_B, BM25_K1, BM25_WEIGHT, PAGERANK_WEIGHT


def idf(n_docs: int, df: int) -> float:
    """Calculate inverse document frequency using the course BM25 formula.

    IDF = ln(1 + (N - df + 0.5) / (df + 0.5))
    """
    return math.log(1.0 + (n_docs - df + 0.5) / (df + 0.5))


def bm25_term(
    tf: float,
    doc_len: float,
    avgdl: float,
    idf_value: float,
    k1: float = BM25_K1,
    b: float = BM25_B,
) -> float:
    """Calculate the BM25 score contribution for a single term."""
    if tf <= 0.0:
        return 0.0
    denom = tf + k1 * (1.0 - b + b * (doc_len / avgdl))
    if denom == 0.0:
        return 0.0
    return idf_value * (tf * (k1 + 1.0)) / denom


def combine_scores(
    bm25: dict[str, float],
    pagerank: dict[str, float],
    max_pagerank: float,
) -> dict[str, float]:
    """Combine BM25 and PageRank scores using the formula:

    Final score = BM25_WEIGHT * (BM25 / max BM25) + PAGERANK_WEIGHT * (PageRank / max PageRank)
    """
    if not bm25:
        return {}

    max_bm25 = max(bm25.values())

    combined: dict[str, float] = {}
    for doc_id, bm25_val in bm25.items():
        norm_bm25 = (bm25_val / max_bm25) if max_bm25 > 0.0 else 0.0
        pr_val = pagerank.get(doc_id, 0.0)
        norm_pr = (pr_val / max_pagerank) if max_pagerank > 0.0 else 0.0
        combined[doc_id] = BM25_WEIGHT * norm_bm25 + PAGERANK_WEIGHT * norm_pr

    return combined
