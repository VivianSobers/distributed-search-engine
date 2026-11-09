import math

from dse.config import BM25_B, BM25_K1


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
