import math


def idf(n_docs: int, df: int) -> float:
    """Calculate inverse document frequency using the course BM25 formula.

    IDF = ln(1 + (N - df + 0.5) / (df + 0.5))
    """
    return math.log(1.0 + (n_docs - df + 0.5) / (df + 0.5))
