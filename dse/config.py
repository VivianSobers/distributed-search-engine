"""Values fixed by the course specification and names shared across the pipeline."""

BM25_K1 = 1.2
BM25_B = 0.75
BM25_WEIGHT = 0.7
PAGERANK_WEIGHT = 0.3
PAGERANK_DAMPING = 0.85
PAGERANK_MIN_ITERATIONS = 10
PAGERANK_DEFAULT_ITERATIONS = 20

CATALOG = "iceberg_catalog"
NAMESPACE = f"{CATALOG}.search"
DOCUMENTS_TABLE = f"{NAMESPACE}.documents"
LINKS_TABLE = f"{NAMESPACE}.links"
REJECTS_TABLE = f"{NAMESPACE}.ingest_rejects"
INDEX_TABLE = f"{NAMESPACE}.inverted_index"
DOC_STATS_TABLE = f"{NAMESPACE}.doc_stats"
PAGERANK_TABLE = f"{NAMESPACE}.pagerank"
PIPELINE_STATE_TABLE = f"{NAMESPACE}.pipeline_state"
