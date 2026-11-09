import json
from operator import add

from pyspark.sql import functions as F
from pyspark.sql import types as T

from dse.config import DOC_STATS_TABLE, DOCUMENTS_TABLE, INDEX_TABLE
from dse.jobs.state import current_snapshot, last_snapshot, record_snapshot
from dse.spark_session import build_session
from dse.text import tokenize


INDEX_SCHEMA = T.StructType(
    [
        T.StructField("term", T.StringType(), nullable=False),
        T.StructField("doc_id", T.StringType(), nullable=False),
        T.StructField("tf", T.IntegerType(), nullable=False),
        T.StructField("df", T.IntegerType(), nullable=False),
    ]
)

DOC_STATS_SCHEMA = T.StructType(
    [
        T.StructField("doc_id", T.StringType(), nullable=False),
        T.StructField("length", T.IntegerType(), nullable=False),
    ]
)


def build_index(documents):
    live_documents = documents.where(F.col("is_deleted") == F.lit(False)).select(
        "doc_id", "text"
    )
    tokenized = live_documents.rdd.map(
        lambda row: (row["doc_id"], tokenize(row["text"] or ""))
    )
    stats = tokenized.map(lambda item: (item[0], len(item[1])))

    term_frequencies = (
        tokenized.flatMap(
            lambda item: [((term, item[0]), 1) for term in item[1]]
        )
        .reduceByKey(add)
    )
    document_frequencies = (
        term_frequencies.map(lambda item: (item[0][0], item[0][1]))
        .distinct()
        .map(lambda item: (item[0], 1))
        .reduceByKey(add)
    )
    postings = (
        term_frequencies.map(
            lambda item: (item[0][0], (item[0][1], item[1]))
        )
        .join(document_frequencies)
        .map(
            lambda item: (
                item[0],
                item[1][0][0],
                int(item[1][0][1]),
                int(item[1][1]),
            )
        )
    )
    return (
        live_documents.sparkSession.createDataFrame(postings, schema=INDEX_SCHEMA),
        live_documents.sparkSession.createDataFrame(stats, schema=DOC_STATS_SCHEMA),
    )
