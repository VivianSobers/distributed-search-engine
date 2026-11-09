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


def main(argv=None) -> int:
    spark = build_session("index-job")
    try:
        source_snapshot = current_snapshot(spark, DOCUMENTS_TABLE)
        previous_snapshot = last_snapshot(spark, "index")
        if (
            source_snapshot is not None
            and previous_snapshot is not None
            and source_snapshot == previous_snapshot
        ):
            print(json.dumps({"status": "skipped"}))
            return 0

        documents = spark.table(DOCUMENTS_TABLE)
        postings, stats = build_index(documents)

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {INDEX_TABLE} (
                term STRING,
                doc_id STRING,
                tf INT,
                df INT
            ) USING iceberg
            """
        )
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {DOC_STATS_TABLE} (
                doc_id STRING,
                length INT
            ) USING iceberg
            """
        )

        postings.createOrReplaceTempView("index_job_postings")
        stats.createOrReplaceTempView("index_job_doc_stats")
        spark.sql(
            f"INSERT OVERWRITE {INDEX_TABLE} "
            "SELECT term, doc_id, tf, df FROM index_job_postings"
        )
        spark.sql(
            f"INSERT OVERWRITE {DOC_STATS_TABLE} "
            "SELECT doc_id, length FROM index_job_doc_stats"
        )
        spark.catalog.dropTempView("index_job_postings")
        spark.catalog.dropTempView("index_job_doc_stats")

        if source_snapshot is not None:
            record_snapshot(spark, "index", source_snapshot)

        print(
            json.dumps(
                {
                    "status": "ok",
                    "postings": postings.count(),
                    "documents": stats.count(),
                    "source_snapshot": source_snapshot,
                },
                separators=(",", ":"),
            )
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
