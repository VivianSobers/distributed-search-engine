from pyspark.sql import Row

from dse.jobs.index_job import build_index


def test_build_index_counts_live_tokens_and_keeps_empty_documents(spark):
    documents = spark.createDataFrame(
        [
            Row(doc_id="A", text="Cat cat DOG!", is_deleted=False),
            Row(doc_id="B", text="dog bird", is_deleted=False),
            Row(doc_id="C", text="", is_deleted=False),
            Row(doc_id="D", text="cat gone", is_deleted=True),
        ]
    )

    postings, stats = build_index(documents)

    assert {
        (row.term, row.doc_id, row.tf, row.df) for row in postings.collect()
    } == {
        ("cat", "A", 2, 1),
        ("dog", "A", 1, 2),
        ("dog", "B", 1, 2),
        ("bird", "B", 1, 1),
    }
    assert {row.doc_id: row.length for row in stats.collect()} == {
        "A": 3,
        "B": 2,
        "C": 0,
    }
