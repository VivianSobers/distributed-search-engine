import json

from pyspark.sql import Row

from dse.config import (
    DOC_STATS_TABLE,
    DOCUMENTS_TABLE,
    INDEX_TABLE,
    PIPELINE_STATE_TABLE,
)
from dse.jobs import index_job
from dse.jobs.index_job import build_index, main


TABLES = (
    DOCUMENTS_TABLE,
    INDEX_TABLE,
    DOC_STATS_TABLE,
    PIPELINE_STATE_TABLE,
)


def _drop_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.search")
    for table in TABLES:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def _create_documents(spark):
    spark.sql(
        f"""
        CREATE TABLE {DOCUMENTS_TABLE} (
            doc_id STRING,
            url STRING,
            title STRING,
            text STRING,
            links ARRAY<STRING>,
            version INT,
            content_hash STRING,
            is_deleted BOOLEAN,
            updated_phase INT
        ) USING iceberg
        """
    )


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


def test_main_skips_when_documents_snapshot_is_unchanged(spark, monkeypatch, capsys):
    _drop_tables(spark)
    _create_documents(spark)
    spark.sql(
        f"""
        INSERT INTO {DOCUMENTS_TABLE}
        VALUES ('A', '', '', 'alpha beta', array(), 1, '', false, 0)
        """
    )
    monkeypatch.setattr(index_job, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    assert main([]) == 0
    assert main([]) == 0

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert json.loads(lines[-1]) == {"status": "skipped"}


def test_main_rebuilds_after_snapshot_change_and_removes_deleted_postings(
    spark, monkeypatch, capsys
):
    _drop_tables(spark)
    _create_documents(spark)
    spark.sql(
        f"""
        INSERT INTO {DOCUMENTS_TABLE}
        VALUES
            ('A', '', '', 'alpha shared', array(), 1, '', false, 0),
            ('B', '', '', 'beta shared', array(), 1, '', false, 0)
        """
    )
    monkeypatch.setattr(index_job, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    assert main([]) == 0
    spark.sql(
        f"""
        INSERT OVERWRITE {DOCUMENTS_TABLE}
        VALUES
            ('A', '', '', 'alpha shared', array(), 1, '', false, 0),
            ('B', '', '', '', array(), 2, '', true, 1),
            ('C', '', '', 'gamma shared', array(), 1, '', false, 1)
        """
    )
    assert main([]) == 0

    latest = json.loads(
        [line for line in capsys.readouterr().out.splitlines() if line.strip()][-1]
    )
    assert latest["status"] == "ok"
    assert {
        (row.term, row.doc_id, row.tf, row.df)
        for row in spark.table(INDEX_TABLE).collect()
    } == {
        ("alpha", "A", 1, 1),
        ("shared", "A", 1, 2),
        ("gamma", "C", 1, 1),
        ("shared", "C", 1, 2),
    }
    assert {row.doc_id: row.length for row in spark.table(DOC_STATS_TABLE).collect()} == {
        "A": 2,
        "C": 2,
    }
