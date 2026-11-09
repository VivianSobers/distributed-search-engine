import json
import inspect

import pytest
from pyspark.sql import Row
from pyspark.sql import types as T

from dse.config import (
    DOCUMENTS_TABLE,
    LINKS_TABLE,
    PIPELINE_STATE_TABLE,
)
from dse.jobs import pagerank_job
from dse.jobs.pagerank_job import compute_pagerank, main
from dse.jobs.state import current_snapshot


def _collect_ranks(df):
    return {row["doc_id"]: row["rank"] for row in df.collect()}


def _nodes_df(spark, ids):
    return spark.createDataFrame([Row(doc_id=node_id) for node_id in ids])


def _edges_df(spark, pairs):
    schema = T.StructType(
        [
            T.StructField("src_id", T.StringType(), False),
            T.StructField("dst_id", T.StringType(), False),
        ]
    )
    rows = [Row(src_id=src, dst_id=dst) for src, dst in pairs]
    return spark.createDataFrame(rows, schema=schema)


def test_cycle_two_nodes_equal_rank(spark):
    ranks = compute_pagerank(
        spark,
        _nodes_df(spark, ["A", "B"]),
        _edges_df(spark, [("A", "B"), ("B", "A")]),
        iterations=20,
    )
    values = _collect_ranks(ranks)
    assert values["A"] == pytest.approx(0.5, abs=1e-9)
    assert values["B"] == pytest.approx(0.5, abs=1e-9)


def test_single_direction_with_dangling_node(spark):
    ranks = compute_pagerank(
        spark,
        _nodes_df(spark, ["A", "B"]),
        _edges_df(spark, [("A", "B")]),
        iterations=100,
    )
    values = _collect_ranks(ranks)
    assert values["A"] == pytest.approx(0.350877, abs=1e-5)
    assert values["B"] == pytest.approx(0.649123, abs=1e-5)


def test_ranks_sum_to_one_with_dangling_and_self_link(spark):
    ranks = compute_pagerank(
        spark,
        _nodes_df(spark, ["A", "B", "C", "D", "E"]),
        _edges_df(spark, [("A", "B"), ("B", "C"), ("C", "C"), ("E", "A")]),
        iterations=40,
    )
    total_rank = sum(_collect_ranks(ranks).values())
    assert total_rank == pytest.approx(1.0, abs=1e-9)


def test_duplicate_edges_do_not_change_ranks(spark):
    nodes = _nodes_df(spark, ["A", "B", "C"])
    unique = _edges_df(spark, [("A", "B"), ("A", "C"), ("C", "A")])
    duplicated = _edges_df(
        spark,
        [("A", "B"), ("A", "B"), ("A", "C"), ("C", "A"), ("C", "A")],
    )
    unique_ranks = _collect_ranks(compute_pagerank(spark, nodes, unique, iterations=30))
    duplicated_ranks = _collect_ranks(compute_pagerank(spark, nodes, duplicated, iterations=30))
    assert duplicated_ranks == pytest.approx(unique_ranks, abs=1e-9)


def test_single_isolated_node_has_rank_one(spark):
    ranks = compute_pagerank(
        spark,
        _nodes_df(spark, ["A"]),
        _edges_df(spark, []),
        iterations=20,
    )
    values = _collect_ranks(ranks)
    assert values["A"] == pytest.approx(1.0, abs=1e-9)


def test_iterations_below_minimum_raise_value_error(spark):
    with pytest.raises(ValueError):
        compute_pagerank(
            spark,
            _nodes_df(spark, ["A", "B"]),
            _edges_df(spark, [("A", "B")]),
            iterations=9,
        )


def test_checkpointing_keeps_same_ranks(spark, tmp_path):
    spark.sparkContext.setCheckpointDir(f"file://{tmp_path / 'chk'}")
    nodes = _nodes_df(spark, ["A", "B", "C", "D"])
    edges = _edges_df(spark, [("A", "B"), ("B", "C"), ("C", "A"), ("D", "C")])
    without_checkpoint = _collect_ranks(
        compute_pagerank(
            spark,
            nodes,
            edges,
            iterations=30,
            checkpoint_every=0,
        )
    )
    with_checkpoint = _collect_ranks(
        compute_pagerank(
            spark,
            nodes,
            edges,
            iterations=30,
            checkpoint_every=5,
        )
    )
    assert with_checkpoint == pytest.approx(without_checkpoint, abs=1e-9)


def test_main_skips_second_run_when_snapshot_unchanged(spark, monkeypatch, tmp_path, capsys):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.search")
    spark.sql(f"DROP TABLE IF EXISTS {DOCUMENTS_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {LINKS_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {PIPELINE_STATE_TABLE}")
    spark.sql(
        f"CREATE TABLE {DOCUMENTS_TABLE} (doc_id STRING, is_deleted BOOLEAN) USING iceberg"
    )
    spark.sql(f"CREATE TABLE {LINKS_TABLE} (src_id STRING, dst_id STRING) USING iceberg")
    spark.sql(
        f"INSERT INTO {DOCUMENTS_TABLE} VALUES ('A', false), ('B', false), ('C', false)"
    )
    spark.sql(f"INSERT INTO {LINKS_TABLE} VALUES ('A', 'B'), ('B', 'C')")

    monkeypatch.setattr(pagerank_job, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    checkpoint_dir = f"file://{tmp_path / 'chk'}"
    assert main(["--checkpoint-dir", checkpoint_dir]) == 0
    assert main(["--checkpoint-dir", checkpoint_dir]) == 0

    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert json.loads(lines[-1]) == {"status": "skipped"}


def test_compute_pagerank_source_has_no_collect_or_to_pandas():
    source = inspect.getsource(compute_pagerank)
    assert "collect(" not in source
    assert "toPandas(" not in source


def test_main_recomputes_when_documents_snapshot_changes(
    spark, monkeypatch, tmp_path, capsys
):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.search")
    spark.sql(f"DROP TABLE IF EXISTS {DOCUMENTS_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {LINKS_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {PIPELINE_STATE_TABLE}")
    spark.sql("DROP TABLE IF EXISTS iceberg_catalog.search.pagerank")
    spark.sql(
        f"CREATE TABLE {DOCUMENTS_TABLE} (doc_id STRING, is_deleted BOOLEAN) USING iceberg"
    )
    spark.sql(f"CREATE TABLE {LINKS_TABLE} (src_id STRING, dst_id STRING) USING iceberg")
    spark.sql(
        f"INSERT INTO {DOCUMENTS_TABLE} VALUES ('A', false), ('B', false), ('C', false)"
    )
    spark.sql(f"INSERT INTO {LINKS_TABLE} VALUES ('A', 'B'), ('B', 'C')")

    monkeypatch.setattr(pagerank_job, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    checkpoint_dir = f"file://{tmp_path / 'chk'}"
    assert main(["--checkpoint-dir", checkpoint_dir]) == 0

    spark.sql(f"INSERT OVERWRITE {DOCUMENTS_TABLE} VALUES ('A', false), ('D', false)")
    spark.sql(f"INSERT OVERWRITE {LINKS_TABLE} VALUES ('A', 'D')")

    assert main(["--checkpoint-dir", checkpoint_dir]) == 0

    output_lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    second_run = json.loads(output_lines[-1])
    assert second_run["status"] == "ok"

    pagerank_doc_ids = {
        row["doc_id"] for row in spark.sql("SELECT doc_id FROM iceberg_catalog.search.pagerank").collect()
    }
    assert pagerank_doc_ids == {"A", "D"}


def test_current_snapshot_follows_rollback_history(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.search")
    spark.sql(f"DROP TABLE IF EXISTS {DOCUMENTS_TABLE}")
    spark.sql(
        f"CREATE TABLE {DOCUMENTS_TABLE} (doc_id STRING, is_deleted BOOLEAN) USING iceberg"
    )
    spark.sql(f"INSERT INTO {DOCUMENTS_TABLE} VALUES ('A', false)")
    first_snapshot = spark.sql(
        f"""
        SELECT snapshot_id
        FROM {DOCUMENTS_TABLE}.history
        ORDER BY made_current_at ASC
        LIMIT 1
        """
    ).collect()[0]["snapshot_id"]
    spark.sql(f"INSERT INTO {DOCUMENTS_TABLE} VALUES ('B', false)")
    spark.sql(
        f"CALL iceberg_catalog.system.rollback_to_snapshot('search.documents', {first_snapshot})"
    )

    assert current_snapshot(spark, DOCUMENTS_TABLE) == int(first_snapshot)
