import json
from pathlib import Path

from pyspark.sql import Row
from pyspark.sql import types as T

from dse.config import DOC_STATS_TABLE, DOCUMENTS_TABLE, INDEX_TABLE, PAGERANK_TABLE
from dse.jobs import export_job
from dse.jobs.export_job import main, validate_export
from dse.jobs.state import current_snapshot, record_snapshot
from dse.serving.store import load_version


def _docs_df(spark, rows):
    schema = T.StructType(
        [
            T.StructField("doc_id", T.StringType(), True),
            T.StructField("url", T.StringType(), True),
            T.StructField("title", T.StringType(), True),
            T.StructField("length", T.IntegerType(), True),
            T.StructField("pagerank", T.DoubleType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema).select(
        "doc_id", "url", "title", "length", "pagerank"
    )


def _index_df(spark, rows):
    schema = T.StructType(
        [
            T.StructField("term", T.StringType(), True),
            T.StructField("doc_id", T.StringType(), True),
            T.StructField("tf", T.IntegerType(), True),
            T.StructField("df", T.IntegerType(), True),
        ]
    )
    return spark.createDataFrame(rows, schema=schema).select("term", "doc_id", "tf", "df")


def _base_docs_rows():
    return [
        Row(doc_id="d1", url="https://ex/1", title="One", length=3, pagerank=0.7),
        Row(doc_id="d2", url="https://ex/2", title="Two", length=2, pagerank=0.3),
    ]


def _base_index_rows():
    return [
        Row(term="alpha", doc_id="d1", tf=2, df=1),
        Row(term="beta", doc_id="d1", tf=1, df=2),
        Row(term="beta", doc_id="d2", tf=1, df=2),
    ]


def _drop_export_tables(spark):
    spark.sql(f"DROP TABLE IF EXISTS {INDEX_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {PAGERANK_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {DOC_STATS_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {DOCUMENTS_TABLE}")


def _create_export_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.search")
    spark.sql(
        f"""
        CREATE TABLE {DOCUMENTS_TABLE} (
            doc_id STRING,
            url STRING,
            title STRING,
            links ARRAY<STRING>,
            version INT,
            content_hash STRING,
            is_deleted BOOLEAN,
            updated_phase INT,
            text STRING
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        CREATE TABLE {DOC_STATS_TABLE} (
            doc_id STRING,
            length INT
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        CREATE TABLE {PAGERANK_TABLE} (
            doc_id STRING,
            rank DOUBLE
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        CREATE TABLE {INDEX_TABLE} (
            term STRING,
            doc_id STRING,
            tf INT,
            df INT
        ) USING iceberg
        """
    )


def _seed_phase_a(spark):
    spark.sql(
        f"""
        INSERT INTO {DOCUMENTS_TABLE} VALUES
        ('d1', 'https://ex/1', 'One', array('d2'), 0, 'h1', false, 0, 'alpha alpha beta'),
        ('d2', 'https://ex/2', 'Two', array(), 0, 'h2', false, 0, 'beta gamma')
        """
    )
    spark.sql(f"INSERT INTO {DOC_STATS_TABLE} VALUES ('d1', 3), ('d2', 2)")
    spark.sql(f"INSERT INTO {PAGERANK_TABLE} VALUES ('d1', 0.7), ('d2', 0.3)")
    spark.sql(
        f"""
        INSERT INTO {INDEX_TABLE} VALUES
        ('alpha', 'd1', 2, 1),
        ('beta', 'd1', 1, 2),
        ('beta', 'd2', 1, 2),
        ('gamma', 'd2', 1, 1)
        """
    )


def _seed_phase_b(spark):
    spark.sql(
        f"""
        INSERT OVERWRITE {DOCUMENTS_TABLE} VALUES
        ('d1', 'https://ex/1', 'One', array('d2'), 1, 'h3', false, 1, 'alpha alpha beta'),
        ('d2', 'https://ex/2', 'Two', array('d1'), 1, 'h4', false, 1, 'beta gamma'),
        ('d3', 'https://ex/3', 'Three', array(), 1, 'h5', false, 1, 'alpha delta')
        """
    )
    spark.sql(
        f"""
        INSERT OVERWRITE {DOC_STATS_TABLE} VALUES
        ('d1', 3), ('d2', 2), ('d3', 2)
        """
    )
    spark.sql(
        f"""
        INSERT OVERWRITE {PAGERANK_TABLE} VALUES
        ('d1', 0.5), ('d2', 0.3), ('d3', 0.2)
        """
    )
    spark.sql(
        f"""
        INSERT OVERWRITE {INDEX_TABLE} VALUES
        ('alpha', 'd1', 2, 2),
        ('alpha', 'd3', 1, 2),
        ('beta', 'd1', 1, 2),
        ('beta', 'd2', 1, 2),
        ('gamma', 'd2', 1, 1),
        ('delta', 'd3', 1, 1)
        """
    )


def _record_job_snapshots(spark):
    snapshot = current_snapshot(spark, DOCUMENTS_TABLE)
    assert snapshot is not None
    record_snapshot(spark, "index", snapshot)
    record_snapshot(spark, "pagerank", snapshot)


def test_validate_export_reports_docs_count_mismatch(spark):
    docs = _docs_df(spark, _base_docs_rows())
    index = _index_df(spark, _base_index_rows())
    errors = validate_export(docs, index, expected_live=3)
    assert "docs count mismatch" in errors


def test_validate_export_reports_docs_count_must_be_positive(spark):
    docs = _docs_df(spark, [])
    index = _index_df(spark, [])
    errors = validate_export(docs, index, expected_live=0)
    assert "docs count must be > 0" in errors


def test_validate_export_reports_null_doc_id_or_url(spark):
    docs = _docs_df(
        spark,
        [
            Row(doc_id=None, url="https://ex/1", title="One", length=3, pagerank=0.6),
            Row(doc_id="d2", url=None, title="Two", length=2, pagerank=0.4),
        ],
    )
    index = _index_df(spark, [])
    errors = validate_export(docs, index, expected_live=2)
    assert "docs has null doc_id" in errors
    assert "docs has null url" in errors


def test_validate_export_reports_null_title(spark):
    docs = _docs_df(
        spark,
        [
            Row(doc_id="d1", url="https://ex/1", title=None, length=3, pagerank=0.7),
            Row(doc_id="d2", url="https://ex/2", title="Two", length=2, pagerank=0.3),
        ],
    )
    index = _index_df(spark, _base_index_rows())
    errors = validate_export(docs, index, expected_live=2)
    assert "docs has null title" in errors


def test_validate_export_reports_null_length(spark):
    docs = _docs_df(
        spark,
        [
            Row(doc_id="d1", url="https://ex/1", title="One", length=None, pagerank=0.7),
            Row(doc_id="d2", url="https://ex/2", title="Two", length=2, pagerank=0.3),
        ],
    )
    index = _index_df(spark, _base_index_rows())
    errors = validate_export(docs, index, expected_live=2)
    assert "docs has null length" in errors


def test_validate_export_reports_duplicate_doc_ids(spark):
    docs = _docs_df(
        spark,
        [
            Row(doc_id="d1", url="https://ex/1", title="One", length=3, pagerank=0.5),
            Row(doc_id="d1", url="https://ex/1b", title="One B", length=4, pagerank=0.5),
        ],
    )
    index = _index_df(spark, [])
    errors = validate_export(docs, index, expected_live=2)
    assert "docs doc_id values are not unique" in errors


def test_validate_export_reports_index_orphans(spark):
    docs = _docs_df(spark, _base_docs_rows())
    index = _index_df(
        spark,
        [
            Row(term="alpha", doc_id="d1", tf=2, df=1),
            Row(term="ghost", doc_id="missing", tf=1, df=1),
        ],
    )
    errors = validate_export(docs, index, expected_live=2)
    assert "index has doc_id values missing in docs" in errors


def test_validate_export_reports_null_pagerank_and_bad_sum(spark):
    docs = _docs_df(
        spark,
        [
            Row(doc_id="d1", url="https://ex/1", title="One", length=3, pagerank=None),
            Row(doc_id="d2", url="https://ex/2", title="Two", length=2, pagerank=0.4),
        ],
    )
    index = _index_df(spark, _base_index_rows())
    errors = validate_export(docs, index, expected_live=2)
    assert "docs has null pagerank" in errors
    assert "pagerank sum must be 1 within 1e-6" in errors


def test_validate_export_reports_df_mismatch(spark):
    docs = _docs_df(spark, _base_docs_rows())
    index = _index_df(
        spark,
        [
            Row(term="beta", doc_id="d1", tf=1, df=1),
            Row(term="beta", doc_id="d2", tf=1, df=2),
        ],
    )
    errors = validate_export(docs, index, expected_live=2)
    assert "index df does not match posting count for term" in errors


def test_main_creates_v1_and_search_service_loads_it(spark, tmp_path: Path, monkeypatch, capsys):
    _drop_export_tables(spark)
    _create_export_tables(spark)
    _seed_phase_a(spark)
    _record_job_snapshots(spark)

    monkeypatch.setattr(export_job, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    root = tmp_path / "exports"
    rc = main(["--export-root", f"file://{root}"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert payload["status"] == "ok"
    assert payload["version"] == 1

    v1 = root / "v1"
    assert v1.is_dir()
    assert (v1 / "manifest.json").is_file()
    assert (v1 / "docs").is_dir()
    assert (v1 / "index").is_dir()
    assert not (root / ".tmp-v1").exists()

    index = load_version(v1)
    results = index.search("alpha", k=5)
    assert results
    assert results[0]["doc_id"] == "d1"


def test_main_second_run_skips_when_snapshot_unchanged(spark, tmp_path: Path, monkeypatch, capsys):
    _drop_export_tables(spark)
    _create_export_tables(spark)
    _seed_phase_a(spark)
    _record_job_snapshots(spark)

    monkeypatch.setattr(export_job, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    root = tmp_path / "exports"
    assert main(["--export-root", f"file://{root}"]) == 0
    assert main(["--export-root", f"file://{root}"]) == 0

    payload = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert payload == {"status": "skipped"}
    assert (root / "v1").is_dir()
    assert not (root / "v2").exists()


def test_main_new_snapshot_creates_v2(spark, tmp_path: Path, monkeypatch):
    _drop_export_tables(spark)
    _create_export_tables(spark)
    _seed_phase_a(spark)
    _record_job_snapshots(spark)

    monkeypatch.setattr(export_job, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    root = tmp_path / "exports"
    assert main(["--export-root", f"file://{root}"]) == 0
    _seed_phase_b(spark)
    _record_job_snapshots(spark)
    assert main(["--export-root", f"file://{root}"]) == 0

    assert (root / "v1").is_dir()
    assert (root / "v2").is_dir()
    manifest = json.loads((root / "v2" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["version"] == 2
    assert manifest["docs"] == 3


def test_main_simulate_failure_keeps_v1_and_cleans_tmp(spark, tmp_path: Path, monkeypatch, capsys):
    _drop_export_tables(spark)
    _create_export_tables(spark)
    _seed_phase_a(spark)
    _record_job_snapshots(spark)

    monkeypatch.setattr(export_job, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    root = tmp_path / "exports"
    assert main(["--export-root", f"file://{root}"]) == 0
    manifest_v1 = (root / "v1" / "manifest.json").read_text(encoding="utf-8")

    _seed_phase_b(spark)
    _record_job_snapshots(spark)
    rc = main(["--export-root", f"file://{root}", "--simulate-failure"])
    assert rc == 1

    payload = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert payload["status"] == "error"
    assert (root / "v1").is_dir()
    assert (root / "v1" / "manifest.json").read_text(encoding="utf-8") == manifest_v1
    assert not (root / "v2").exists()
    assert not (root / ".tmp-v2").exists()


def test_main_stale_index_state_returns_error_and_creates_nothing(
    spark, tmp_path: Path, monkeypatch, capsys
):
    _drop_export_tables(spark)
    _create_export_tables(spark)
    _seed_phase_a(spark)

    snapshot = current_snapshot(spark, DOCUMENTS_TABLE)
    assert snapshot is not None
    record_snapshot(spark, "index", snapshot - 1)
    record_snapshot(spark, "pagerank", snapshot)

    monkeypatch.setattr(export_job, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    root = tmp_path / "exports"
    rc = main(["--export-root", f"file://{root}"])
    assert rc == 1

    payload = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert payload == {"status": "error", "errors": ["index built from a different snapshot"]}
    assert not root.exists() or not any(root.iterdir())
