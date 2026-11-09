import json

from dse.config import DOCUMENTS_TABLE, LINKS_TABLE
from dse.jobs import fault_check
from dse.jobs.fault_check import main


def _read_parquet_ranks(spark, path):
    return {
        row["doc_id"]: row["rank"]
        for row in spark.read.parquet(path).select("doc_id", "rank").collect()
    }


def _seed_graph(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.search")
    spark.sql(f"DROP TABLE IF EXISTS {DOCUMENTS_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {LINKS_TABLE}")
    spark.sql(
        f"CREATE TABLE {DOCUMENTS_TABLE} (doc_id STRING, is_deleted BOOLEAN) USING iceberg"
    )
    spark.sql(f"CREATE TABLE {LINKS_TABLE} (src_id STRING, dst_id STRING) USING iceberg")
    spark.sql(
        f"""
        INSERT INTO {DOCUMENTS_TABLE} VALUES
        ('A', false), ('B', false), ('C', false), ('Z', true)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {LINKS_TABLE} VALUES
        ('A', 'B'), ('B', 'C'), ('C', 'A'), ('Z', 'A')
        """
    )


def test_main_identical_runs_ok(spark, monkeypatch, tmp_path, capsys):
    _seed_graph(spark)
    monkeypatch.setattr(fault_check, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    checkpoint = f"file://{tmp_path / 'chk'}"
    baseline = str(tmp_path / "baseline")
    after = str(tmp_path / "after")
    assert main(["--iterations", "40", "--checkpoint-dir", checkpoint, "--output", baseline]) == 0
    assert (
        main(
            [
                "--iterations",
                "40",
                "--checkpoint-dir",
                checkpoint,
                "--output",
                after,
                "--compare-to",
                baseline,
            ]
        )
        == 0
    )

    output_lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    payload = json.loads(output_lines[-1])
    assert payload["docs"] == 3
    assert payload["ok"] is True
    assert payload["max_abs_diff"] == 0.0


def test_main_returns_1_when_rank_tampered(spark, monkeypatch, tmp_path, capsys):
    _seed_graph(spark)
    monkeypatch.setattr(fault_check, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    checkpoint = f"file://{tmp_path / 'chk'}"
    baseline = str(tmp_path / "baseline")
    after = str(tmp_path / "after")
    assert main(["--iterations", "40", "--checkpoint-dir", checkpoint, "--output", baseline]) == 0

    ranks = _read_parquet_ranks(spark, baseline)
    tampered_rows = []
    for doc_id, rank in ranks.items():
        if doc_id == "A":
            tampered_rows.append((doc_id, float(rank) + 1e-4))
        else:
            tampered_rows.append((doc_id, float(rank)))
    spark.createDataFrame(tampered_rows, ["doc_id", "rank"]).write.mode("overwrite").parquet(
        str(tmp_path / "tampered")
    )

    assert (
        main(
            [
                "--iterations",
                "40",
                "--checkpoint-dir",
                checkpoint,
                "--output",
                after,
                "--compare-to",
                str(tmp_path / "tampered"),
            ]
        )
        == 1
    )

    output_lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    payload = json.loads(output_lines[-1])
    assert payload["ok"] is False
    assert payload["max_abs_diff"] > 1e-9


def test_main_returns_1_when_comparison_missing_doc(spark, monkeypatch, tmp_path, capsys):
    _seed_graph(spark)
    monkeypatch.setattr(fault_check, "build_session", lambda *args, **kwargs: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    checkpoint = f"file://{tmp_path / 'chk'}"
    baseline = str(tmp_path / "baseline")
    after = str(tmp_path / "after")
    assert main(["--iterations", "40", "--checkpoint-dir", checkpoint, "--output", baseline]) == 0

    missing_one = spark.read.parquet(baseline).where("doc_id <> 'A'")
    missing_path = str(tmp_path / "missing")
    missing_one.write.mode("overwrite").parquet(missing_path)

    assert (
        main(
            [
                "--iterations",
                "40",
                "--checkpoint-dir",
                checkpoint,
                "--output",
                after,
                "--compare-to",
                missing_path,
            ]
        )
        == 1
    )

    output_lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    payload = json.loads(output_lines[-1])
    assert payload["ok"] is False
