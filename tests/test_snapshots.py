import json

import pytest

from dse.config import (
    DOC_STATS_TABLE,
    DOCUMENTS_TABLE,
    INDEX_TABLE,
    LINKS_TABLE,
    PIPELINE_STATE_TABLE,
    REJECTS_TABLE,
)
from dse.jobs import index_job, snapshots
from dse.jobs.ingest import (
    deduplicate,
    ensure_tables,
    merge_documents,
    read_phase,
    rebuild_links,
    record_rejects,
)
from dse.jobs.snapshots import main
from dse.jobs.state import current_snapshot, last_snapshot


def _row(doc_id, links=()):
    return json.dumps(
        {
            "doc_id": doc_id,
            "url": f"http://x/{doc_id}",
            "title": f"t {doc_id}",
            "text": f"text {doc_id}",
            "links": list(links),
        }
    )


def _ingest(spark, path, rows, phase):
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    ensure_tables(spark)
    accepted, rejected = read_phase(spark, str(path), phase)
    record_rejects(spark, rejected, phase)
    merge_documents(spark, deduplicate(accepted), phase)
    rebuild_links(spark)


@pytest.fixture
def env(spark, monkeypatch, tmp_path, capsys):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.search")
    for table in (
        DOCUMENTS_TABLE,
        LINKS_TABLE,
        REJECTS_TABLE,
        INDEX_TABLE,
        DOC_STATS_TABLE,
        PIPELINE_STATE_TABLE,
    ):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    monkeypatch.setattr(snapshots, "build_session", lambda *a, **k: spark)
    monkeypatch.setattr(index_job, "build_session", lambda *a, **k: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    _ingest(spark, tmp_path / "p0.jsonl", [_row("A", ["B"]), _row("B", ["A"]), _row("C", ["A"])], 0)
    snap0 = current_snapshot(spark, DOCUMENTS_TABLE)
    _ingest(
        spark,
        tmp_path / "p1.jsonl",
        [json.dumps({"op": "DELETE", "doc_id": "B"})],
        1,
    )
    capsys.readouterr()
    return spark, snap0, capsys


def _run(capsys, argv):
    code = main(argv)
    lines = [l for l in capsys.readouterr().out.splitlines() if l.strip()]
    return code, [json.loads(l) for l in lines]


def _links(spark):
    return {(r.src_id, r.dst_id) for r in spark.table(LINKS_TABLE).collect()}


def test_list_shows_phase0_snapshot(env):
    spark, snap0, capsys = env
    code, rows = _run(capsys, ["list"])
    assert code == 0
    by_id = {r["snapshot_id"]: r for r in rows}
    assert by_id[snap0]["live_documents"] == 3
    assert by_id[snap0]["is_current"] is False
    assert rows[0]["is_current"] is True
    assert rows[0]["live_documents"] == 2
    assert set(rows[0]) == {
        "snapshot_id",
        "made_current_at",
        "operation",
        "is_current",
        "live_documents",
    }


def test_show_as_of_snapshot_and_now(env):
    spark, snap0, capsys = env
    _, rows = _run(capsys, ["show", "--snapshot", str(snap0), "--doc-id", "B"])
    assert rows[0]["found"] is True and rows[0]["is_deleted"] is False
    now = current_snapshot(spark, DOCUMENTS_TABLE)
    _, rows = _run(capsys, ["show", "--snapshot", str(now), "--doc-id", "B"])
    assert rows[0]["is_deleted"] is True
    _, rows = _run(capsys, ["show", "--snapshot", str(snap0), "--doc-id", "ZZ"])
    assert rows == [{"found": False}]


def test_rollback_restores_documents_and_links(env):
    spark, snap0, capsys = env
    assert _links(spark) == {("C", "A")}
    code, rows = _run(capsys, ["rollback", "--snapshot", str(snap0)])
    assert code == 0
    assert rows[-1] == {
        "status": "ok",
        "current_snapshot": snap0,
        "live_documents": 3,
        "link_count": 3,
    }
    assert current_snapshot(spark, DOCUMENTS_TABLE) == snap0
    assert _links(spark) == {("A", "B"), ("B", "A"), ("C", "A")}
    live = {r.doc_id for r in spark.table(DOCUMENTS_TABLE).where("NOT is_deleted").collect()}
    assert live == {"A", "B", "C"}


def test_unknown_snapshot_returns_one_and_changes_nothing(env):
    spark, snap0, capsys = env
    before = current_snapshot(spark, DOCUMENTS_TABLE)
    code, rows = _run(capsys, ["rollback", "--snapshot", "123456789"])
    assert code == 1
    assert rows[-1]["status"] == "error"
    assert current_snapshot(spark, DOCUMENTS_TABLE) == before
    assert spark.table(DOCUMENTS_TABLE).where("NOT is_deleted").count() == 2


def test_rollback_to_non_ancestor_returns_error_and_keeps_current(env):
    spark, snap0, capsys = env
    newest = current_snapshot(spark, DOCUMENTS_TABLE)
    assert newest is not None
    assert newest != snap0

    code, rows = _run(capsys, ["rollback", "--snapshot", str(snap0)])
    assert code == 0
    assert rows[-1]["status"] == "ok"
    assert current_snapshot(spark, DOCUMENTS_TABLE) == snap0

    code, rows = _run(capsys, ["rollback", "--snapshot", str(newest)])
    assert code == 1
    assert rows[-1]["status"] == "error"
    assert current_snapshot(spark, DOCUMENTS_TABLE) == snap0


def test_index_not_skipped_after_rollback(env):
    spark, snap0, capsys = env
    assert index_job.main([]) == 0
    assert last_snapshot(spark, "index") == current_snapshot(spark, DOCUMENTS_TABLE)
    capsys.readouterr()
    assert main(["rollback", "--snapshot", str(snap0)]) == 0
    assert last_snapshot(spark, "index") != current_snapshot(spark, DOCUMENTS_TABLE)


def test_show_finds_doc_id_with_apostrophe(spark, monkeypatch, tmp_path, capsys):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg_catalog.search")
    for table in (
        DOCUMENTS_TABLE,
        LINKS_TABLE,
        REJECTS_TABLE,
        INDEX_TABLE,
        DOC_STATS_TABLE,
        PIPELINE_STATE_TABLE,
    ):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    monkeypatch.setattr(snapshots, "build_session", lambda *a, **k: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    _ingest(
        spark,
        tmp_path / "apostrophe.jsonl",
        [
            json.dumps(
                {
                    "doc_id": "O'Brien",
                    "url": "http://x/o-brien",
                    "title": "t o-brien",
                    "text": "text o-brien",
                    "links": [],
                }
            )
        ],
        0,
    )
    snapshot = current_snapshot(spark, DOCUMENTS_TABLE)
    capsys.readouterr()
    code, rows = _run(capsys, ["show", "--snapshot", str(snapshot), "--doc-id", "O'Brien"])
    assert code == 0
    assert rows[0]["found"] is True
    assert rows[0]["doc_id"] == "O'Brien"
    assert rows[0]["url"] == "http://x/o-brien"
    assert rows[0]["title"] == "t o-brien"
    assert rows[0]["text"] == "text o-brien"
    assert rows[0]["links"] == []
    assert rows[0]["version"] == 0
    assert rows[0]["is_deleted"] is False
    assert rows[0]["updated_phase"] == 0
