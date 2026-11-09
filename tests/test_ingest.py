import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from dse.config import DOCUMENTS_TABLE, LINKS_TABLE, REJECTS_TABLE
from dse.text import content_hash
from dse.jobs.ingest import (
    deduplicate,
    ensure_tables,
    merge_documents,
    read_phase,
    rebuild_links,
    record_rejects,
)


def _drop_ingest_tables(spark) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {LINKS_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {REJECTS_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {DOCUMENTS_TABLE}")


def _snapshot_id(spark):
    rows = spark.sql(
        f"SELECT snapshot_id FROM {DOCUMENTS_TABLE}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return rows[0][0] if rows else None


def _write_jsonl(path: Path, rows: list[str]) -> str:
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return str(path)


def _run_phase(spark, path: str, phase: int) -> dict[str, int | None]:
    ensure_tables(spark)
    accepted, rejected = read_phase(spark, path, phase)
    deduped = deduplicate(accepted)
    lines_accepted = accepted.count()
    deduped_count = deduped.count()
    rejected_count = rejected.count()
    record_rejects(spark, rejected, phase)
    changed = merge_documents(spark, deduped, phase)
    link_count = rebuild_links(spark)
    live_docs = (
        spark.table(DOCUMENTS_TABLE)
        .where(~F.col("is_deleted"))
        .select("doc_id")
        .count()
    )
    return {
        "lines_accepted": lines_accepted,
        "rejected": rejected_count,
        "duplicates_removed": lines_accepted - deduped_count,
        "documents_changed": changed,
        "live_documents": live_docs,
        "link_count": link_count,
        "documents_snapshot": _snapshot_id(spark),
    }


def test_toy_phase0_live_docs_and_rejects(spark, tmp_path: Path):
    _drop_ingest_tables(spark)
    ensure_tables(spark)

    phase0 = _write_jsonl(
        tmp_path / "phase0.jsonl",
        [
            json.dumps(
                {
                    "doc_id": "1",
                    "url": "https://ex/1",
                    "title": "One",
                    "text": "Plants need water.",
                    "links": ["2", "3"],
                }
            ),
            json.dumps(
                {
                    "doc_id": "2",
                    "url": "https://ex/2",
                    "title": "Two",
                    "text": "Soil supports a plant.",
                    "links": ["1", "9"],
                }
            ),
            "{",
            json.dumps({"doc_id": "x", "url": "https://ex/x", "text": "missing title"}),
            json.dumps(
                {
                    "doc_id": "3",
                    "url": "https://ex/3",
                    "title": "Three",
                    "text": "Sunlight helps plants.",
                    "links": ["2"],
                }
            ),
            json.dumps(
                {
                    "doc_id": "2",
                    "url": "https://ex/2",
                    "title": "Two",
                    "text": "WRONG duplicate text",
                    "links": ["1"],
                }
            ),
            json.dumps(
                {
                    "doc_id": "4",
                    "url": "https://ex/4",
                    "title": "Four",
                    "text": "Roots absorb water.",
                    "links": [],
                }
            ),
        ],
    )

    summary = _run_phase(spark, phase0, 0)

    live = {
        row["doc_id"]
        for row in spark.table(DOCUMENTS_TABLE).where(~F.col("is_deleted")).select("doc_id").collect()
    }
    assert live == {"1", "2", "3", "4"}
    assert summary["rejected"] == 2

    doc2 = spark.table(DOCUMENTS_TABLE).where(F.col("doc_id") == "2").select("text").collect()[0][0]
    assert doc2 == "Soil supports a plant."


def test_toy_phase1_tombstone_update_and_live_links(spark, tmp_path: Path):
    _drop_ingest_tables(spark)

    phase0 = _write_jsonl(
        tmp_path / "phase0.jsonl",
        [
            json.dumps(
                {
                    "doc_id": "1",
                    "url": "https://ex/1",
                    "title": "One",
                    "text": "Plants need water.",
                    "links": ["2", "3"],
                }
            ),
            json.dumps(
                {
                    "doc_id": "2",
                    "url": "https://ex/2",
                    "title": "Two",
                    "text": "Soil supports a plant.",
                    "links": ["1", "9"],
                }
            ),
            json.dumps(
                {
                    "doc_id": "3",
                    "url": "https://ex/3",
                    "title": "Three",
                    "text": "Sunlight helps plants.",
                    "links": ["2"],
                }
            ),
            json.dumps(
                {
                    "doc_id": "4",
                    "url": "https://ex/4",
                    "title": "Four",
                    "text": "Roots absorb water.",
                    "links": [],
                }
            ),
        ],
    )
    phase1 = _write_jsonl(
        tmp_path / "phase1.jsonl",
        [
            json.dumps({"op": "DELETE", "doc_id": "2"}),
            json.dumps(
                {
                    "doc_id": "3",
                    "url": "https://ex/3",
                    "title": "Three",
                    "text": "Sunlight powers plants.",
                    "links": ["1", "2"],
                }
            ),
            json.dumps(
                {
                    "doc_id": "5",
                    "url": "https://ex/5",
                    "title": "Five",
                    "text": "Leaves capture light.",
                    "links": ["1", "9"],
                }
            ),
        ],
    )

    _run_phase(spark, phase0, 0)
    _run_phase(spark, phase1, 1)

    live = {
        row["doc_id"]
        for row in spark.table(DOCUMENTS_TABLE).where(~F.col("is_deleted")).select("doc_id").collect()
    }
    assert live == {"1", "3", "4", "5"}

    doc2 = (
        spark.table(DOCUMENTS_TABLE)
        .where(F.col("doc_id") == "2")
        .select("is_deleted", "url", "title", "text")
        .collect()[0]
    )
    assert doc2["is_deleted"] is True
    assert doc2["url"] == ""
    assert doc2["title"] == ""
    assert doc2["text"] == ""

    doc3_text = spark.table(DOCUMENTS_TABLE).where(F.col("doc_id") == "3").select("text").collect()[0][0]
    assert doc3_text == "Sunlight powers plants."

    bad_links = (
        spark.table(LINKS_TABLE)
        .where((F.col("src_id") == "2") | (F.col("dst_id") == "2"))
        .count()
    )
    assert bad_links == 0


def test_same_phase_twice_keeps_rows_and_snapshot(spark, tmp_path: Path):
    _drop_ingest_tables(spark)

    phase0 = _write_jsonl(
        tmp_path / "phase0.jsonl",
        [
            json.dumps(
                {
                    "doc_id": "1",
                    "url": "https://ex/1",
                    "title": "One",
                    "text": "Plants need water.",
                    "links": ["2", "3"],
                }
            ),
            json.dumps(
                {
                    "doc_id": "2",
                    "url": "https://ex/2",
                    "title": "Two",
                    "text": "Soil supports a plant.",
                    "links": ["1"],
                }
            ),
        ],
    )

    _run_phase(spark, phase0, 0)
    docs_before = spark.table(DOCUMENTS_TABLE).orderBy("doc_id").collect()
    links_before = spark.table(LINKS_TABLE).orderBy("src_id", "dst_id").collect()
    rejects_before = spark.table(REJECTS_TABLE).orderBy("phase", "line_no").collect()
    snapshot_before = _snapshot_id(spark)

    _run_phase(spark, phase0, 0)
    docs_after = spark.table(DOCUMENTS_TABLE).orderBy("doc_id").collect()
    links_after = spark.table(LINKS_TABLE).orderBy("src_id", "dst_id").collect()
    rejects_after = spark.table(REJECTS_TABLE).orderBy("phase", "line_no").collect()
    snapshot_after = _snapshot_id(spark)

    assert docs_after == docs_before
    assert links_after == links_before
    assert rejects_after == rejects_before
    assert snapshot_after == snapshot_before


def test_reingest_phase0_after_phase1_keeps_tombstone(spark, tmp_path: Path):
    _drop_ingest_tables(spark)

    phase0 = _write_jsonl(
        tmp_path / "phase0.jsonl",
        [
            json.dumps(
                {
                    "doc_id": "2",
                    "url": "https://ex/2",
                    "title": "Two",
                    "text": "Soil supports a plant.",
                    "links": ["1"],
                }
            ),
            json.dumps(
                {
                    "doc_id": "1",
                    "url": "https://ex/1",
                    "title": "One",
                    "text": "Plants need water.",
                    "links": [],
                }
            ),
        ],
    )
    phase1 = _write_jsonl(tmp_path / "phase1.jsonl", [json.dumps({"op": "DELETE", "doc_id": "2"})])

    _run_phase(spark, phase0, 0)
    _run_phase(spark, phase1, 1)
    docs_before = spark.table(DOCUMENTS_TABLE).orderBy("doc_id").collect()
    links_before = spark.table(LINKS_TABLE).orderBy("src_id", "dst_id").collect()
    snapshot_before = _snapshot_id(spark)

    _run_phase(spark, phase0, 0)

    docs_after = spark.table(DOCUMENTS_TABLE).orderBy("doc_id").collect()
    links_after = spark.table(LINKS_TABLE).orderBy("src_id", "dst_id").collect()
    snapshot_after = _snapshot_id(spark)

    assert docs_after == docs_before
    assert links_after == links_before
    assert snapshot_after == snapshot_before

    doc2_deleted = (
        spark.table(DOCUMENTS_TABLE)
        .where(F.col("doc_id") == "2")
        .select("is_deleted")
        .collect()[0][0]
    )
    assert doc2_deleted is True
