from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dse.config import DOC_STATS_TABLE, DOCUMENTS_TABLE, INDEX_TABLE, PAGERANK_TABLE
from dse.jobs.state import current_snapshot, last_snapshot
from dse.spark_session import build_session


def validate_export(docs: DataFrame, index: DataFrame, expected_live: int) -> list[str]:
    errors: list[str] = []

    docs_count = docs.count()
    if docs_count != expected_live:
        errors.append("docs count mismatch")
    if docs_count <= 0:
        errors.append("docs count must be > 0")

    if docs.where(F.col("doc_id").isNull()).limit(1).count() > 0:
        errors.append("docs has null doc_id")
    if docs.where(F.col("url").isNull()).limit(1).count() > 0:
        errors.append("docs has null url")
    if docs.where(F.col("title").isNull()).limit(1).count() > 0:
        errors.append("docs has null title")
    if docs.where(F.col("length").isNull()).limit(1).count() > 0:
        errors.append("docs has null length")

    distinct_doc_ids = docs.select("doc_id").distinct().count()
    if distinct_doc_ids != docs_count:
        errors.append("docs doc_id values are not unique")

    orphan_postings = index.join(
        docs.select("doc_id").distinct(),
        on="doc_id",
        how="left_anti",
    ).limit(1)
    if orphan_postings.count() > 0:
        errors.append("index has doc_id values missing in docs")

    if docs.where(F.col("pagerank").isNull()).limit(1).count() > 0:
        errors.append("docs has null pagerank")

    pagerank_sum = docs.select(F.sum("pagerank").alias("rank_sum")).first()["rank_sum"]
    if pagerank_sum is None or abs(float(pagerank_sum) - 1.0) > 1e-6:
        errors.append("pagerank sum must be 1 within 1e-6")

    term_df_check = index.groupBy("term").agg(
        F.count(F.lit(1)).alias("posting_count"),
        F.min("df").alias("min_df"),
        F.max("df").alias("max_df"),
        F.sum(F.when(F.col("df").isNull(), F.lit(1)).otherwise(F.lit(0))).alias(
            "null_df_count"
        ),
    )
    bad_df = term_df_check.where(
        (F.col("null_df_count") > 0)
        | (F.col("min_df") != F.col("posting_count"))
        | (F.col("max_df") != F.col("posting_count"))
    ).limit(1)
    if bad_df.count() > 0:
        errors.append("index df does not match posting count for term")

    return errors


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export live search index snapshot")
    parser.add_argument("--export-root", type=str, required=True)
    parser.add_argument("--simulate-failure", action="store_true")
    return parser.parse_args(argv)


def _path(jvm, *parts):
    Path = jvm.org.apache.hadoop.fs.Path
    current = Path(parts[0])
    for part in parts[1:]:
        current = Path(current, part)
    return current


def _list_versions(fs, root_path) -> list[int]:
    versions: list[int] = []
    if not fs.exists(root_path):
        return versions

    matcher = re.compile(r"^v(\d+)$")
    for status in fs.listStatus(root_path):
        if not status.isDirectory():
            continue
        match = matcher.match(status.getPath().getName())
        if match:
            versions.append(int(match.group(1)))
    versions.sort()
    return versions


def _write_manifest(fs, jvm, manifest_path, payload: dict) -> None:
    stream = fs.create(manifest_path, True)
    try:
        stream.write(bytearray(json.dumps(payload).encode("utf-8")))
    finally:
        stream.close()


def _load_manifest_snapshot(spark, manifest_uri: str) -> int | None:
    rows = spark.read.json(manifest_uri).select("documents_snapshot").collect()
    if not rows:
        return None
    value = rows[0]["documents_snapshot"]
    if value is None:
        return None
    return int(value)


def _build_export_frames(spark) -> tuple[DataFrame, DataFrame, int]:
    live_docs = spark.table(DOCUMENTS_TABLE).where(~F.col("is_deleted")).select(
        "doc_id", "url", "title"
    )
    expected_live = live_docs.count()

    stats = spark.table(DOC_STATS_TABLE).select("doc_id", F.col("length").cast("int").alias("length"))
    pagerank = spark.table(PAGERANK_TABLE).select(
        "doc_id", F.col("rank").cast("double").alias("pagerank")
    )
    docs = (
        live_docs.join(stats, on="doc_id", how="left")
        .join(pagerank, on="doc_id", how="left")
        .select("doc_id", "url", "title", "length", "pagerank")
    )

    index = spark.table(INDEX_TABLE).select(
        "term",
        "doc_id",
        F.col("tf").cast("int").alias("tf"),
        F.col("df").cast("int").alias("df"),
    )
    return docs, index, expected_live


def _run_export(spark, export_root: str, simulate_failure: bool) -> tuple[int, dict]:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()

    documents_snapshot = current_snapshot(spark, DOCUMENTS_TABLE)
    index_snapshot = last_snapshot(spark, "index")
    if index_snapshot != documents_snapshot:
        return 1, {"status": "error", "errors": ["index built from a different snapshot"]}
    pagerank_snapshot = last_snapshot(spark, "pagerank")
    if pagerank_snapshot != documents_snapshot:
        return 1, {"status": "error", "errors": ["pagerank built from a different snapshot"]}

    root_path = _path(jvm, export_root)
    fs = root_path.getFileSystem(conf)
    fs.mkdirs(root_path)

    versions = _list_versions(fs, root_path)
    newest = versions[-1] if versions else 0

    if newest > 0:
        latest_manifest_path = _path(jvm, root_path.toString(), f"v{newest}", "manifest.json")
        latest_snapshot = _load_manifest_snapshot(spark, latest_manifest_path.toString())
        if (
            latest_snapshot is not None
            and documents_snapshot is not None
            and int(latest_snapshot) == int(documents_snapshot)
        ):
            return 0, {"status": "skipped"}

    version = newest + 1
    tmp_path = _path(jvm, root_path.toString(), f".tmp-v{version}")
    final_path = _path(jvm, root_path.toString(), f"v{version}")
    docs_path = _path(jvm, tmp_path.toString(), "docs")
    index_path = _path(jvm, tmp_path.toString(), "index")
    manifest_path = _path(jvm, tmp_path.toString(), "manifest.json")

    if fs.exists(tmp_path):
        fs.delete(tmp_path, True)

    try:
        docs, index, expected_live = _build_export_frames(spark)
        docs.write.mode("overwrite").parquet(docs_path.toString())
        index.write.mode("overwrite").parquet(index_path.toString())

        exported_docs = spark.read.parquet(docs_path.toString()).select(
            "doc_id", "url", "title", "length", "pagerank"
        )
        exported_index = spark.read.parquet(index_path.toString()).select("term", "doc_id", "tf", "df")

        errors = validate_export(exported_docs, exported_index, expected_live)
        if simulate_failure:
            errors.append("simulated failure")
        if errors:
            return 1, {"status": "error", "errors": errors}

        docs_count = exported_docs.count()
        postings_count = exported_index.count()
        avgdl_value = exported_docs.select(F.avg("length").alias("avgdl")).first()["avgdl"]
        max_pr = exported_docs.select(F.max("pagerank").alias("max_pr")).first()["max_pr"]

        manifest = {
            "version": int(version),
            "documents_snapshot": int(documents_snapshot) if documents_snapshot is not None else -1,
            "docs": int(docs_count),
            "postings": int(postings_count),
            "avgdl": float(max(1.0, float(avgdl_value) if avgdl_value is not None else 0.0)),
            "max_pagerank": float(max_pr) if max_pr is not None else 0.0,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_manifest(fs, jvm, manifest_path, manifest)

        if fs.exists(final_path):
            return 1, {"status": "error", "errors": [f"destination exists: {final_path.toString()}"]}
        if not fs.rename(tmp_path, final_path):
            return 1, {"status": "error", "errors": [f"failed to rename to {final_path.toString()}"]}

        return 0, {"status": "ok", "version": int(version)}
    except Exception as exc:
        return 1, {"status": "error", "errors": [str(exc)]}
    finally:
        if fs.exists(tmp_path):
            fs.delete(tmp_path, True)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    spark = build_session("export-job")
    try:
        code, payload = _run_export(spark, args.export_root, args.simulate_failure)
        print(json.dumps(payload))
        return code
    finally:
        spark.stop()
