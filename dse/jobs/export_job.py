from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from typing import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dse.config import DOC_STATS_TABLE, DOCUMENTS_TABLE, INDEX_TABLE, PAGERANK_TABLE
from dse.jobs.state import current_snapshot
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
