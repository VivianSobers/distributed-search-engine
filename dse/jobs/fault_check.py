from __future__ import annotations

import argparse
import json
from typing import Sequence

from pyspark.sql import functions as F

from dse.config import (
    DOCUMENTS_TABLE,
    LINKS_TABLE,
    PAGERANK_DEFAULT_ITERATIONS,
)
from dse.jobs.pagerank_job import compute_pagerank
from dse.spark_session import build_session


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic PageRank fault-tolerance check")
    parser.add_argument("--iterations", type=int, default=PAGERANK_DEFAULT_ITERATIONS)
    parser.add_argument("--checkpoint-dir", type=str, required=True)
    parser.add_argument("--output", type=str, required=True)
    parser.add_argument("--compare-to", type=str, default=None)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    spark = build_session("fault-check")
    try:
        spark.sparkContext.setCheckpointDir(args.checkpoint_dir)

        nodes = spark.sql(
            f"SELECT doc_id FROM {DOCUMENTS_TABLE} WHERE is_deleted = false"
        )
        edges = spark.sql(f"SELECT src_id, dst_id FROM {LINKS_TABLE}")
        ranks = compute_pagerank(
            spark=spark,
            nodes=nodes,
            edges=edges,
            iterations=args.iterations,
        ).select("doc_id", F.col("rank").cast("double").alias("rank"))

        ranks.write.mode("overwrite").parquet(args.output)
        docs = ranks.count()

        max_abs_diff = 0.0
        ok = True
        if args.compare_to:
            expected = spark.read.parquet(args.compare_to).select(
                "doc_id", F.col("rank").cast("double").alias("rank")
            )
            joined = ranks.alias("actual").join(expected.alias("expected"), on="doc_id", how="full_outer")
            missing = joined.where(
                F.col("actual.rank").isNull() | F.col("expected.rank").isNull()
            ).limit(1).count() > 0
            max_abs_diff_value = joined.select(
                F.max(
                    F.abs(
                        F.coalesce(F.col("actual.rank"), F.lit(0.0))
                        - F.coalesce(F.col("expected.rank"), F.lit(0.0))
                    )
                ).alias("max_abs_diff")
            ).first()["max_abs_diff"]
            max_abs_diff = float(max_abs_diff_value) if max_abs_diff_value is not None else 0.0
            ok = (not missing) and max_abs_diff <= 1e-9

        print(json.dumps({"docs": int(docs), "max_abs_diff": max_abs_diff, "ok": ok}))
        return 0 if ok else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
