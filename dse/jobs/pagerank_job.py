import argparse
import json
from operator import add

from pyspark.sql import DataFrame
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel

from dse.config import (
    DOCUMENTS_TABLE,
    LINKS_TABLE,
    PAGERANK_DAMPING,
    PAGERANK_DEFAULT_ITERATIONS,
    PAGERANK_MIN_ITERATIONS,
    PAGERANK_TABLE,
)
from dse.jobs.state import current_snapshot, last_snapshot, record_snapshot
from dse.spark_session import build_session


def load_graph(spark) -> tuple[DataFrame, DataFrame]:
    nodes = spark.sql(
        f"SELECT doc_id FROM {DOCUMENTS_TABLE} WHERE is_deleted = false"
    )
    edges = spark.sql(f"SELECT src_id, dst_id FROM {LINKS_TABLE}")
    return nodes, edges


def compute_pagerank(
    spark,
    nodes,
    edges,
    iterations: int = PAGERANK_DEFAULT_ITERATIONS,
    damping: float = PAGERANK_DAMPING,
    checkpoint_every: int = 5,
):
    if iterations < PAGERANK_MIN_ITERATIONS:
        raise ValueError(
            f"iterations must be >= {PAGERANK_MIN_ITERATIONS}, got {iterations}"
        )

    schema = T.StructType(
        [
            T.StructField("doc_id", T.StringType()),
            T.StructField("rank", T.DoubleType()),
        ]
    )

    sc = spark.sparkContext
    partitions = max(2, sc.defaultParallelism)
    node_df = nodes.select("doc_id").dropna(subset=["doc_id"]).distinct()
    n = node_df.count()
    if n == 0:
        return spark.createDataFrame([], schema=schema)

    node_rdd = (
        node_df.rdd.map(lambda row: (row["doc_id"], None))
        .partitionBy(partitions)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    src_nodes = node_df.withColumnRenamed("doc_id", "src_id")
    dst_nodes = node_df.withColumnRenamed("doc_id", "dst_id")
    valid_edges = (
        edges.select("src_id", "dst_id")
        .dropna(subset=["src_id", "dst_id"])
        .join(src_nodes, on="src_id", how="inner")
        .join(dst_nodes, on="dst_id", how="inner")
        .select("src_id", "dst_id")
        .distinct()
    )
    links = (
        valid_edges.rdd.map(lambda row: (row["src_id"], row["dst_id"]))
        .groupByKey(numPartitions=partitions)
        .mapValues(list)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    initial_rank = 1.0 / float(n)
    ranks = (
        node_rdd.mapValues(lambda _: initial_rank)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    has_checkpoint_dir = sc.getCheckpointDir() is not None
    teleport = (1.0 - damping) / float(n)
    damping_value = float(damping)
    for idx in range(1, iterations + 1):
        dangling_mass = ranks.subtractByKey(links, numPartitions=partitions).values().sum()
        contribs = links.join(ranks, numPartitions=partitions).flatMap(
            lambda item: [
                (dst_id, item[1][1] / len(item[1][0]))
                for dst_id in item[1][0]
            ]
        )

        dangling_share = dangling_mass / float(n)
        new_ranks = (
            node_rdd.mapValues(lambda _: 0.0)
            .union(contribs)
            .reduceByKey(add, numPartitions=partitions)
            .mapValues(lambda value: teleport + damping_value * (value + dangling_share))
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        if checkpoint_every > 0 and idx % checkpoint_every == 0:
            if has_checkpoint_dir:
                new_ranks.checkpoint()
            else:
                new_ranks.localCheckpoint()

        new_ranks.count()
        ranks.unpersist()
        ranks = new_ranks

    result = spark.createDataFrame(
        ranks.map(lambda kv: (kv[0], float(kv[1]))),
        schema=schema,
    )
    links.unpersist()
    node_rdd.unpersist()
    return result


def _parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Compute PageRank from current corpus")
    parser.add_argument("--iterations", type=int, default=PAGERANK_DEFAULT_ITERATIONS)
    parser.add_argument("--checkpoint-dir", type=str, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    spark = build_session("pagerank-job")
    try:
        spark.sparkContext.setCheckpointDir(args.checkpoint_dir)

        documents_snapshot = current_snapshot(spark, DOCUMENTS_TABLE)
        previous_snapshot = last_snapshot(spark, "pagerank")
        if (
            documents_snapshot is not None
            and previous_snapshot is not None
            and previous_snapshot == documents_snapshot
        ):
            print(json.dumps({"status": "skipped"}))
            return 0

        nodes, edges = load_graph(spark)
        ranks = compute_pagerank(
            spark=spark,
            nodes=nodes,
            edges=edges,
            iterations=args.iterations,
        )

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {PAGERANK_TABLE} (
                doc_id STRING,
                rank DOUBLE
            ) USING iceberg
            """
        )
        ranks.createOrReplaceTempView("pagerank_result")
        spark.sql(
            f"INSERT OVERWRITE {PAGERANK_TABLE} SELECT doc_id, rank FROM pagerank_result"
        )

        if documents_snapshot is not None:
            record_snapshot(spark, "pagerank", documents_snapshot)

        print(
            json.dumps(
                {
                    "status": "ok",
                    "rows": ranks.count(),
                    "source_snapshot": documents_snapshot,
                    "iterations": args.iterations,
                }
            )
        )
        return 0
    finally:
        spark.stop()
