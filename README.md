<div align="center">

# Distributed Search Engine

<p>
A four-node search engine for the Big Data 2026 project <em>Distributed Search Engine</em>. Spark ingests a Simple English Wikipedia corpus into Apache Iceberg tables on HDFS, builds an inverted index and PageRank, and exports a validated Parquet version that a FastAPI service loads into memory and ranks with BM25 and PageRank.
</p>

![Course](https://img.shields.io/badge/Course-Big%20Data%202026%20%7C%20UE24CS343AB2-1565C0?style=for-the-badge&labelColor=42A5F5)
![Storage](https://img.shields.io/badge/Storage-HDFS%203.3.6%20RF%202-2E7D32?style=for-the-badge&labelColor=66BB6A)
![Processing](https://img.shields.io/badge/Processing-Apache%20Spark%203.3.4-E65100?style=for-the-badge&labelColor=FF9800)
![Tables](https://img.shields.io/badge/Tables-Apache%20Iceberg%201.8.1-6A1B9A?style=for-the-badge&labelColor=AB47BC)
![Serving](https://img.shields.io/badge/Serving-FastAPI-00897B?style=for-the-badge&labelColor=26A69A)
![Network](https://img.shields.io/badge/Network-ZeroTier%20Private%20LAN-B8860B?style=for-the-badge&labelColor=FFD54F)
![Language](https://img.shields.io/badge/Language-Python%203.10-1C1C1C?style=for-the-badge&logo=python&logoColor=FFD700&labelColor=424242)
![Runtime](https://img.shields.io/badge/Runtime-Java%2011-B71C1C?style=for-the-badge&labelColor=EF5350)

<p>
  <a href="#overview">Overview</a> &bull;
  <a href="#architecture">Architecture</a> &bull;
  <a href="#pipeline">Pipeline</a> &bull;
  <a href="#ranking">Ranking</a> &bull;
  <a href="#verified-results">Results</a> &bull;
  <a href="#fault-tolerance">Fault tolerance</a> &bull;
  <a href="#repository-layout">Repository</a> &bull;
  <a href="#getting-started">Getting started</a> &bull;
  <a href="#design-decisions">Decisions</a>
</p>

</div>

> [!NOTE]
> Every stage is built and tested, and the full pipeline passes on one machine against real HDFS and a real Spark standalone cluster (`bash scripts/local_e2e.sh`). It has not yet been run across four separate machines. [RUNBOOK.md](RUNBOOK.md) has the steps for that.

## Overview

The corpus is about 1,000 Simple English Wikipedia pages. It arrives in two phases: a dirty base file in Phase 0, then a change set of deletes and upserts in Phase 1. Some rows are malformed and some are duplicates, and Phase 1 removes pages that other pages link to.

The system does the following:

- Validates records, rejects bad ones, and removes duplicate document IDs.
- Keeps versioned tables in Iceberg on HDFS, so any earlier state can be read or restored.
- Builds the inverted index and computes PageRank on Spark executors.
- Publishes each new search version only after it passes validation.
- Keeps serving the previous version when a later export fails.

## Architecture

| Node | Runs | Role |
|---|---|---|
| Node 1 | Spark driver, HDFS client | Holds the corpus, uploads it to HDFS, runs ingestion |
| Node 2 | NameNode, DataNode, Spark worker | Stores Iceberg data and metadata |
| Node 3 | Spark master, Spark worker, DataNode, Spark drivers | Runs indexing, PageRank and export |
| Node 4 | FastAPI service, HDFS client | Pulls exports, serves search and the frontend |

The HDFS replication factor is 2, across Nodes 2 and 3. Node 4 runs no Spark or HDFS daemon and never reads Iceberg.

```mermaid
flowchart LR
    C[Corpus on Node 1] -->|hdfs put| H[(HDFS)]
    H --> I[Ingestion job]
    I -->|merge| T[(Iceberg tables)]
    T --> X[Index job]
    T --> P[PageRank job]
    X --> E[Export job]
    P --> E
    E -->|validated Parquet vN| H
    H -->|pull and validate| S[Search service on Node 4]
    S --> U[Browser]
```

## Pipeline

Ingestion parses each line and records rejected lines with a reason. It keeps one row per document ID, then merges only the rows that changed into the `documents` table. Running the same phase again commits nothing, so the Iceberg snapshot stays the same.

Deletes are stored as tombstones. Re-ingesting Phase 0 after Phase 1 therefore cannot bring a deleted page back. The `links` table holds only edges whose source and destination are both live, and it is rebuilt after every change.

The index job tokenizes live documents on the executors and writes postings and document lengths. PageRank runs 20 iterations on RDDs with a checkpoint every 5 iterations. Both jobs skip their work when the documents snapshot has not moved.

The export job writes Parquet to a temporary directory, reads it back, validates it, writes a manifest, and renames the directory to `vN`. Validation checks the document count, nulls, unique IDs, orphan postings, document frequencies, and that PageRank sums to 1. A failure removes the temporary directory.

Node 4 copies the newest complete export, loads it, and only then switches the `current` symlink. A corrupt copy leaves `current` where it was, and `/reload` keeps the old index in memory if the new one cannot be loaded.

`scripts/node3_snapshots.sh` lists Iceberg versions, shows a page as of an earlier version, and rolls the `documents` table back after a bad update.
