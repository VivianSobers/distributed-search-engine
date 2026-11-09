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

## Ranking

Parameters come from the project specification: BM25 with k1 = 1.2 and b = 0.75, PageRank damping of 0.85, and at least 10 iterations.

```text
IDF(t)      = ln(1 + (N - df + 0.5) / (df + 0.5))
BM25(D, Q)  = sum over t in Q of IDF(t) * tf * (k1 + 1) / (tf + k1 * (1 - b + b * len(D) / avgdl))
Final score = 0.7 * (BM25 / max BM25) + 0.3 * (PageRank / max PageRank)
```

The BM25 maximum is taken over the results of the current query, and the PageRank maximum over the whole served corpus. A page with no incoming links gets a low PageRank, so it can rank below a page with a slightly lower BM25 score. The page titled April ranks 37th for the query `april` for this reason.

## Verified results

These figures come from `scripts/local_e2e.sh` on the full corpus. The PageRank scores, posting count, average document length, and the top five results for `april` also matched a separate plain Python calculation from the raw files.

| Stage | Lines accepted | Rejected | Duplicates removed | Live pages | Links | Postings | Search version |
|---|---|---|---|---|---|---|---|
| Phase 0 | 1,020 | 20 | 20 | 1,000 | 499 | 453,438 | v1 |
| Phase 1 | 100 | 0 | 0 | 975 | 467 | 427,998 | v2 |
| Rollback to Phase 0 | n/a | n/a | n/a | 1,000 | 499 | 453,438 | v3 |

Re-ingesting Phase 0 changes no rows and leaves the documents snapshot ID unchanged.

## Fault tolerance

`scripts/fault_tolerance_check.sh` runs PageRank once undisturbed. It runs it again, and during that second run sends `kill -9` to one Spark worker and its executors while the job is running. The check requires the job's log to show the lost executor, requires the job to finish, and requires the ranks to match the undisturbed run to within 1e-9.

RDD lineage and HDFS checkpoints are what let the job recover. Spark reschedules the lost tasks on the remaining worker.

The end-to-end run also forces an export failure with `--simulate-failure`. Search stays on version 1, and no `v2` or temporary directory is left behind. The next successful export becomes version 2.

## Repository layout

```text
config/cluster.env        node addresses and ports
conf/                     Iceberg catalog template
deploy/                   cluster config rendering and the Spark job launcher
dse/records.py            record parsing and validation
dse/text.py               tokenizer and content hash
dse/ranking.py            BM25 and score combination
dse/jobs/                 ingest, index, PageRank, export, snapshots, fault check
dse/serving/              search index, API, frontend, export pull
scripts/install_deps.sh   installs Hadoop, Spark, Iceberg and the Python environment
scripts/node*.sh          one script per node role
scripts/local_e2e.sh      full pipeline on one machine
tests/                    unit and Spark tests
```

## Getting started

You need Java 11 and Python 3.10 on every machine. Installation is unattended and needs no root access:

```bash
bash scripts/install_deps.sh
source scripts/env.sh
```

Everything installs into `~/.local/opt/bd-search-engine`. To override that location, set `BD_HOME`.

The corpus is not in this repository. Download it from the course page into `data/full/phase-0/pages.jsonl` and `data/full/phase-1/changes.json`.

Run the unit and Spark tests:

```bash
source scripts/env.sh
pytest -q
```

Run the whole system on one machine. This takes about 9 minutes:

```bash
bash scripts/local_e2e.sh
```

The script prints `E2E PASSED` at the end. Use `--keep` to leave the cluster running afterwards.

## Design decisions

The specification leaves several rules open. These are the choices made here, based on the data:

| Question | Decision | Reason |
|---|---|---|
| Required fields | `doc_id`, `url`, `title` and `text` for an upsert; only `doc_id` for a delete | Phase 1 deletes contain only `doc_id` and `op` |
| Missing `links` | Treated as empty | 40 corpus rows omit it, and requiring it would drop 20 pages |
| Meaning of "version" | A `version` field if present, otherwise the phase number. The earliest line wins a tie | The corpus has no version field, and the toy data labels its later row as a duplicate |
| Deletes | Stored as tombstones | Re-ingesting an earlier phase cannot restore a deleted page |
| Pages with no outgoing links | Their rank is shared evenly across all pages | Ranks stay a probability distribution that sums to 1 |
| Index updates | Rebuilt from the current snapshot and skipped when it has not moved | A single atomic rebuild is simple and correct at this corpus size |
