Phase 1 outputs from VM1 (ingestion node)
Run on: standby server, ZeroTier 192.168.193.214
When:   2026-11-13, 14:16-15:05 IST
Command: bash scripts/node1_ingest.sh 1   (repo commit 0937ec3)

phase1_ingest.log          full console output of the run
phase1_ingest_result.json  final result line:
  lines_accepted 100, rejected 0, duplicates_removed 0,
  documents_changed 100, live_documents 975, link_count 467,
  documents_snapshot 2152842344036546620

To print the log:  bash scripts/show_vm1_output.sh results/phase1/phase1_ingest.log

Phase 1 processing (VM3: index, PageRank, export)
Run on: VM3, ZeroTier 192.168.193.217
When:   2026-11-13, 15:07-15:24 IST
Command: bash scripts/node3_process.sh   (repo commit 0937ec3)
phase1_process.log result lines:
  index:    status ok, postings 427998, documents 975
  pagerank: status ok, rows 975, iterations 20
  export:   status ok, version 2
  all built from documents snapshot 2152842344036546620
