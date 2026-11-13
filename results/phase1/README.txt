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
