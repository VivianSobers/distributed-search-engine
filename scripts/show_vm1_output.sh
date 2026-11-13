#!/usr/bin/env bash
# Prints the saved console output of a VM1 ingest run, exactly as it was
# captured, under a header that says where and when it actually ran.
# Usage: bash show_vm1_output.sh [phase0_ingest.log | phase1_ingest.log]
set -euo pipefail

LOG="${1:-$(dirname "$0")/../results/phase1/phase1_ingest.log}"
if [ ! -f "$LOG" ]; then
    echo "Log not found: $LOG" >&2
    exit 1
fi

echo "=== Saved output of node1_ingest.sh, run on VM1 (standby server, ZeroTier 192.168.193.214) ==="
echo "=== Log file: $(basename "$LOG") (run details in $(dirname "$LOG")/README.txt) ==="
echo
cat "$LOG"
