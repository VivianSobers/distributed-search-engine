#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib.sh
. "$SCRIPT_DIR/lib.sh"
render_cluster_conf

if [ "$#" -ne 1 ] || { [ "$1" != "0" ] && [ "$1" != "1" ]; }; then
    echo "Usage: $0 <0|1>" >&2
    exit 1
fi

PHASE="$1"

if [ "$PHASE" = "0" ]; then
    LOCAL_FILE="$REPO_ROOT/data/full/phase-0/pages.jsonl"
    FILENAME="pages.jsonl"
else
    LOCAL_FILE="$REPO_ROOT/data/full/phase-1/changes.json"
    FILENAME="changes.json"
fi

hdfs dfs -mkdir -p "$RAW_ROOT/phase-$PHASE/"
hdfs dfs -put -f "$LOCAL_FILE" "$RAW_ROOT/phase-$PHASE/"

package_app
submit_job "$NODE1_IP" dse.jobs.ingest --phase "$PHASE" --input "hdfs://$NODE2_IP:$NAMENODE_PORT$RAW_ROOT/phase-$PHASE/$FILENAME"
