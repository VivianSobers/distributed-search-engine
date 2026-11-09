#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib.sh
. "$SCRIPT_DIR/lib.sh"
render_cluster_conf

package_app

submit_job "$NODE3_IP" dse.jobs.index_job
submit_job "$NODE3_IP" dse.jobs.pagerank_job --checkpoint-dir "hdfs://$NODE2_IP:$NAMENODE_PORT$CHECKPOINT_ROOT/pagerank"
submit_job "$NODE3_IP" dse.jobs.export_job --export-root "hdfs://$NODE2_IP:$NAMENODE_PORT$EXPORT_ROOT"
