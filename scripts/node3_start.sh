#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib.sh
. "$SCRIPT_DIR/lib.sh"
render_cluster_conf
export SPARK_LOCAL_IP="$NODE3_IP"

hdfs --daemon start datanode
start-master.sh
start-worker.sh "spark://$NODE3_IP:$SPARK_MASTER_PORT"
