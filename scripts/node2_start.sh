#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib.sh
. "$SCRIPT_DIR/lib.sh"
render_cluster_conf
export SPARK_LOCAL_IP="$NODE2_IP"

if [ ! -e "$BD_HOME/hdfs/name/current" ]; then
    hdfs namenode -format -nonInteractive
fi

hdfs --daemon start namenode
hdfs --daemon start datanode
start-worker.sh "spark://$NODE3_IP:$SPARK_MASTER_PORT"
