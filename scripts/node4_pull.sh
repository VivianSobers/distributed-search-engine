#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib.sh
. "$SCRIPT_DIR/lib.sh"
render_cluster_conf

python -m dse.serving.pull \
    --export-root "hdfs://$NODE2_IP:$NAMENODE_PORT$EXPORT_ROOT" \
    --serving-root "$SERVING_ROOT" \
    --reload-url "http://127.0.0.1:$SEARCH_PORT/reload" \
    "$@"
