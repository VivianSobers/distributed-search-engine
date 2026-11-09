#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib.sh
. "$SCRIPT_DIR/lib.sh"
render_cluster_conf

SERVING_ROOT="$SERVING_ROOT" uvicorn --factory dse.serving.asgi:make_app --host 0.0.0.0 --port "$SEARCH_PORT" "$@"
