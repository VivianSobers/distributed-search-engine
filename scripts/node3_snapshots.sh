#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib.sh
. "$SCRIPT_DIR/lib.sh"
render_cluster_conf

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 list | show --snapshot ID --doc-id X | rollback --snapshot ID" >&2
    exit 1
fi

package_app
submit_job "$NODE3_IP" dse.jobs.snapshots "$@"
