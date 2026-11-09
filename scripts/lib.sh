#!/usr/bin/env bash
# scripts/lib.sh: Common environment setup and cluster helper functions.
# Meant to be sourced from bash.

# Resolve the repo root even when its path has spaces
_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$_LIB_DIR/.." && pwd)"

# Source scripts/env.sh
# shellcheck source=scripts/env.sh
. "$REPO_ROOT/scripts/env.sh"

# Read config/cluster.env (defaults populated if not already in environment)
_CLUSTER_ENV_FILE="${CLUSTER_ENV:-$REPO_ROOT/config/cluster.env}"
if [ -f "$_CLUSTER_ENV_FILE" ]; then
    while IFS='=' read -r key val || [ -n "$key" ]; do
        key="${key#"${key%%[![:space:]]*}"}"
        key="${key%"${key##*[![:space:]]}"}"
        [[ -z "$key" || "$key" =~ ^# ]] && continue
        val="${val#"${val%%[![:space:]]*}"}"
        val="${val%"${val##*[![:space:]]}"}"
        if [ -z "${!key:-}" ]; then
            export "$key=$val"
        fi
    done < "$_CLUSTER_ENV_FILE"
fi

# Export Hadoop and Spark configuration directories
export HADOOP_CONF_DIR="$BD_HOME/cluster-conf/hadoop"
export SPARK_CONF_DIR="$BD_HOME/cluster-conf/spark"

render_cluster_conf() {
    python "$REPO_ROOT/deploy/render_configs.py" --env "${CLUSTER_ENV:-$REPO_ROOT/config/cluster.env}" --out "$BD_HOME/cluster-conf" "$@"
    if [ -f "$HADOOP_HOME/etc/hadoop/log4j.properties" ]; then
        cp -f "$HADOOP_HOME/etc/hadoop/log4j.properties" "$BD_HOME/cluster-conf/hadoop/"
    fi
}

package_app() {
    mkdir -p "$BD_HOME/app"
    python -c '
import shutil
import sys
import zipfile
from pathlib import Path

repo = Path(sys.argv[1])
app_dir = Path(sys.argv[2])
zip_path = app_dir / "dse.zip"
with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for p in sorted((repo / "dse").rglob("*")):
        if "__pycache__" not in p.parts and not p.name.endswith(".pyc") and p.is_file():
            zf.write(p, p.relative_to(repo))
shutil.copy2(repo / "deploy" / "run_job.py", app_dir / "run_job.py")
' "$REPO_ROOT" "$BD_HOME/app"
}

submit_job() {
    local node_ip="$1"
    local module="$2"
    shift 2
    if [ ! -f "$BD_HOME/app/dse.zip" ] || [ ! -f "$BD_HOME/app/run_job.py" ]; then
        package_app
    fi
    spark-submit \
        --deploy-mode client \
        --conf "spark.driver.host=$node_ip" \
        --py-files "$BD_HOME/app/dse.zip" \
        "$BD_HOME/app/run_job.py" \
        "$module" \
        "$@"
}
