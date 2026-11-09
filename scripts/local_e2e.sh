#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_ROOT=""

KEEP=0
if [ "${1:-}" = "--keep" ]; then
  KEEP=1
  shift
fi
if [ "$#" -ne 0 ]; then
  echo "Usage: $0 [--keep]" >&2
  exit 1
fi

REAL_BD_HOME="${BD_HOME:-$HOME/.local/opt/bd-search-engine}"
E2E_HOME="${E2E_HOME:-$HOME/.local/opt/bd-search-engine/e2e}"
rm -rf "$E2E_HOME"
mkdir -p "$E2E_HOME/bd" "$E2E_HOME/logs" "$E2E_HOME/pids" "$E2E_HOME/serving"
ln -s "$REPO_ROOT" "$E2E_HOME/repo"
RUN_ROOT="$E2E_HOME/repo"

ln -s "$REAL_BD_HOME/hadoop-3.3.6" "$E2E_HOME/bd/hadoop-3.3.6"
ln -s "$REAL_BD_HOME/spark-3.3.4-bin-hadoop3" "$E2E_HOME/bd/spark-3.3.4-bin-hadoop3"
ln -s "$REAL_BD_HOME/iceberg" "$E2E_HOME/bd/iceberg"
ln -s "$REAL_BD_HOME/venv" "$E2E_HOME/bd/venv"

export BD_HOME="$E2E_HOME/bd"
export HADOOP_LOG_DIR="$E2E_HOME/logs/hadoop"
export HADOOP_PID_DIR="$E2E_HOME/pids/hadoop"
export SPARK_LOG_DIR="$E2E_HOME/logs/spark"
export SPARK_PID_DIR="$E2E_HOME/pids/spark"
mkdir -p "$HADOOP_LOG_DIR" "$HADOOP_PID_DIR" "$SPARK_LOG_DIR" "$SPARK_PID_DIR"

cat >"$E2E_HOME/cluster.env" <<EOF
NODE1_IP=127.0.0.1
NODE2_IP=127.0.0.1
NODE3_IP=127.0.0.1
NODE4_IP=127.0.0.1
NAMENODE_PORT=19000
SPARK_MASTER_PORT=17077
SEARCH_PORT=18000
SPARK_MASTER_WEBUI_PORT=18080
WAREHOUSE_PATH=/warehouse
RAW_ROOT=/raw
EXPORT_ROOT=/exports
CHECKPOINT_ROOT=/checkpoints
SERVING_ROOT=$E2E_HOME/serving
EOF
export CLUSTER_ENV="$E2E_HOME/cluster.env"

# shellcheck source=scripts/lib.sh
. "$RUN_ROOT/scripts/lib.sh"
render_cluster_conf
package_app

SERVE_PID=""
cleanup() {
  if [ "$KEEP" -eq 1 ]; then
    return
  fi
  if [ -n "$SERVE_PID" ] && kill -0 "$SERVE_PID" 2>/dev/null; then
    kill "$SERVE_PID" 2>/dev/null || true
    wait "$SERVE_PID" 2>/dev/null || true
  fi
  SPARK_WORKER_INSTANCES=2 "$SPARK_HOME/sbin/stop-worker.sh" >/dev/null 2>&1 || true
  "$SPARK_HOME/sbin/stop-master.sh" >/dev/null 2>&1 || true
  hdfs --daemon stop datanode >/dev/null 2>&1 || true
  hdfs --daemon stop namenode >/dev/null 2>&1 || true
}
trap cleanup EXIT

hdfs namenode -format -nonInteractive
hdfs --daemon start namenode
hdfs --daemon start datanode

SPARK_MASTER_HOST=127.0.0.1 SPARK_MASTER_WEBUI_PORT=18080 "$SPARK_HOME/sbin/start-master.sh"
export SPARK_WORKER_INSTANCES=2 SPARK_WORKER_CORES=2 SPARK_WORKER_MEMORY=2g
export SPARK_WORKER_WEBUI_PORT=18081 SPARK_WORKER_PORT=18091
"$SPARK_HOME/sbin/start-worker.sh" "spark://127.0.0.1:$SPARK_MASTER_PORT"

hdfs dfsadmin -safemode wait
for _ in $(seq 1 60); do
  if hdfs dfsadmin -report | grep -q "Live datanodes (1)"; then
    break
  fi
  sleep 1
done
hdfs dfsadmin -report | grep -q "Live datanodes (1)"

for _ in $(seq 1 120); do
  alive="$(curl -sf "http://127.0.0.1:18080/json/" | python -c 'import json,sys; print(json.load(sys.stdin).get("aliveworkers", -1))')"
  if [ "$alive" = "2" ]; then
    break
  fi
  sleep 1
done
if [ "$alive" != "2" ]; then
  echo "Spark master did not report aliveworkers=2" >&2
  exit 1
fi

first_ingest_output="$(bash "$RUN_ROOT/scripts/node1_ingest.sh" 0)"
first_snapshot="$(python -c 'import json,sys
for line in sys.stdin.read().splitlines():
    line=line.strip()
    if line.startswith("{"):
        print(json.loads(line)["documents_snapshot"])
        break
' <<<"$first_ingest_output")"

bash "$RUN_ROOT/scripts/node3_process.sh"
python -m dse.serving.pull \
  --export-root "hdfs://${NODE2_IP}:${NAMENODE_PORT}${EXPORT_ROOT}" \
  --serving-root "$SERVING_ROOT"

bash "$RUN_ROOT/scripts/node4_serve.sh" >"$E2E_HOME/node4_serve.log" 2>&1 &
SERVE_PID=$!

for _ in $(seq 1 120); do
  if curl -sf "http://127.0.0.1:${SEARCH_PORT}/health" >/dev/null; then
    break
  fi
  sleep 1
done
health_phase0="$(curl -sf "http://127.0.0.1:${SEARCH_PORT}/health")"
search_april_phase0="$(curl -sf "http://127.0.0.1:${SEARCH_PORT}/search?q=april&k=100")"
python -c 'import json,sys
health=json.loads(sys.argv[1])
search=json.loads(sys.argv[2])
if health.get("version") != 1 or health.get("docs") != 1000:
    raise SystemExit("e2e assertion 1 failed: {}".format(sys.argv[1:]))
ok=False
for row in search.get("results", []):
    if row.get("doc_id") == "1":
        if not isinstance(row.get("url"), str) or not row["url"]:
            raise SystemExit("e2e assertion 2 failed: {}".format(sys.argv[1:]))
        if not isinstance(row.get("title"), str) or not row["title"]:
            raise SystemExit("e2e assertion 3 failed: {}".format(sys.argv[1:]))
        if not isinstance(row.get("score"), (int, float)):
            raise SystemExit("e2e assertion 4 failed: {}".format(sys.argv[1:]))
        ok=True
        break
if not ok:
    raise SystemExit("e2e assertion 5 failed: {}".format(sys.argv[1:]))
' "$health_phase0" "$search_april_phase0"

second_ingest_output="$(bash "$RUN_ROOT/scripts/node1_ingest.sh" 0)"
second_snapshot="$(python -c 'import json,sys
for line in sys.stdin.read().splitlines():
    line=line.strip()
    if line.startswith("{"):
        print(json.loads(line)["documents_snapshot"])
        break
' <<<"$second_ingest_output")"
if [ -z "$first_snapshot" ] || [ "$first_snapshot" != "$second_snapshot" ]; then
  echo "documents snapshot changed after re-ingesting phase 0" >&2
  exit 1
fi

bash "$RUN_ROOT/scripts/fault_tolerance_check.sh"

bash "$RUN_ROOT/scripts/node1_ingest.sh" 1
submit_job "$NODE3_IP" dse.jobs.index_job
submit_job "$NODE3_IP" dse.jobs.pagerank_job --checkpoint-dir "hdfs://${NODE2_IP}:${NAMENODE_PORT}${CHECKPOINT_ROOT}/pagerank"
if submit_job "$NODE3_IP" dse.jobs.export_job --export-root "hdfs://${NODE2_IP}:${NAMENODE_PORT}${EXPORT_ROOT}" --simulate-failure; then
  echo "simulate-failure export unexpectedly succeeded" >&2
  exit 1
fi

health_after_failed_export="$(curl -sf "http://127.0.0.1:${SEARCH_PORT}/health")"
python -c 'import json,sys
health=json.loads(sys.argv[1])
if health.get("version") != 1:
    raise SystemExit("e2e assertion 6 failed: {}".format(sys.argv[1:]))
' "$health_after_failed_export"
if hdfs dfs -test -e "${EXPORT_ROOT}/v2"; then
  echo "v2 exists after failed export" >&2
  exit 1
fi
if hdfs dfs -ls "$EXPORT_ROOT" | grep -q '\.tmp-'; then
  echo "temporary export directory remained after failure" >&2
  exit 1
fi

bash "$RUN_ROOT/scripts/node3_process.sh"
bash "$RUN_ROOT/scripts/node4_pull.sh"

health_phase1="$(curl -sf "http://127.0.0.1:${SEARCH_PORT}/health")"
search_april_phase1="$(curl -sf "http://127.0.0.1:${SEARCH_PORT}/search?q=april&k=100")"
search_added_phase1="$(curl -sf "http://127.0.0.1:${SEARCH_PORT}/search?q=added+during+project+phase")"
python -c 'import json,sys
health=json.loads(sys.argv[1])
april=json.loads(sys.argv[2])
added=json.loads(sys.argv[3])
if health.get("version") != 2 or health.get("docs") != 975:
    raise SystemExit("e2e assertion 7 failed: {}".format(sys.argv[1:]))
if any(row.get("doc_id") == "1" for row in april.get("results", [])):
    raise SystemExit("e2e assertion 8 failed: {}".format(sys.argv[1:]))
if not any(str(row.get("doc_id", "")).startswith("phase1-new-") for row in added.get("results", [])):
    raise SystemExit("e2e assertion 9 failed: {}".format(sys.argv[1:]))
' "$health_phase1" "$search_april_phase1" "$search_added_phase1"

bash "$RUN_ROOT/scripts/node3_snapshots.sh" list
bash "$RUN_ROOT/scripts/node3_snapshots.sh" rollback --snapshot "$first_snapshot"
bash "$RUN_ROOT/scripts/node3_process.sh"
bash "$RUN_ROOT/scripts/node4_pull.sh"

health_rollback="$(curl -sf "http://127.0.0.1:${SEARCH_PORT}/health")"
search_april_rollback="$(curl -sf "http://127.0.0.1:${SEARCH_PORT}/search?q=april&k=100")"
python -c 'import json,sys
health=json.loads(sys.argv[1])
april=json.loads(sys.argv[2])
if health.get("version") != 3 or health.get("docs") != 1000:
    raise SystemExit("rollback did not restore the phase 0 corpus: {}".format(health))
if not any(row.get("doc_id") == "1" for row in april.get("results", [])):
    raise SystemExit("rollback did not restore a deleted page")
' "$health_rollback" "$search_april_rollback"

echo "E2E PASSED"
