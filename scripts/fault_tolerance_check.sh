#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib.sh
. "$SCRIPT_DIR/lib.sh"
render_cluster_conf

MASTER_URL="http://127.0.0.1:${SPARK_MASTER_WEBUI_PORT}/json/"
CHECKPOINT_BASE="hdfs://${NODE2_IP}:${NAMENODE_PORT}${CHECKPOINT_ROOT}/fault-baseline"
BASELINE_OUT="hdfs://${NODE2_IP}:${NAMENODE_PORT}/fault/baseline"
AFTER_OUT="hdfs://${NODE2_IP}:${NAMENODE_PORT}/fault/after"
JOB_LOG="${SPARK_LOG_DIR:-/tmp}/fault_after_$(date +%s).log"

submit_job "$NODE3_IP" dse.jobs.fault_check \
  --iterations 40 \
  --checkpoint-dir "$CHECKPOINT_BASE" \
  --output "$BASELINE_OUT"

submit_job "$NODE3_IP" dse.jobs.fault_check \
  --iterations 40 \
  --checkpoint-dir "$CHECKPOINT_BASE" \
  --output "$AFTER_OUT" \
  --compare-to "$BASELINE_OUT" >"$JOB_LOG" 2>&1 &
job_pid=$!

target_worker_json=""
for _ in $(seq 1 120); do
  target_worker_json="$(curl -sf "$MASTER_URL" | python -c 'import json,sys
d=json.load(sys.stdin)
apps=[a for a in d.get("activeapps",[]) if a.get("state")=="RUNNING"]
workers=[w for w in d.get("workers",[]) if w.get("state")=="ALIVE"]
if apps and workers:
    w=workers[0]
    print(json.dumps({"id":w["id"],"host":w["host"],"port":w["port"]}))
')"
  if [ -n "$target_worker_json" ]; then
    break
  fi
  sleep 1
done
if [ -z "$target_worker_json" ]; then
  echo "No RUNNING app on Spark master" >&2
  kill "$job_pid" 2>/dev/null || true
  wait "$job_pid" 2>/dev/null || true
  exit 1
fi

sleep 5
if ! curl -sf "$MASTER_URL" | python -c 'import json,sys
d=json.load(sys.stdin)
apps=[a for a in d.get("activeapps",[]) if a.get("state")=="RUNNING"]
raise SystemExit(0 if apps else 1)
'; then
  echo "Application did not stay RUNNING long enough" >&2
  kill "$job_pid" 2>/dev/null || true
  wait "$job_pid" 2>/dev/null || true
  exit 1
fi

worker_id="$(python -c 'import json,sys; print(json.loads(sys.argv[1])["id"])' "$target_worker_json")"
worker_host="$(python -c 'import json,sys; print(json.loads(sys.argv[1])["host"])' "$target_worker_json")"
worker_port="$(python -c 'import json,sys; print(json.loads(sys.argv[1])["port"])' "$target_worker_json")"

worker_pid="$(jps -lm | awk -v port="--port ${worker_port}" '
  /org\.apache\.spark\.deploy\.worker\.Worker/ {
    if (index($0, port) > 0) { print $1; exit }
  }'
)"
if [ -z "$worker_pid" ]; then
  echo "Could not find Worker JVM for ${worker_id}" >&2
  kill "$job_pid" 2>/dev/null || true
  wait "$job_pid" 2>/dev/null || true
  exit 1
fi

executor_pids="$(jps -lm | awk -v marker="@${worker_host}:${worker_port}" '
  /org\.apache\.spark\.executor\.CoarseGrainedExecutorBackend/ {
    if (index($0, marker) > 0) print $1
  }'
)"
if [ -z "$executor_pids" ]; then
  echo "Worker ${worker_id} has no executor, so killing it would prove nothing" >&2
  kill "$job_pid" 2>/dev/null || true
  wait "$job_pid" 2>/dev/null || true
  exit 1
fi
for pid in $executor_pids; do
  kill -9 "$pid"
done
kill -9 "$worker_pid"

if ! wait "$job_pid"; then
  cat "$JOB_LOG" >&2
  exit 1
fi

if ! grep -qE 'Lost executor|Removed executor|Removing executor' "$JOB_LOG"; then
  echo "job log shows no lost executor, so the kill did not disturb this job" >&2
  exit 1
fi
if ! grep -q '"ok":[[:space:]]*true' "$JOB_LOG"; then
  cat "$JOB_LOG" >&2
  exit 1
fi

for _ in $(seq 1 60); do
  aliveworkers="$(curl -sf "$MASTER_URL" | python -c 'import json,sys; print(json.load(sys.stdin).get("aliveworkers", -1))')"
  if [ "$aliveworkers" = "1" ]; then
    break
  fi
  sleep 1
done
if [ "$aliveworkers" != "1" ]; then
  echo "aliveworkers did not drop to 1 after worker kill" >&2
  exit 1
fi

instance=$(( worker_port - SPARK_WORKER_PORT + 1 ))
"$SPARK_HOME/sbin/spark-daemon.sh" start org.apache.spark.deploy.worker.Worker "$instance" \
  --webui-port "$(( SPARK_WORKER_WEBUI_PORT + instance - 1 ))" \
  --port "$worker_port" \
  "spark://127.0.0.1:${SPARK_MASTER_PORT}"

for _ in $(seq 1 120); do
  aliveworkers="$(curl -sf "$MASTER_URL" | python -c 'import json,sys; print(json.load(sys.stdin).get("aliveworkers", -1))')"
  if [ "$aliveworkers" = "2" ]; then
    break
  fi
  sleep 1
done
if [ "$aliveworkers" != "2" ]; then
  echo "aliveworkers did not recover to 2 after restart" >&2
  exit 1
fi

json_line="$(python -c 'import sys
for line in open(sys.argv[1], "r", encoding="utf-8"):
    if line.strip().startswith("{") and "\"ok\"" in line:
        last=line.strip()
print(last if "last" in locals() else "")
' "$JOB_LOG")"
if [ -z "$json_line" ]; then
  echo "Fault check JSON output line not found" >&2
  exit 1
fi

echo "$json_line"
echo "FAULT TOLERANCE PASSED"
