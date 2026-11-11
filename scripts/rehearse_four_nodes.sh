#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"

KEEP=0
if [ "${1:-}" = "--keep" ]; then
  KEEP=1
  shift
fi
if [ "$#" -ne 0 ]; then
  echo "Usage: $0 [--keep]" >&2
  exit 1
fi

BD_HOME_REAL="${BD_HOME:-$HOME/.local/opt/bd-search-engine}"
PY="$BD_HOME_REAL/venv/bin/python"
if [ ! -x "$PY" ]; then
  echo "Missing Python interpreter: $PY" >&2
  exit 1
fi

REHEARSAL_BASE="${REHEARSAL_HOME:-$HOME/.local/opt/bd-search-engine/rehearsal}"
RUN_DIR="$REHEARSAL_BASE/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN_DIR"
mkdir -p "$RUN_DIR/logs"

NODE1_IP="10.89.77.11"
NODE2_IP="10.89.77.12"
NODE3_IP="10.89.77.13"
NODE4_IP="10.89.77.14"
NODE_IPS_JSON="{\"1\":\"$NODE1_IP\",\"2\":\"$NODE2_IP\",\"3\":\"$NODE3_IP\",\"4\":\"$NODE4_IP\"}"

SEARCH_PORT=""
NAMENODE_PORT=""
SPARK_MASTER_PORT=""
EXPORT_ROOT=""

PHASE0_SNAPSHOT_FILE="$RUN_DIR/phase0.snapshot"
HEALTH_V1_FILE="$RUN_DIR/health-v1.json"
HEALTH_V2_FILE="$RUN_DIR/health-v2.json"
HEALTH_V3_FILE="$RUN_DIR/health-v3.json"
MANIFEST_V1_FILE="$RUN_DIR/manifest-v1.json"
MANIFEST_V2_FILE="$RUN_DIR/manifest-v2.json"
MANIFEST_V3_FILE="$RUN_DIR/manifest-v3.json"
FSCK_WAREHOUSE_FILE="$RUN_DIR/fsck-warehouse.txt"
FSCK_EXPORTS_FILE="$RUN_DIR/fsck-exports.txt"
FSCK_RAW_FILE="$RUN_DIR/fsck-raw.txt"
KILL_SECONDS_FILE="$RUN_DIR/kill-seconds"

SUMMARY_FILE="$RUN_DIR/summary.json"
STEPS_FILE="$RUN_DIR/steps.tsv"
FAILED_STEP=""
FAILED_LOG=""
PASSED=0
STEP_INDEX=1

container_name() {
  echo "dse-node$1"
}

step_log() {
  local idx="$1"
  local name="$2"
  local safe
  safe="$(echo "$name" | tr ' /' '--' | tr -cd '[:alnum:]-_')"
  printf "%s/%02d-%s.log" "$RUN_DIR" "$idx" "$safe"
}

record_step() {
  local name="$1"
  local seconds="$2"
  printf "%s\t%s\n" "$name" "$seconds" >>"$STEPS_FILE"
}

read_file_or_empty() {
  local path="$1"
  if [ -f "$path" ]; then
    cat "$path"
    return 0
  fi
  return 0
}

run_step() {
  local name="$1"
  shift
  local idx="$STEP_INDEX"
  STEP_INDEX=$((STEP_INDEX + 1))
  local log
  log="$(step_log "$idx" "$name")"
  local start end seconds rc
  start="$(date +%s)"

  set +e
  (
    set -e
    echo "== $name"
    "$@"
  ) 2>&1 | tee "$log"
  rc=${PIPESTATUS[0]}
  set -e

  end="$(date +%s)"
  seconds=$((end - start))
  record_step "$name" "$seconds"

  if [ "$rc" -ne 0 ]; then
    FAILED_STEP="$name"
    FAILED_LOG="$log"
    echo "FAILED STEP: $name" >&2
    echo "LOG: $log" >&2
    exit "$rc"
  fi
}

node_exec() {
  local node="$1"
  shift
  podman exec --user "node$node" --workdir / "$(container_name "$node")" "$@"
}

node_script() {
  local node="$1"
  local script="$2"
  shift 2
  node_exec "$node" bash "/home/node$node/distributed-search-engine/scripts/$script" "$@"
}

node_source_run() {
  local node="$1"
  local cmd="$2"
  node_exec "$node" bash -c 'source "$HOME/distributed-search-engine/scripts/lib.sh" && eval "$1"' _ "$cmd"
}

set_cluster_env_ips() {
  local env_file="$1"
  "$PY" - "$env_file" "$NODE1_IP" "$NODE2_IP" "$NODE3_IP" "$NODE4_IP" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
values = {
    "NODE1_IP": sys.argv[2],
    "NODE2_IP": sys.argv[3],
    "NODE3_IP": sys.argv[4],
    "NODE4_IP": sys.argv[5],
}
lines = []
for raw in path.read_text(encoding="utf-8").splitlines():
    if "=" in raw:
        key, _, _ = raw.partition("=")
        if key in values:
            lines.append(f"{key}={values[key]}")
            continue
    lines.append(raw)
path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
}

cluster_env_value() {
  local key="$1"
  local env_file="$RUN_DIR/src/config/cluster.env"
  local value
  value="$(grep "^${key}=" "$env_file" | head -n 1 | cut -d= -f2-)"
  if [ -z "$value" ]; then
    echo "Missing ${key} in $env_file" >&2
    exit 1
  fi
  printf "%s" "$value"
}

load_cluster_env_values() {
  SEARCH_PORT="$(cluster_env_value SEARCH_PORT)"
  NAMENODE_PORT="$(cluster_env_value NAMENODE_PORT)"
  SPARK_MASTER_PORT="$(cluster_env_value SPARK_MASTER_PORT)"
  EXPORT_ROOT="$(cluster_env_value EXPORT_ROOT)"
}

parse_ingest_json_field() {
  local field="$1"
  "$PY" -c 'import json
import sys

field = sys.argv[1]
value = None
for line in sys.stdin.read().splitlines():
    line = line.strip()
    if not line.startswith("{"):
        continue
    obj = json.loads(line)
    if field in obj:
        value = obj[field]
if value is None:
    raise SystemExit(f"missing field {field}")
print(value)' "$field"
}

fsck_for_path() {
  local path="$1"
  node_source_run 2 "hdfs fsck '$path'"
}

fsck_summary() {
  "$PY" -c 'import sys

keep = []
for line in sys.stdin.read().splitlines():
    s = line.strip()
    if (
        s.startswith("Under-replicated blocks:")
        or s.startswith("Average block replication:")
        or s.startswith("Missing blocks:")
        or s.startswith("The filesystem under path ")
    ):
        keep.append(s)
print(" | ".join(keep))'
}

wait_for_datanodes_2() {
  local report=""
  for _ in $(seq 1 120); do
    report="$(node_source_run 2 'hdfs dfsadmin -report')"
    if grep -q "Live datanodes (2)" <<<"$report"; then
      echo "$report"
      return 0
    fi
    sleep 1
  done
  echo "$report" >&2
  return 1
}

wait_for_workers_2() {
  local json=""
  local alive=""
  for _ in $(seq 1 120); do
    if json="$(node_exec 1 curl -sf "http://$NODE3_IP:8080/json/" 2>/dev/null)"; then
      alive="$("$PY" -c 'import json,sys; print(json.load(sys.stdin).get("aliveworkers", -1))' <<<"$json")"
      if [ "$alive" = "2" ]; then
        return 0
      fi
    fi
    sleep 1
  done
  echo "$json" >&2
  return 1
}

hash_exports_v2_list() {
  node_source_run 4 "python /tools/hash_export.py /exports/v2"
}

restart_datanode() {
  local node="$1"
  node_source_run "$node" 'set -euo pipefail
user="$(whoami)"
pid_file="/tmp/hadoop-${user}-datanode.pid"
if [ -f "$pid_file" ]; then
  rm -f "$pid_file"
fi
unset SPARK_LOCAL_IP
hdfs --daemon start datanode'
}

kill_spark_executor_and_worker_on_node2() {
  node_source_run 2 'set -euo pipefail
exec_pids="$(pgrep -f "org.apache.spark.executor.[C]oarseGrainedExecutorBackend" || true)"
if [ -z "$exec_pids" ]; then
  echo "No executor process found on node2" >&2
  exit 1
fi
for p in $exec_pids; do
  kill -9 "$p"
done
worker_pids="$(pgrep -f "org.apache.spark.deploy.worker.[W]orker" || true)"
if [ -z "$worker_pids" ]; then
  echo "No worker process found on node2" >&2
  exit 1
fi
for p in $worker_pids; do
  kill -9 "$p"
done'
}

kill_datanode_on_node() {
  local node="$1"
  node_source_run "$node" 'set -euo pipefail
pids="$(pgrep -f "proc_[d]atanode" || true)"
if [ -z "$pids" ]; then
  echo "No DataNode process found" >&2
  exit 1
fi
for p in $pids; do
  kill -9 "$p"
done'
}

write_summary() {
  local source_commit dirty
  source_commit="$(git -C "$REPO" rev-parse HEAD)"
  if [ -n "$(git -C "$REPO" status --porcelain)" ]; then
    dirty=true
  else
    dirty=false
  fi
  SOURCE_COMMIT="$source_commit"
  DIRTY="$dirty"
  export SOURCE_COMMIT DIRTY PASSED FAILED_STEP NODE_IPS_JSON SUMMARY_FILE STEPS_FILE
  export HEALTH_V1_FILE HEALTH_V2_FILE HEALTH_V3_FILE
  export MANIFEST_V1_FILE MANIFEST_V2_FILE MANIFEST_V3_FILE
  export FSCK_WAREHOUSE_FILE FSCK_EXPORTS_FILE FSCK_RAW_FILE KILL_SECONDS_FILE
  "$PY" - <<'PY'
import json
import os
from pathlib import Path

def file_text(path_env: str) -> str:
    path = Path(os.environ[path_env])
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()

def load_json_or_none(path_env: str):
    text = file_text(path_env)
    if not text:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text

steps = []
steps_path = Path(os.environ["STEPS_FILE"])
if steps_path.exists():
    for row in steps_path.read_text(encoding="utf-8").splitlines():
        if not row.strip():
            continue
        name, seconds = row.split("\t", 1)
        steps.append({"name": name, "seconds": float(seconds)})

summary = {
    "source_commit": os.environ["SOURCE_COMMIT"],
    "dirty": os.environ["DIRTY"] == "true",
    "passed": os.environ["PASSED"] == "1",
    "failed_step": os.environ.get("FAILED_STEP") or None,
    "node_ips": json.loads(os.environ["NODE_IPS_JSON"]),
    "steps": steps,
    "health": {
        "v1": load_json_or_none("HEALTH_V1_FILE"),
        "v2": load_json_or_none("HEALTH_V2_FILE"),
        "v3": load_json_or_none("HEALTH_V3_FILE"),
    },
    "manifests": {
        "v1": load_json_or_none("MANIFEST_V1_FILE"),
        "v2": load_json_or_none("MANIFEST_V2_FILE"),
        "v3": load_json_or_none("MANIFEST_V3_FILE"),
    },
    "fsck_lines": {
        "warehouse": file_text("FSCK_WAREHOUSE_FILE"),
        "exports": file_text("FSCK_EXPORTS_FILE"),
        "raw": file_text("FSCK_RAW_FILE"),
    },
    "max_kill_timing_seconds": float(file_text("KILL_SECONDS_FILE") or "0"),
}
Path(os.environ["SUMMARY_FILE"]).write_text(json.dumps(summary, indent=2), encoding="utf-8")
PY
}

cleanup() {
  if [ "$KEEP" -eq 0 ]; then
    for n in 1 2 3 4; do
      local c
      c="$(container_name "$n")"
      mkdir -p "$RUN_DIR/logs/node$n"

      local hadoop_home spark_home
      hadoop_home="$(node_source_run "$n" 'echo "$HADOOP_HOME"' 2>/dev/null || true)"
      spark_home="$(node_source_run "$n" 'echo "$SPARK_HOME"' 2>/dev/null || true)"
      if [ -n "$hadoop_home" ]; then
        podman cp "$c:$hadoop_home/logs" "$RUN_DIR/logs/node$n/hadoop-logs" >/dev/null 2>&1 || true
      fi
      if [ -n "$spark_home" ]; then
        podman cp "$c:$spark_home/logs" "$RUN_DIR/logs/node$n/spark-logs" >/dev/null 2>&1 || true
      fi
      podman cp "$c:/home/node4/serve.log" "$RUN_DIR/logs/node4/serve.log" >/dev/null 2>&1 || true
      podman rm -f "$c" >/dev/null 2>&1 || true
    done
    podman network rm dse-rehearsal >/dev/null 2>&1 || true
  fi
  write_summary
}
trap cleanup EXIT

step_prepare_source_tree() {
  rm -rf "$RUN_DIR/src"
  mkdir -p "$RUN_DIR/src"
  git -C "$REPO" ls-files -z | tar -C "$REPO" --null -T - -cf - | tar -xf - -C "$RUN_DIR/src"
}

step_prepare_tools_dir() {
  mkdir -p "$RUN_DIR/tools"
  cat >"$RUN_DIR/tools/hash_export.py" <<'PY'
#!/usr/bin/env python3
import hashlib
import subprocess
import sys

if len(sys.argv) != 2:
    raise SystemExit("usage: hash_export.py <hdfs_path>")

root = sys.argv[1]
ls_out = subprocess.check_output(["hdfs", "dfs", "-ls", "-R", root], text=True)
paths = []
for line in ls_out.splitlines():
    parts = line.split()
    if len(parts) >= 8 and parts[0].startswith("-"):
        paths.append(parts[-1])
paths.sort()
for p in paths:
    data = subprocess.check_output(["hdfs", "dfs", "-cat", p])
    print(f"{hashlib.sha256(data).hexdigest()} {p}")
PY
  chmod 755 "$RUN_DIR/tools/hash_export.py"
}

step_prepare_node_source_and_hosts() {
  local n="$1"
  rm -rf "$RUN_DIR/node$n"
  mkdir -p "$RUN_DIR/node$n"
  cp -a "$RUN_DIR/src" "$RUN_DIR/node$n/distributed-search-engine"
  printf '127.0.0.1 localhost\n127.0.1.1 node%s\n' "$n" >"$RUN_DIR/node$n/hosts"
  set_cluster_env_ips "$RUN_DIR/node$n/distributed-search-engine/config/cluster.env"
}

step_build_rehearsal_image() {
  podman build --security-opt label=disable \
    -v "$BD_HOME_REAL/downloads:/cache:ro" \
    -f "$REPO/deploy/rehearsal/Containerfile" \
    -t dse-rehearsal:latest \
    "$RUN_DIR/src"
}

step_remove_stale_containers_and_network() {
  for n in 1 2 3 4; do
    podman rm -f "dse-node$n" >/dev/null 2>&1 || true
  done
  podman network rm dse-rehearsal >/dev/null 2>&1 || true
}

step_create_rehearsal_network() {
  podman network create --disable-dns --subnet 10.89.77.0/24 dse-rehearsal
}

step_start_container() {
  local n="$1"
  local mem="2g"
  local cpu_args=()
  if [ "$n" = "2" ] || [ "$n" = "3" ]; then
    mem="4g"
    cpu_args=(--cpus 3)
  fi
  local mount_data=()
  if [ "$n" = "1" ]; then
    rm -rf "$RUN_DIR/node1-data"
    mkdir -p "$RUN_DIR/node1-data"
    cp -rL "$REPO/data/full" "$RUN_DIR/node1-data/full"
    chmod -R a+rX "$RUN_DIR/node1-data"
    mount_data=(-v "$RUN_DIR/node1-data:/home/node1/distributed-search-engine/data:ro")
  fi
  podman run -d --name "dse-node$n" \
    --network dse-rehearsal \
    --ip "10.89.77.1$n" \
    --hostname "node$n" \
    --no-hosts \
    --init \
    --security-opt label=disable \
    --user "node$n" \
    --memory "$mem" \
    "${cpu_args[@]}" \
    -v "$RUN_DIR/node$n/hosts:/etc/hosts:ro" \
    -v "$RUN_DIR/node$n/distributed-search-engine:/home/node$n/distributed-search-engine:rw" \
    -v "$RUN_DIR/tools:/tools:ro" \
    "${mount_data[@]}" \
    dse-rehearsal:latest
}

step_start_node2_services() {
  node_script 2 node2_start.sh
}

step_start_node3_services() {
  node_script 3 node3_start.sh
}

step_start_node4_serving_process() {
  podman exec -d --user node4 --workdir / dse-node4 bash -c 'exec bash /home/node4/distributed-search-engine/scripts/node4_serve.sh > /home/node4/serve.log 2>&1'
}

step_wait_node4_health_endpoint() {
  local url="http://$NODE4_IP:$SEARCH_PORT/health"
  for _ in $(seq 1 120); do
    local status
    status="$(node_exec 1 bash -c "curl -s -o /dev/null -w '%{http_code}' '$url' || true")"
    if [ "$status" != "000" ] && [ -n "$status" ]; then
      echo "Node4 /health status: $status"
      return 0
    fi
    sleep 1
  done
  echo "Node4 /health never answered at $url" >&2
  node_exec 4 cat /home/node4/serve.log || true
  return 1
}

step_assert_search_unavailable_before_first_pull() {
  local status
  status="$(node_exec 1 curl -s -o /tmp/rehearse-search.out -w '%{http_code}' "http://$NODE4_IP:$SEARCH_PORT/search?q=alpha")"
  if [ "$status" != "503" ]; then
    echo "Expected /search to return 503 before first pull, got $status" >&2
    return 1
  fi
}

step_wait_for_two_datanodes() {
  wait_for_datanodes_2
}

step_wait_for_two_spark_workers() {
  wait_for_workers_2
}

step_check_placement_node() {
  local n="$1"
  local jps_text
  jps_text="$(node_source_run "$n" '"$JAVA_HOME/bin/jps" -lm')"
  printf '%s\n' "$jps_text" | "$PY" "$REPO/deploy/rehearsal/checks.py" placement "$n"
}

step_phase0_ingest() {
  local out snapshot changed
  out="$(node_script 1 node1_ingest.sh 0)"
  printf '%s\n' "$out"
  snapshot="$(printf '%s\n' "$out" | parse_ingest_json_field documents_snapshot)"
  changed="$(printf '%s\n' "$out" | parse_ingest_json_field documents_changed)"
  if [ -z "$snapshot" ] || [ -z "$changed" ]; then
    echo "Phase 0 ingest did not provide required fields" >&2
    return 1
  fi
  printf '%s\n' "$snapshot" >"$PHASE0_SNAPSHOT_FILE"
}

step_phase0_process() {
  node_script 3 node3_process.sh
}

step_phase0_pull() {
  node_script 4 node4_pull.sh
}

step_health_v1_docs1000() {
  local h
  h="$(node_exec 1 curl -sf "http://$NODE4_IP:$SEARCH_PORT/health")"
  printf '%s\n' "$h" | "$PY" "$REPO/deploy/rehearsal/checks.py" health 1 1000
  printf '%s\n' "$h" >"$HEALTH_V1_FILE"
}

step_manifest_v1_postings453438() {
  local m
  m="$(node_source_run 2 "hdfs dfs -cat '$EXPORT_ROOT/v1/manifest.json'")"
  printf '%s\n' "$m" | "$PY" "$REPO/deploy/rehearsal/checks.py" manifest 1 1000 453438
  printf '%s\n' "$m" >"$MANIFEST_V1_FILE"
}

step_assert_phase0_doc1_present() {
  local search
  search="$(node_exec 1 curl -sf "http://$NODE4_IP:$SEARCH_PORT/search?q=april&k=100")"
  "$PY" - "$search" <<'PY'
import json
import sys

rows = json.loads(sys.argv[1]).get("results", [])
for row in rows:
    if str(row.get("doc_id")) != "1":
        continue
    if not str(row.get("url", "")).strip():
        raise SystemExit("doc 1 url is empty")
    if not str(row.get("title", "")).strip():
        raise SystemExit("doc 1 title is empty")
    score = row.get("score")
    if not isinstance(score, (int, float)):
        raise SystemExit("doc 1 score is not numeric")
    raise SystemExit(0)
raise SystemExit("doc 1 not found after phase0")
PY
}

step_fsck_replication_warehouse_exports_raw() {
  local fw fe fr
  fw="$(fsck_for_path '/warehouse')"
  fe="$(fsck_for_path '/exports')"
  fr="$(fsck_for_path '/raw')"
  printf '%s\n' "$fw" | "$PY" "$REPO/deploy/rehearsal/checks.py" replication 2.0
  printf '%s\n' "$fe" | "$PY" "$REPO/deploy/rehearsal/checks.py" replication 2.0
  printf '%s\n' "$fr" | "$PY" "$REPO/deploy/rehearsal/checks.py" replication 2.0
  printf '%s\n' "$fw" >"$FSCK_WAREHOUSE_FILE"
  printf '%s\n' "$fe" >"$FSCK_EXPORTS_FILE"
  printf '%s\n' "$fr" >"$FSCK_RAW_FILE"
}

step_phase0_reingest_changed0() {
  local out changed
  out="$(node_script 1 node1_ingest.sh 0)"
  printf '%s\n' "$out"
  changed="$(printf '%s\n' "$out" | parse_ingest_json_field documents_changed)"
  if [ "$changed" != "0" ]; then
    echo "Expected documents_changed=0 on phase0 reingest, got $changed" >&2
    return 1
  fi
}

step_phase1_ingest() {
  node_script 1 node1_ingest.sh 1
}

step_phase1_index_job() {
  node_source_run 3 'package_app; submit_job "$NODE3_IP" dse.jobs.index_job'
}

step_phase1_pagerank_with_worker_kill() {
  local pr_log="$RUN_DIR/pagerank-worker-kill.log"
  node_source_run 3 'package_app; submit_job "$NODE3_IP" dse.jobs.pagerank_job --checkpoint-dir "hdfs://$NODE2_IP:$NAMENODE_PORT$CHECKPOINT_ROOT/pagerank"' >"$pr_log" 2>&1 &
  local job_pid=$!

  local running=0
  local node2_has_exec=0
  for _ in $(seq 1 120); do
    local json
    if json="$(node_exec 1 curl -sf "http://$NODE3_IP:8080/json/" 2>/dev/null)"; then
      if "$PY" -c 'import json,sys; d=json.load(sys.stdin); raise SystemExit(0 if any(a.get("state")=="RUNNING" for a in d.get("activeapps", [])) else 1)' <<<"$json"; then
        running=1
      fi
    fi
    if node_source_run 2 '"$JAVA_HOME/bin/jps" -lm' | grep "CoarseGrainedExecutorBackend" >/dev/null; then
      node2_has_exec=1
    fi
    if [ "$running" -eq 1 ] && [ "$node2_has_exec" -eq 1 ]; then
      break
    fi
    sleep 1
  done
  if [ "$running" -ne 1 ] || [ "$node2_has_exec" -ne 1 ]; then
    set +e
    wait "$job_pid"
    set -e
    echo "No RUNNING app with an executor on node2 was seen (running=$running node2_has_exec=$node2_has_exec)" >&2
    cat "$pr_log"
    return 1
  fi

  sleep 5
  if ! node_exec 1 curl -sf "http://$NODE3_IP:8080/json/" | "$PY" -c 'import json,sys; d=json.load(sys.stdin); raise SystemExit(0 if any(a.get("state")=="RUNNING" for a in d.get("activeapps", [])) else 1)'; then
    set +e
    wait "$job_pid"
    set -e
    echo "Pagerank app was not RUNNING when kill should occur" >&2
    cat "$pr_log"
    return 1
  fi

  local kill_start kill_end rc
  kill_start="$(date +%s)"
  kill_spark_executor_and_worker_on_node2

  set +e
  wait "$job_pid"
  rc=$?
  set -e
  if [ "$rc" -ne 0 ]; then
    echo "Pagerank job exit code: $rc" >&2
    cat "$pr_log"
    return 1
  fi
  if ! grep -q "Lost executor" "$pr_log"; then
    echo "Expected Lost executor in pagerank log" >&2
    cat "$pr_log"
    return 1
  fi

  local aliveworkers=""
  for _ in $(seq 1 120); do
    if aliveworkers="$(node_exec 1 curl -sf "http://$NODE3_IP:8080/json/" | "$PY" -c 'import json,sys; print(json.load(sys.stdin).get("aliveworkers", -1))')"; then
      if [ "$aliveworkers" = "1" ]; then
        break
      fi
    fi
    sleep 1
  done
  if [ "$aliveworkers" != "1" ]; then
    echo "Expected 1 alive worker after node2 worker kill, got ${aliveworkers:-unknown}" >&2
    return 1
  fi

  node_source_run 2 'export SPARK_LOCAL_IP="$NODE2_IP"; start-worker.sh "spark://$NODE3_IP:$SPARK_MASTER_PORT"'
  for _ in $(seq 1 120); do
    aliveworkers="$(node_exec 1 curl -sf "http://$NODE3_IP:8080/json/" | "$PY" -c 'import json,sys; print(json.load(sys.stdin).get("aliveworkers", -1))')"
    if [ "$aliveworkers" = "2" ]; then
      break
    fi
    sleep 1
  done
  if [ "$aliveworkers" != "2" ]; then
    echo "Expected 2 alive workers after restart, got $aliveworkers" >&2
    return 1
  fi

  kill_end="$(date +%s)"
  printf '%s\n' "$((kill_end - kill_start))" >"$KILL_SECONDS_FILE"
}

step_simulate_export_failure_keeps_v1() {
  local out rc h
  set +e
  out="$(node_source_run 3 "package_app; submit_job \"\$NODE3_IP\" dse.jobs.export_job --export-root \"hdfs://\$NODE2_IP:\$NAMENODE_PORT\$EXPORT_ROOT\" --simulate-failure" 2>&1)"
  rc=$?
  set -e
  printf '%s\n' "$out"
  if [ "$rc" -eq 0 ]; then
    echo "Expected export failure but command succeeded" >&2
    return 1
  fi
  if ! printf '%s\n' "$out" | grep >/dev/null 'simulated failure'; then
    echo "Expected simulated failure marker in export output" >&2
    return 1
  fi
  h="$(node_exec 1 curl -sf "http://$NODE4_IP:$SEARCH_PORT/health")"
  printf '%s\n' "$h" | "$PY" "$REPO/deploy/rehearsal/checks.py" health 1 1000
  if node_source_run 2 "hdfs dfs -test -e '$EXPORT_ROOT/v2'"; then
    echo "Unexpected v2 export exists after simulated failure" >&2
    return 1
  fi
  if node_source_run 2 "hdfs dfs -ls '$EXPORT_ROOT'" | grep >/dev/null '\.tmp-'; then
    echo "Found temporary export directory after simulated failure" >&2
    return 1
  fi
}

step_phase1_process_and_pull() {
  node_script 3 node3_process.sh
  node_script 4 node4_pull.sh
}

step_health_v2_docs975_and_content_checks() {
  local h search_april search_phase1
  h="$(node_exec 1 curl -sf "http://$NODE4_IP:$SEARCH_PORT/health")"
  printf '%s\n' "$h" | "$PY" "$REPO/deploy/rehearsal/checks.py" health 2 975
  printf '%s\n' "$h" >"$HEALTH_V2_FILE"

  search_april="$(node_exec 1 curl -sf "http://$NODE4_IP:$SEARCH_PORT/search?q=april&k=100")"
  "$PY" - "$search_april" <<'PY'
import json
import sys

rows = json.loads(sys.argv[1]).get("results", [])
ids = [str(r.get("doc_id", "")) for r in rows]
if "1" in ids:
    raise SystemExit("doc 1 still present after phase1")
PY

  search_phase1="$(node_exec 1 curl -sf "http://$NODE4_IP:$SEARCH_PORT/search?q=added+during+project+phase&k=100")"
  "$PY" - "$search_phase1" <<'PY'
import json
import sys

rows = json.loads(sys.argv[1]).get("results", [])
ids = [str(r.get("doc_id", "")) for r in rows]
if not any(doc_id.startswith("phase1-new-") for doc_id in ids):
    raise SystemExit("phase1-new document not present")
PY
}

step_manifest_v2_postings427998() {
  local m
  m="$(node_source_run 2 "hdfs dfs -cat '$EXPORT_ROOT/v2/manifest.json'")"
  printf '%s\n' "$m" | "$PY" "$REPO/deploy/rehearsal/checks.py" manifest 2 975 427998
  printf '%s\n' "$m" >"$MANIFEST_V2_FILE"
}

step_capture_v2_export_hash_list() {
  hash_exports_v2_list >"$RUN_DIR/exports-v2.hashlist.before"
}

wait_for_datanode_port() {
  # The NameNode keeps a killed DataNode listed as live for minutes, so
  # confirm the restarted one really accepts data transfer connections.
  local ip="$1"
  for _ in $(seq 1 60); do
    if node_exec 4 bash -c "exec 3<>/dev/tcp/$ip/9866" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "DataNode on $ip did not accept connections on port 9866" >&2
  return 1
}

assert_datanode_port_closed() {
  local ip="$1"
  if node_exec 4 bash -c "exec 3<>/dev/tcp/$ip/9866" 2>/dev/null; then
    echo "DataNode on $ip still accepts connections after the kill" >&2
    return 1
  fi
}

step_datanode_loss_on_node3_and_recovery() {
  local fw fe fr
  kill_datanode_on_node 3
  sleep 2
  assert_datanode_port_closed "$NODE3_IP"
  hash_exports_v2_list >"$RUN_DIR/exports-v2.hashlist.node3-down"
  diff "$RUN_DIR/exports-v2.hashlist.before" "$RUN_DIR/exports-v2.hashlist.node3-down"
  node_script 3 node3_snapshots.sh list
  restart_datanode 3
  wait_for_datanode_port "$NODE3_IP"
  wait_for_datanodes_2 >/dev/null
  fw="$(fsck_for_path '/warehouse')"
  fe="$(fsck_for_path '/exports')"
  fr="$(fsck_for_path '/raw')"
  printf '%s\n' "$fw" | "$PY" "$REPO/deploy/rehearsal/checks.py" replication 2.0
  printf '%s\n' "$fe" | "$PY" "$REPO/deploy/rehearsal/checks.py" replication 2.0
  printf '%s\n' "$fr" | "$PY" "$REPO/deploy/rehearsal/checks.py" replication 2.0
}

step_datanode_loss_on_node2_and_recovery() {
  local fw fe fr
  kill_datanode_on_node 2
  sleep 2
  assert_datanode_port_closed "$NODE2_IP"
  hash_exports_v2_list >"$RUN_DIR/exports-v2.hashlist.node2-down"
  diff "$RUN_DIR/exports-v2.hashlist.before" "$RUN_DIR/exports-v2.hashlist.node2-down"
  restart_datanode 2
  wait_for_datanode_port "$NODE2_IP"
  wait_for_datanodes_2 >/dev/null
  fw="$(fsck_for_path '/warehouse')"
  fe="$(fsck_for_path '/exports')"
  fr="$(fsck_for_path '/raw')"
  printf '%s\n' "$fw" | "$PY" "$REPO/deploy/rehearsal/checks.py" replication 2.0
  printf '%s\n' "$fe" | "$PY" "$REPO/deploy/rehearsal/checks.py" replication 2.0
  printf '%s\n' "$fr" | "$PY" "$REPO/deploy/rehearsal/checks.py" replication 2.0
}

step_rollback_to_phase0_snapshot_and_republish() {
  local snapshot
  snapshot="$(read_file_or_empty "$PHASE0_SNAPSHOT_FILE")"
  if [ -z "$snapshot" ]; then
    echo "Missing phase0 snapshot id" >&2
    return 1
  fi
  node_script 3 node3_snapshots.sh rollback --snapshot "$snapshot"
  node_script 3 node3_process.sh
  node_script 4 node4_pull.sh
}

step_health_v3_docs1000_and_doc1_back() {
  local h s
  h="$(node_exec 1 curl -sf "http://$NODE4_IP:$SEARCH_PORT/health")"
  printf '%s\n' "$h" | "$PY" "$REPO/deploy/rehearsal/checks.py" health 3 1000
  printf '%s\n' "$h" >"$HEALTH_V3_FILE"
  s="$(node_exec 1 curl -sf "http://$NODE4_IP:$SEARCH_PORT/search?q=april&k=100")"
  "$PY" - "$s" <<'PY'
import json
import sys

rows = json.loads(sys.argv[1]).get("results", [])
if not any(str(r.get("doc_id")) == "1" for r in rows):
    raise SystemExit("doc 1 not restored")
PY
}

step_manifest_v3_docs1000() {
  local m
  m="$(node_source_run 2 "hdfs dfs -cat '$EXPORT_ROOT/v3/manifest.json'")"
  printf '%s\n' "$m" | "$PY" "$REPO/deploy/rehearsal/checks.py" manifest 3 1000 453438
  printf '%s\n' "$m" >"$MANIFEST_V3_FILE"
}

step_executor_launch_evidence_on_both_workers() {
  local count2 count3
  count2="$(node_source_run 2 'grep -R "Asked to launch executor" "$SPARK_HOME/logs" | wc -l' | tr -d '[:space:]')"
  count3="$(node_source_run 3 'grep -R "Asked to launch executor" "$SPARK_HOME/logs" | wc -l' | tr -d '[:space:]')"
  echo "node2 Asked-to-launch-executor count: $count2"
  echo "node3 Asked-to-launch-executor count: $count3"
  if [ "$count2" -eq 0 ]; then
    echo "Node2 worker logs do not show executor launch" >&2
    return 1
  fi
  if [ "$count3" -eq 0 ]; then
    echo "Node3 worker logs do not show executor launch" >&2
    return 1
  fi
}

run_step "prepare source tree" step_prepare_source_tree
load_cluster_env_values
run_step "prepare tools" step_prepare_tools_dir

for n in 1 2 3 4; do
  run_step "prepare node$n source and hosts" step_prepare_node_source_and_hosts "$n"
done

run_step "build rehearsal image" step_build_rehearsal_image
run_step "remove stale containers and network" step_remove_stale_containers_and_network
run_step "create rehearsal network" step_create_rehearsal_network

for n in 1 2 3 4; do
  run_step "start container node$n" step_start_container "$n"
done

run_step "start node2 services" step_start_node2_services
run_step "start node3 services" step_start_node3_services
run_step "start node4 serving process" step_start_node4_serving_process
run_step "wait for node4 /health" step_wait_node4_health_endpoint
run_step "assert search is unavailable before first pull" step_assert_search_unavailable_before_first_pull
run_step "wait for two datanodes" step_wait_for_two_datanodes
run_step "wait for two spark workers" step_wait_for_two_spark_workers

for n in 1 2 3 4; do
  run_step "check placement node$n" step_check_placement_node "$n"
done

run_step "phase0 ingest" step_phase0_ingest
run_step "phase0 process" step_phase0_process
run_step "phase0 pull" step_phase0_pull
run_step "health v1 docs1000" step_health_v1_docs1000
run_step "manifest v1 postings453438" step_manifest_v1_postings453438
run_step "assert phase0 content for doc1" step_assert_phase0_doc1_present
run_step "fsck replication warehouse exports raw" step_fsck_replication_warehouse_exports_raw
run_step "phase0 reingest changed0" step_phase0_reingest_changed0
run_step "phase1 ingest" step_phase1_ingest
run_step "phase1 index job" step_phase1_index_job
run_step "phase1 pagerank with worker kill" step_phase1_pagerank_with_worker_kill
run_step "simulate export failure keeps v1" step_simulate_export_failure_keeps_v1
run_step "phase1 process and pull" step_phase1_process_and_pull
run_step "health v2 docs975 and content checks" step_health_v2_docs975_and_content_checks
run_step "manifest v2 postings427998" step_manifest_v2_postings427998
run_step "capture v2 export hash list" step_capture_v2_export_hash_list
run_step "datanode loss on node3 and recovery" step_datanode_loss_on_node3_and_recovery
run_step "datanode loss on node2 and recovery" step_datanode_loss_on_node2_and_recovery
run_step "rollback to phase0 snapshot and republish" step_rollback_to_phase0_snapshot_and_republish
run_step "health v3 docs1000 and doc1 back" step_health_v3_docs1000_and_doc1_back
run_step "manifest v3 docs1000" step_manifest_v3_docs1000
run_step "executor launch evidence on both workers" step_executor_launch_evidence_on_both_workers

PASSED=1
echo "FOUR NODE REHEARSAL PASSED"
