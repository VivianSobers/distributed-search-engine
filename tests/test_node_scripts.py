from __future__ import annotations

import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from dse.serving.asgi import make_app

NODE_SCRIPTS = [
    "scripts/node1_ingest.sh",
    "scripts/node2_start.sh",
    "scripts/node3_start.sh",
    "scripts/node3_process.sh",
    "scripts/node4_pull.sh",
    "scripts/node4_serve.sh",
]

SCRIPTS = ["scripts/lib.sh", *NODE_SCRIPTS]


@pytest.fixture(autouse=True)
def sample_cluster_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / "cluster.env"
    env_file.write_text(
        "\n".join(
            [
                "NODE1_IP=10.0.0.1",
                "NODE2_IP=10.0.0.2",
                "NODE3_IP=10.0.0.3",
                "NODE4_IP=10.0.0.4",
                "NAMENODE_PORT=9000",
                "SPARK_MASTER_PORT=7077",
                "SEARCH_PORT=8000",
                "WAREHOUSE_PATH=/warehouse",
                "RAW_ROOT=/raw",
                "EXPORT_ROOT=/exports",
                "CHECKPOINT_ROOT=/checkpoints",
                "SERVING_ROOT=/serving",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CLUSTER_ENV", str(env_file))


@pytest.mark.parametrize("script_path", SCRIPTS)
def test_bash_syntax_check(script_path: str):
    p = Path(script_path)
    assert p.exists(), f"Script {script_path} does not exist"
    proc = subprocess.run(["bash", "-n", str(p)], capture_output=True, text=True)
    assert proc.returncode == 0, f"bash -n failed on {script_path}:\n{proc.stderr}"


@pytest.mark.parametrize("script_path", NODE_SCRIPTS)
def test_node_script_names_lib_sh_and_strict_mode(script_path: str):
    content = Path(script_path).read_text(encoding="utf-8")
    assert "lib.sh" in content, f"{script_path} does not reference lib.sh"
    assert "set -euo pipefail" in content, f"{script_path} does not set strict mode"


def test_node2_start_guards_format(tmp_path: Path):
    script_path = Path("scripts/node2_start.sh")
    assert script_path.exists()
    content = script_path.read_text(encoding="utf-8")
    assert "hdfs/name/current" in content, "node2_start.sh must check for hdfs/name/current"

    # Create dummy bin directory to mock commands called by node2_start.sh
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_file = tmp_path / "calls.log"

    hdfs_mock = bin_dir / "hdfs"
    hdfs_mock.write_text(
        f"#!/usr/bin/env bash\necho \"hdfs $@\" >> '{log_file}'\n",
        encoding="utf-8",
    )
    hdfs_mock.chmod(0o755)

    start_worker_mock = bin_dir / "start-worker.sh"
    start_worker_mock.write_text(
        f"#!/usr/bin/env bash\necho \"start-worker $@\" >> '{log_file}'\n",
        encoding="utf-8",
    )
    start_worker_mock.chmod(0o755)

    # Mock BD_HOME
    bd_home = tmp_path / "bd_home"
    current_dir = bd_home / "hdfs" / "name" / "current"

    env = os.environ.copy()
    env["BD_HOME"] = str(bd_home)
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"

    # Case 1: current is absent -> format SHOULD run
    if log_file.exists():
        log_file.unlink()
    proc1 = subprocess.run(["bash", str(script_path.resolve())], env=env, capture_output=True, text=True)
    assert proc1.returncode == 0, f"Failed when current absent: {proc1.stderr}"
    calls1 = log_file.read_text(encoding="utf-8")
    assert "namenode -format" in calls1, "namenode format should be called when current is absent"

    # Case 2: current is present -> format SHOULD NOT run
    current_dir.mkdir(parents=True, exist_ok=True)
    if log_file.exists():
        log_file.unlink()
    proc2 = subprocess.run(["bash", str(script_path.resolve())], env=env, capture_output=True, text=True)
    assert proc2.returncode == 0, f"Failed when current present: {proc2.stderr}"
    calls2 = log_file.read_text(encoding="utf-8")
    assert "namenode -format" not in calls2, "namenode format must NOT be called when current is present"


def _write_minimal_export(root: Path, version: int) -> Path:
    version_dir = root / f"v{version}"
    docs_dir = version_dir / "docs"
    index_dir = version_dir / "index"
    docs_dir.mkdir(parents=True)
    index_dir.mkdir(parents=True)

    docs = [{"doc_id": "d1", "url": "https://example.com/d1", "title": "D1", "length": 1, "pagerank": 1.0}]
    index_rows = [{"term": "test", "doc_id": "d1", "tf": 1}]
    pq.write_table(pa.Table.from_pylist(docs), docs_dir / "part-000.parquet")
    pq.write_table(pa.Table.from_pylist(index_rows), index_dir / "part-000.parquet")

    manifest = {"version": version, "docs": 1, "postings": 1}
    (version_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return version_dir


def test_make_app_honours_serving_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    serving_dir = tmp_path / "serving"
    serving_dir.mkdir()
    v1 = _write_minimal_export(serving_dir, 1)
    (serving_dir / "current").symlink_to(v1.name)

    monkeypatch.setenv("SERVING_ROOT", str(serving_dir))
    app = make_app()

    with TestClient(app) as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["status"] == "ok"
        assert payload["version"] == 1


def test_make_app_default_or_missing_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    empty_serving = tmp_path / "empty_serving"
    empty_serving.mkdir()
    monkeypatch.setenv("SERVING_ROOT", str(empty_serving))
    app = make_app()

    with TestClient(app) as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "unloaded"


def test_run_job_script(tmp_path: Path):
    dummy_module = tmp_path / "test_module.py"
    dummy_module.write_text(
        "def main(args):\n"
        "    if args == ['--check', 'ok']:\n"
        "        return 0\n"
        "    return 42\n",
        encoding="utf-8",
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = f"{tmp_path}:{env.get('PYTHONPATH', '')}"

    proc = subprocess.run(
        [sys.executable, "deploy/run_job.py", "test_module", "--check", "ok"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0

    proc_fail = subprocess.run(
        [sys.executable, "deploy/run_job.py", "test_module", "bad"],
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc_fail.returncode == 42


def test_lib_sh_functions_and_paths(tmp_path: Path):
    spaced_repo = tmp_path / "repo with spaces"
    spaced_scripts = spaced_repo / "scripts"
    spaced_scripts.mkdir(parents=True)

    env_content = Path("scripts/env.sh").read_text(encoding="utf-8")
    (spaced_scripts / "env.sh").write_text(env_content, encoding="utf-8")
    lib_content = Path("scripts/lib.sh").read_text(encoding="utf-8")
    (spaced_scripts / "lib.sh").write_text(lib_content, encoding="utf-8")

    (spaced_repo / "config").mkdir()
    (spaced_repo / "config" / "cluster.env").write_text(
        "NODE1_IP=10.0.0.1\nNODE2_IP=10.0.0.2\nNODE3_IP=10.0.0.3\nNODE4_IP=10.0.0.4\n"
        "NAMENODE_PORT=9000\nSPARK_MASTER_PORT=7077\nSEARCH_PORT=8000\n"
        "WAREHOUSE_PATH=/warehouse\nRAW_ROOT=/raw\nEXPORT_ROOT=/exports\n"
        "CHECKPOINT_ROOT=/checkpoints\nSERVING_ROOT=/serving\n",
        encoding="utf-8",
    )

    test_bash = f"""
    set -euo pipefail
    . '{spaced_scripts / "lib.sh"}'
    echo "REPO_ROOT=$REPO_ROOT"
    echo "HADOOP_CONF_DIR=$HADOOP_CONF_DIR"
    echo "SPARK_CONF_DIR=$SPARK_CONF_DIR"
    echo "NODE1_IP=$NODE1_IP"
    type render_cluster_conf >/dev/null
    type package_app >/dev/null
    type submit_job >/dev/null
    """
    proc = subprocess.run(["bash", "-c", test_bash], capture_output=True, text=True)
    assert proc.returncode == 0, f"lib.sh failed: {proc.stderr}"
    assert f"REPO_ROOT={spaced_repo}" in proc.stdout
    assert "HADOOP_CONF_DIR=" in proc.stdout
    assert "SPARK_CONF_DIR=" in proc.stdout
    assert "NODE1_IP=10.0.0.1" in proc.stdout


def test_package_app_zips_dse_and_copies_run_job(tmp_path: Path):
    bd_home = tmp_path / "bd_home"
    env = os.environ.copy()
    env["BD_HOME"] = str(bd_home)

    test_bash = f"""
    set -euo pipefail
    . '{Path("scripts/lib.sh").resolve()}'
    package_app
    """
    proc = subprocess.run(["bash", "-c", test_bash], env=env, capture_output=True, text=True)
    assert proc.returncode == 0, f"package_app failed: {proc.stderr}"

    zip_path = bd_home / "app" / "dse.zip"
    run_job_path = bd_home / "app" / "run_job.py"
    assert zip_path.exists()
    assert run_job_path.exists()

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        assert "dse/__init__.py" in names
        assert "dse/config.py" in names
        assert not any("__pycache__" in n for n in names)
        assert not any(n.endswith(".pyc") for n in names)


def test_node1_ingest_argument_validation():
    proc1 = subprocess.run(["bash", "scripts/node1_ingest.sh"], capture_output=True, text=True)
    assert proc1.returncode != 0
    proc2 = subprocess.run(["bash", "scripts/node1_ingest.sh", "2"], capture_output=True, text=True)
    assert proc2.returncode != 0


def test_node1_ingest_execution(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_file = tmp_path / "calls.log"

    hdfs_mock = bin_dir / "hdfs"
    hdfs_mock.write_text(f"#!/usr/bin/env bash\necho \"hdfs $@\" >> '{log_file}'\n", encoding="utf-8")
    hdfs_mock.chmod(0o755)

    spark_mock = bin_dir / "spark-submit"
    spark_mock.write_text(f"#!/usr/bin/env bash\necho \"spark-submit $@\" >> '{log_file}'\n", encoding="utf-8")
    spark_mock.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
    env["BD_HOME"] = str(tmp_path / "bd_home")
    env["NODE1_IP"] = "10.0.0.1"
    env["NODE2_IP"] = "10.0.0.2"
    env["NAMENODE_PORT"] = "9000"
    env["RAW_ROOT"] = "/raw"

    proc0 = subprocess.run(["bash", "scripts/node1_ingest.sh", "0"], env=env, capture_output=True, text=True)
    assert proc0.returncode == 0, f"node1_ingest 0 failed: {proc0.stderr}"
    calls0 = log_file.read_text(encoding="utf-8")
    assert "hdfs dfs -mkdir -p /raw/phase-0/" in calls0
    assert "hdfs dfs -put -f" in calls0
    assert "pages.jsonl /raw/phase-0/" in calls0
    assert "spark-submit" in calls0
    assert "dse.jobs.ingest --phase 0 --input hdfs://10.0.0.2:9000/raw/phase-0/pages.jsonl" in calls0

    log_file.unlink()
    proc1 = subprocess.run(["bash", "scripts/node1_ingest.sh", "1"], env=env, capture_output=True, text=True)
    assert proc1.returncode == 0, f"node1_ingest 1 failed: {proc1.stderr}"
    calls1 = log_file.read_text(encoding="utf-8")
    assert "hdfs dfs -mkdir -p /raw/phase-1/" in calls1
    assert "hdfs dfs -put -f" in calls1
    assert "changes.json /raw/phase-1/" in calls1
    assert "dse.jobs.ingest --phase 1 --input hdfs://10.0.0.2:9000/raw/phase-1/changes.json" in calls1


def test_node3_start_execution(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_file = tmp_path / "calls.log"

    for cmd in ["hdfs", "start-master.sh", "start-worker.sh"]:
        mock = bin_dir / cmd
        mock.write_text(f"#!/usr/bin/env bash\necho \"{cmd} $@\" >> '{log_file}'\n", encoding="utf-8")
        mock.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
    env["BD_HOME"] = str(tmp_path / "bd_home")
    env["NODE3_IP"] = "10.0.0.3"
    env["SPARK_MASTER_PORT"] = "7077"

    proc = subprocess.run(["bash", "scripts/node3_start.sh"], env=env, capture_output=True, text=True)
    assert proc.returncode == 0, f"node3_start failed: {proc.stderr}"
    calls = log_file.read_text(encoding="utf-8").splitlines()
    assert any("hdfs --daemon start datanode" in c for c in calls)
    assert any("start-master.sh" in c for c in calls)
    assert any("start-worker.sh spark://10.0.0.3:7077" in c for c in calls)


def test_node3_process_execution(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_file = tmp_path / "calls.log"

    spark_mock = bin_dir / "spark-submit"
    spark_mock.write_text(f"#!/usr/bin/env bash\necho \"spark-submit $@\" >> '{log_file}'\n", encoding="utf-8")
    spark_mock.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
    env["BD_HOME"] = str(tmp_path / "bd_home")
    env["NODE2_IP"] = "10.0.0.2"
    env["NODE3_IP"] = "10.0.0.3"
    env["NAMENODE_PORT"] = "9000"
    env["CHECKPOINT_ROOT"] = "/checkpoints"
    env["EXPORT_ROOT"] = "/exports"

    proc = subprocess.run(["bash", "scripts/node3_process.sh"], env=env, capture_output=True, text=True)
    assert proc.returncode == 0, f"node3_process failed: {proc.stderr}"
    calls = log_file.read_text(encoding="utf-8").splitlines()
    assert len(calls) == 3
    assert "dse.jobs.index_job" in calls[0]
    assert "dse.jobs.pagerank_job --checkpoint-dir hdfs://10.0.0.2:9000/checkpoints/pagerank" in calls[1]
    assert "dse.jobs.export_job --export-root hdfs://10.0.0.2:9000/exports" in calls[2]


def test_node4_pull_execution(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_file = tmp_path / "calls.log"

    py_mock = bin_dir / "python"
    py_mock.write_text(f"#!/usr/bin/env bash\necho \"python $@\" >> '{log_file}'\n", encoding="utf-8")
    py_mock.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
    env["BD_HOME"] = str(tmp_path / "bd_home")
    env["NODE2_IP"] = "10.0.0.2"
    env["NAMENODE_PORT"] = "9000"
    env["EXPORT_ROOT"] = "/exports"
    env["SERVING_ROOT"] = "/serving"
    env["SEARCH_PORT"] = "8000"

    proc = subprocess.run(["bash", "scripts/node4_pull.sh"], env=env, capture_output=True, text=True)
    assert proc.returncode == 0, f"node4_pull failed: {proc.stderr}"
    calls = log_file.read_text(encoding="utf-8")
    assert "-m dse.serving.pull" in calls
    assert "--export-root hdfs://10.0.0.2:9000/exports" in calls
    assert "--serving-root /serving" in calls
    assert "--reload-url http://127.0.0.1:8000/reload" in calls


def test_node4_serve_execution(tmp_path: Path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_file = tmp_path / "calls.log"
    repo_root = Path(__file__).resolve().parents[1]

    uvicorn_mock = bin_dir / "uvicorn"
    uvicorn_mock.write_text(
        f"#!/usr/bin/env bash\necho \"PYTHONPATH=$PYTHONPATH SERVING_ROOT=$SERVING_ROOT uvicorn $@\" >> '{log_file}'\n",
        encoding="utf-8",
    )
    uvicorn_mock.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env.get('PATH', '')}"
    env["BD_HOME"] = str(tmp_path / "bd_home")
    env["SEARCH_PORT"] = "8000"
    env["SERVING_ROOT"] = "/serving"
    script_path = Path("scripts/node4_serve.sh").resolve()

    proc = subprocess.run(
        ["bash", str(script_path)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"node4_serve failed: {proc.stderr}"
    calls = log_file.read_text(encoding="utf-8")
    assert f"PYTHONPATH={repo_root}" in calls
    assert "SERVING_ROOT=/serving" in calls
    assert "--factory dse.serving.asgi:make_app" in calls
    assert "--host 0.0.0.0" in calls
    assert "--port 8000" in calls


@pytest.mark.parametrize(
    "name",
    ["node1_ingest", "node2_start", "node3_start", "node3_process", "node4_pull", "node4_serve"],
)
def test_node_script_renders_cluster_config(name: str):
    assert "render_cluster_conf" in Path(f"scripts/{name}.sh").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("name", "variable"),
    [("node2_start", "NODE2_IP"), ("node3_start", "NODE3_IP")],
)
def test_spark_daemons_bind_to_their_own_node(name: str, variable: str):
    text = Path(f"scripts/{name}.sh").read_text(encoding="utf-8")
    assert f'export SPARK_LOCAL_IP="${variable}"' in text
