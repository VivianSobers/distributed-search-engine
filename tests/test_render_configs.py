from __future__ import annotations

import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from deploy.render_configs import read_env, render


def test_read_env_parses_key_values(tmp_path: Path):
    env_file = tmp_path / "test.env"
    env_file.write_text(
        "# comment line\n"
        "\n"
        "FOO=bar\n"
        "KEY_WITH_EQUALS=val=123\n"
        "SPACED = trimmed \n",
        encoding="utf-8",
    )
    res = read_env(env_file)
    assert res["FOO"] == "bar"
    assert res["KEY_WITH_EQUALS"] == "val=123"
    assert res["SPACED"] == "trimmed"


def test_cluster_env_contains_expected_keys():
    cluster_env_path = Path("config/cluster.env")
    assert cluster_env_path.exists(), "config/cluster.env must exist"
    env = read_env(cluster_env_path)

    expected_keys = [
        "NODE1_IP",
        "NODE2_IP",
        "NODE3_IP",
        "NODE4_IP",
        "NAMENODE_PORT",
        "SPARK_MASTER_PORT",
        "SEARCH_PORT",
        "WAREHOUSE_PATH",
        "RAW_ROOT",
        "EXPORT_ROOT",
        "CHECKPOINT_ROOT",
        "SERVING_ROOT",
    ]
    for key in expected_keys:
        assert key in env, f"Key {key} missing from cluster.env"

    assert env["NODE1_IP"] == "0.0.0.0"
    assert env["NODE2_IP"] == "0.0.0.0"
    assert env["NODE3_IP"] == "0.0.0.0"
    assert env["NODE4_IP"] == "0.0.0.0"
    assert env["NAMENODE_PORT"] == "9000"
    assert env["SPARK_MASTER_PORT"] == "7077"
    assert env["SEARCH_PORT"] == "8000"
    assert env["WAREHOUSE_PATH"] == "/warehouse"
    assert env["RAW_ROOT"] == "/raw"
    assert env["EXPORT_ROOT"] == "/exports"
    assert env["CHECKPOINT_ROOT"] == "/checkpoints"
    assert env["SERVING_ROOT"] == "/serving"


@pytest.mark.parametrize("placeholder_key", ["NODE1_IP", "NODE2_IP", "NODE3_IP", "NODE4_IP"])
def test_render_rejects_placeholder_ip(placeholder_key: str, tmp_path: Path):
    env = {
        "NODE1_IP": "10.0.0.1",
        "NODE2_IP": "10.0.0.2",
        "NODE3_IP": "10.0.0.3",
        "NODE4_IP": "10.0.0.4",
        "NAMENODE_PORT": "9000",
        "SPARK_MASTER_PORT": "7077",
        "SEARCH_PORT": "8000",
        "WAREHOUSE_PATH": "/warehouse",
        "RAW_ROOT": "/raw",
        "EXPORT_ROOT": "/exports",
        "CHECKPOINT_ROOT": "/checkpoints",
        "SERVING_ROOT": "/serving",
    }
    env[placeholder_key] = "0.0.0.0"
    bd_home = tmp_path / "bd_home"
    out = tmp_path / "out"

    with pytest.raises(ValueError) as excinfo:
        render(env, bd_home, out)

    assert placeholder_key in str(excinfo.value)


@pytest.mark.parametrize("missing_key", ["NODE1_IP", "NODE2_IP", "NODE3_IP", "NODE4_IP"])
def test_render_rejects_missing_ip(missing_key: str, tmp_path: Path):
    env = {
        "NODE1_IP": "10.0.0.1",
        "NODE2_IP": "10.0.0.2",
        "NODE3_IP": "10.0.0.3",
        "NODE4_IP": "10.0.0.4",
        "NAMENODE_PORT": "9000",
        "SPARK_MASTER_PORT": "7077",
        "SEARCH_PORT": "8000",
        "WAREHOUSE_PATH": "/warehouse",
        "RAW_ROOT": "/raw",
        "EXPORT_ROOT": "/exports",
        "CHECKPOINT_ROOT": "/checkpoints",
        "SERVING_ROOT": "/serving",
    }
    del env[missing_key]
    bd_home = tmp_path / "bd_home"
    out = tmp_path / "out"

    with pytest.raises(ValueError) as excinfo:
        render(env, bd_home, out)

    assert missing_key in str(excinfo.value)


def _parse_xml_properties(xml_path: Path) -> dict[str, str]:
    tree = ET.parse(xml_path)
    root = tree.getroot()
    props = {}
    for prop in root.findall("property"):
        name = prop.find("name")
        value = prop.find("value")
        if name is not None and value is not None and name.text:
            props[name.text.strip()] = value.text.strip() if value.text else ""
    return props


def test_render_outputs_valid_configs(tmp_path: Path):
    env = {
        "NODE1_IP": "10.0.0.1",
        "NODE2_IP": "10.0.0.2",
        "NODE3_IP": "10.0.0.3",
        "NODE4_IP": "10.0.0.4",
        "NAMENODE_PORT": "9000",
        "SPARK_MASTER_PORT": "7077",
        "SEARCH_PORT": "8000",
        "WAREHOUSE_PATH": "/warehouse",
        "RAW_ROOT": "/raw",
        "EXPORT_ROOT": "/exports",
        "CHECKPOINT_ROOT": "/checkpoints",
        "SERVING_ROOT": "/serving",
    }
    bd_home = tmp_path / "my_bd_home"
    out = tmp_path / "cluster_conf"

    render(env, bd_home, out)

    # 1. hadoop/core-site.xml
    core_props = _parse_xml_properties(out / "hadoop" / "core-site.xml")
    assert core_props.get("fs.defaultFS") == "hdfs://10.0.0.2:9000"

    # 2. hadoop/hdfs-site.xml
    hdfs_props = _parse_xml_properties(out / "hadoop" / "hdfs-site.xml")
    assert hdfs_props.get("dfs.replication") == "2"
    assert hdfs_props.get("dfs.namenode.name.dir") == f"{bd_home}/hdfs/name"
    assert hdfs_props.get("dfs.datanode.data.dir") == f"{bd_home}/hdfs/data"
    assert hdfs_props.get("dfs.namenode.rpc-bind-host") == "0.0.0.0"
    assert hdfs_props.get("dfs.namenode.datanode.registration.ip-hostname-check") == "false"

    # 3. hadoop/workers
    hadoop_workers = (out / "hadoop" / "workers").read_text(encoding="utf-8").splitlines()
    assert [w.strip() for w in hadoop_workers if w.strip()] == ["10.0.0.2", "10.0.0.3"]

    # 4. spark/workers
    spark_workers = (out / "spark" / "workers").read_text(encoding="utf-8").splitlines()
    assert [w.strip() for w in spark_workers if w.strip()] == ["10.0.0.2", "10.0.0.3"]

    # 5. spark/spark-defaults.conf
    spark_defaults = (out / "spark" / "spark-defaults.conf").read_text(encoding="utf-8")
    assert "spark.master spark://10.0.0.3:7077" in spark_defaults
    assert "spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" in spark_defaults
    assert "spark.sql.catalog.iceberg_catalog org.apache.iceberg.spark.SparkCatalog" in spark_defaults
    assert "spark.sql.catalog.iceberg_catalog.type hadoop" in spark_defaults
    assert "spark.sql.catalog.iceberg_catalog.warehouse hdfs://10.0.0.2:9000/warehouse" in spark_defaults
    assert f"spark.jars {bd_home}/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar" in spark_defaults
    assert f"spark.pyspark.python {bd_home}/venv/bin/python" in spark_defaults
    assert "spark.task.maxFailures 4" in spark_defaults

    # 6. spark/spark-env.sh
    spark_env = (out / "spark" / "spark-env.sh").read_text(encoding="utf-8")
    assert "export SPARK_MASTER_HOST=10.0.0.3" in spark_env


def test_render_contains_no_home_paths_except_bd_home(tmp_path: Path):
    env = {
        "NODE1_IP": "10.0.0.1",
        "NODE2_IP": "10.0.0.2",
        "NODE3_IP": "10.0.0.3",
        "NODE4_IP": "10.0.0.4",
        "NAMENODE_PORT": "9000",
        "SPARK_MASTER_PORT": "7077",
        "SEARCH_PORT": "8000",
        "WAREHOUSE_PATH": "/warehouse",
        "RAW_ROOT": "/raw",
        "EXPORT_ROOT": "/exports",
        "CHECKPOINT_ROOT": "/checkpoints",
        "SERVING_ROOT": "/serving",
    }
    bd_home = Path("/opt/custom_bd_home")
    out = tmp_path / "out_conf"

    render(env, bd_home, out)

    for conf_file in out.rglob("*"):
        if conf_file.is_file():
            content = conf_file.read_text(encoding="utf-8")
            assert "/home/" not in content, f"{conf_file} contains unexpected '/home/':\n{content}"


def test_render_configs_cli(tmp_path: Path):
    env_file = tmp_path / "custom.env"
    env_file.write_text(
        "NODE1_IP=172.16.0.1\n"
        "NODE2_IP=172.16.0.2\n"
        "NODE3_IP=172.16.0.3\n"
        "NODE4_IP=172.16.0.4\n"
        "NAMENODE_PORT=9000\n"
        "SPARK_MASTER_PORT=7077\n"
        "SEARCH_PORT=8000\n"
        "WAREHOUSE_PATH=/warehouse\n"
        "RAW_ROOT=/raw\n"
        "EXPORT_ROOT=/exports\n"
        "CHECKPOINT_ROOT=/checkpoints\n"
        "SERVING_ROOT=/serving\n",
        encoding="utf-8",
    )
    out_dir = tmp_path / "cli_out"
    bd_home_dir = tmp_path / "cli_bd_home"

    proc = subprocess.run(
        [
            sys.executable,
            "deploy/render_configs.py",
            "--env",
            str(env_file),
            "--out",
            str(out_dir),
            "--bd-home",
            str(bd_home_dir),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, f"CLI failed: {proc.stderr}"
    assert (out_dir / "hadoop" / "core-site.xml").exists()
    assert (out_dir / "spark" / "spark-defaults.conf").exists()
