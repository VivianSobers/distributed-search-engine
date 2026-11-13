from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def read_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    content = path.read_text(encoding="utf-8")
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, val = line.split("=", 1)
            env[key.strip()] = val.strip()
    return env


def render(env: dict[str, str], bd_home: Path, out: Path) -> None:
    node_keys = ["NODE1_IP", "NODE2_IP", "NODE3_IP", "NODE4_IP"]
    for key in node_keys:
        val = env.get(key)
        if not val or val.strip() == "0.0.0.0":
            raise ValueError(f"Invalid or placeholder IP address for key: {key}")

    node2_ip = env["NODE2_IP"].strip()
    node3_ip = env["NODE3_IP"].strip()

    namenode_port = env.get("NAMENODE_PORT", "9000").strip()
    spark_master_port = env.get("SPARK_MASTER_PORT", "7077").strip()
    warehouse_path = env.get("WAREHOUSE_PATH", "/warehouse").strip()

    bd_home_str = str(bd_home)

    hadoop_dir = out / "hadoop"
    spark_dir = out / "spark"
    hadoop_dir.mkdir(parents=True, exist_ok=True)
    spark_dir.mkdir(parents=True, exist_ok=True)

    # 1. hadoop/core-site.xml
    core_site = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n'
        "<configuration>\n"
        "  <property>\n"
        "    <name>fs.defaultFS</name>\n"
        f"    <value>hdfs://{node2_ip}:{namenode_port}</value>\n"
        "  </property>\n"
        "</configuration>\n"
    )
    (hadoop_dir / "core-site.xml").write_text(core_site, encoding="utf-8")

    # 2. hadoop/hdfs-site.xml
    hdfs_site = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n'
        "<configuration>\n"
        "  <property>\n"
        "    <name>dfs.replication</name>\n"
        "    <value>2</value>\n"
        "  </property>\n"
        "  <property>\n"
        "    <name>dfs.namenode.name.dir</name>\n"
        f"    <value>{bd_home_str}/hdfs/name</value>\n"
        "  </property>\n"
        "  <property>\n"
        "    <name>dfs.datanode.data.dir</name>\n"
        f"    <value>{bd_home_str}/hdfs/data</value>\n"
        "  </property>\n"
        "  <property>\n"
        "    <name>dfs.permissions.enabled</name>\n"
        "    <value>false</value>\n"
        "  </property>\n"
        "  <property>\n"
        "    <name>dfs.namenode.rpc-bind-host</name>\n"
        "    <value>0.0.0.0</value>\n"
        "  </property>\n"
        "  <property>\n"
        "    <name>dfs.namenode.datanode.registration.ip-hostname-check</name>\n"
        "    <value>false</value>\n"
        "  </property>\n"
        "  <property>\n"
        "    <name>dfs.client.socket-timeout</name>\n"
        "    <value>600000</value>\n"
        "  </property>\n"
        "  <property>\n"
        "    <name>dfs.datanode.socket.write.timeout</name>\n"
        "    <value>600000</value>\n"
        "  </property>\n"
        "</configuration>\n"
    )
    (hadoop_dir / "hdfs-site.xml").write_text(hdfs_site, encoding="utf-8")

    # 3. hadoop/workers
    hadoop_workers = f"{node2_ip}\n{node3_ip}\n"
    (hadoop_dir / "workers").write_text(hadoop_workers, encoding="utf-8")

    # 4. spark/workers
    spark_workers = f"{node2_ip}\n{node3_ip}\n"
    (spark_dir / "workers").write_text(spark_workers, encoding="utf-8")

    # 5. spark/spark-defaults.conf
    iceberg_keys: list[tuple[str, str]] = [
        ("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        ("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog"),
        ("spark.sql.catalog.iceberg_catalog.type", "hadoop"),
        ("spark.sql.catalog.iceberg_catalog.warehouse", f"hdfs://{node2_ip}:{namenode_port}{warehouse_path}"),
    ]

    spark_defaults_lines = [
        f"spark.master spark://{node3_ip}:{spark_master_port}",
    ]
    for k, v in iceberg_keys:
        spark_defaults_lines.append(f"{k} {v}")
    spark_defaults_lines.extend([
        f"spark.jars {bd_home_str}/iceberg/iceberg-spark-runtime-3.3_2.12-1.8.1.jar",
        # A bare name, so each node resolves its own venv through PATH
        "spark.pyspark.python python",
        "spark.task.maxFailures 4",
    ])
    (spark_dir / "spark-defaults.conf").write_text("\n".join(spark_defaults_lines) + "\n", encoding="utf-8")

    # 6. spark/spark-env.sh
    spark_env = (
        "#!/usr/bin/env bash\n"
        f"export SPARK_MASTER_HOST={node3_ip}\n"
    )
    (spark_dir / "spark-env.sh").write_text(spark_env, encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Render cluster configurations")
    parser.add_argument("--env", type=Path, required=True, help="Path to cluster.env")
    parser.add_argument("--out", type=Path, required=True, help="Path to output directory")
    parser.add_argument(
        "--bd-home",
        type=Path,
        default=Path(os.environ.get("BD_HOME", Path.home() / ".local/opt/bd-search-engine")),
        help="Path to bd_home",
    )
    args = parser.parse_args(argv)
    env = read_env(args.env)
    render(env, args.bd_home, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
