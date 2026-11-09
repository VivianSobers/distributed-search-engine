# Four-node runbook

These steps run the project across four machines. They have not yet been rehearsed on four separate machines. The single-machine rehearsal is `bash scripts/local_e2e.sh`.

## 1. Network

Put all four machines on one ZeroTier network and note each machine's ZeroTier IP with `sudo zerotier-cli listnetworks`.

Spark and HDFS use many ports, so trust the ZeroTier interface on each machine:

| System | Command |
|---|---|
| Ubuntu (ufw) | `sudo ufw allow in on <zerotier-interface>` |
| Fedora (firewalld) | `sudo firewall-cmd --permanent --zone=trusted --add-interface=<zerotier-interface> && sudo firewall-cmd --reload` |

Find the interface name with `ip link`. It starts with `zt`.

## 2. Software, on every node

Each machine needs Java 11 and Python 3.10.

```bash
git clone https://github.com/VivianSobers/distributed-search-engine.git
cd distributed-search-engine
bash scripts/install_deps.sh
```

Edit `config/cluster.env` and replace the four `NODE*_IP` placeholders with the ZeroTier IPs. Every machine needs the same values, and the node scripts refuse to run while a placeholder remains.

On Node 1, download the corpus from the course page to `data/full/phase-0/pages.jsonl` and `data/full/phase-1/changes.json`.

On Node 4, create the serving directory:

```bash
sudo mkdir -p /serving && sudo chown "$USER" /serving
```

The node scripts are bash scripts. Run them with `bash scripts/<name>.sh` from any directory.

## 3. Start the cluster

Run these in order:

| Order | Node | Command |
|---|---|---|
| 1 | Node 2 | `bash scripts/node2_start.sh` |
| 2 | Node 3 | `bash scripts/node3_start.sh` |
| 3 | Node 4 | `bash scripts/node4_serve.sh`, and leave it running |

`node2_start.sh` formats the NameNode only the first time. Check the cluster from Node 2 or Node 3:

```bash
source scripts/lib.sh
hdfs dfsadmin -report | grep 'Live datanodes'
```

It should report 2 live DataNodes. The Spark master page at `http://<node3-ip>:8080` should list 2 workers.

## 4. Demo order

Phase 0:

| Node | Command |
|---|---|
| Node 1 | `bash scripts/node1_ingest.sh 0` |
| Node 3 | `bash scripts/node3_process.sh` |
| Node 4 | `bash scripts/node4_pull.sh` |

Open `http://<node4-ip>:8000` and search. `/health` should report version 1 and 1000 documents.

Phase 1 uses the same three commands with `1` in place of `0`. `/health` should then report version 2 and 975 documents.

To show that re-ingesting changes nothing, run `bash scripts/node1_ingest.sh 0` again. The printed `documents_changed` is 0.

## 5. Failure demonstrations

To show Spark recovering from a lost worker, run `bash scripts/node3_process.sh` on Node 3 after a new ingest. While PageRank is running, stop the worker on Node 2:

```bash
pkill -9 -f org.apache.spark.executor.CoarseGrainedExecutorBackend
pkill -9 -f org.apache.spark.deploy.worker.Worker
```

The job finishes on Node 3's worker. Restart the Node 2 worker afterwards:

```bash
source scripts/lib.sh
export SPARK_LOCAL_IP="$NODE2_IP"
start-worker.sh "spark://$NODE3_IP:$SPARK_MASTER_PORT"
```

To show the previous version staying live when an export fails, run this on Node 3 after a new ingest and after the index and PageRank jobs have finished:

```bash
source scripts/lib.sh
package_app
submit_job "$NODE3_IP" dse.jobs.export_job --export-root "hdfs://$NODE2_IP:$NAMENODE_PORT$EXPORT_ROOT" --simulate-failure
```

The job exits with an error, and `/health` keeps reporting the old version.

To recover from a bad update, list the Iceberg versions and roll back on Node 3:

```bash
bash scripts/node3_snapshots.sh list
bash scripts/node3_snapshots.sh rollback --snapshot <snapshot_id>
bash scripts/node3_process.sh
```

Then run `bash scripts/node4_pull.sh` on Node 4. Search moves to a new version holding the restored data. To read a page as it was at an earlier version, use `bash scripts/node3_snapshots.sh show --snapshot <snapshot_id> --doc-id <doc_id>`.
